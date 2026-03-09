"""Export Text-Fabric datasets into Neo4j."""

from __future__ import annotations

from collections.abc import Iterable as IterableABC
from dataclasses import dataclass
import hashlib
from typing import Any, Iterable, Iterator, Mapping, Sequence
from urllib.parse import urlparse, urlunparse

from neo4j import GraphDatabase
from tf.fabric import Fabric


@dataclass
class ExportStats:
    """Counts produced by a completed export run."""

    node_count: int = 0
    relationship_count: int = 0


@dataclass
class TFExportConfig:
    """Configuration for exporting a Text-Fabric dataset into Neo4j."""

    tf_locations: str | Sequence[str]
    tf_modules: str | Sequence[str] | None = None
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "neo4j"
    neo4j_database: str = "neo4j"
    node_features: Sequence[str] | None = None
    edge_features: Sequence[str] | None = None
    batch_size: int = 2000
    clear_database: bool = False
    clear_batch_size: int = 10000
    add_previous_next: bool = False
    previous_next_node_types: Sequence[str] | None = None
    hierarchy_node_types: Sequence[str] | None = None
    frame_semantic_relations: bool = True
    silent: str | bool | None = "auto"
    show_progress: bool = True
    progress_use_tqdm: bool = True
    progress_every: int = 50000


FRAME_ROLE_MAP: dict[str, tuple[str, str]] = {
    # Relation direction is always source(frame owner) -> target(argument node).
    "A0": ("HAS_AGENT", "agent"),
    "A1": ("HAS_PATIENT", "patient"),
    "A2": ("HAS_RECIPIENT", "recipient"),
    "AA2": ("HAS_ADVERBIAL", "adverbial"),
}


class _ProgressCounter:
    """Progress counter with optional tqdm support and print fallback."""

    def __init__(
        self,
        label: str,
        total: int | None,
        enabled: bool,
        use_tqdm: bool,
        every: int,
        unit: str = "items",
    ) -> None:
        self.label = label
        self.total = total
        self.enabled = enabled
        self.every = max(1, every)
        self.unit = unit
        self.count = 0
        self._next_tick = self.every
        self._bar: Any | None = None

        if not enabled:
            return
        if use_tqdm:
            try:
                from tqdm.auto import tqdm

                # Prefer tqdm when available for a smooth in-place progress display.
                self._bar = tqdm(total=total, desc=label, unit=unit, leave=False)
                return
            except Exception:
                # Fall back to plain prints if tqdm is unavailable/misconfigured.
                self._bar = None
        print(f"[tf2neo4j] {label} started")

    def update(self, amount: int) -> None:
        """Advance the counter by a number of processed items."""
        if amount <= 0:
            return
        self.count += amount
        if not self.enabled:
            return
        if self._bar is not None:
            self._bar.update(amount)
            return
        if self.count >= self._next_tick:
            if self.total:
                print(f"[tf2neo4j] {self.label}: {self.count}/{self.total} {self.unit}")
            else:
                print(f"[tf2neo4j] {self.label}: {self.count} {self.unit}")
            self._next_tick += self.every

    def close(self) -> None:
        """Finalize the counter output."""
        if not self.enabled:
            return
        if self._bar is not None:
            self._bar.close()
            return
        if self.total:
            print(f"[tf2neo4j] {self.label} done: {self.count}/{self.total} {self.unit}")
        else:
            print(f"[tf2neo4j] {self.label} done: {self.count} {self.unit}")


class _ProgressReporter:
    """Factory and logger for progress output."""

    def __init__(self, config: TFExportConfig) -> None:
        self.enabled = bool(config.show_progress)
        self.use_tqdm = bool(config.progress_use_tqdm)
        self.every = max(1, int(config.progress_every))

    def log(self, message: str) -> None:
        """Print a one-line progress message when enabled."""
        if self.enabled:
            print(f"[tf2neo4j] {message}")

    def counter(self, label: str, total: int | None = None, unit: str = "items") -> _ProgressCounter:
        """Create a progress counter for a task."""
        return _ProgressCounter(
            label=label,
            total=total,
            enabled=self.enabled,
            use_tqdm=self.use_tqdm,
            every=self.every,
            unit=unit,
        )


def _as_list(value: str | Sequence[str] | None) -> list[str] | None:
    """Normalize a scalar-or-sequence input to a list."""
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    return list(value)


def _neo4j_safe(value: Any) -> Any:
    """Convert values to Neo4j-compatible scalar/list/map shapes."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_neo4j_safe(v) for v in value]
    if isinstance(value, set):
        return [_neo4j_safe(v) for v in sorted(value)]
    if isinstance(value, Mapping):
        return {str(k): _neo4j_safe(v) for k, v in value.items()}
    return str(value)


def _call_maybe(api: Any, name: str) -> list[str]:
    """Call an API function if present and return normalized string results."""
    fn = getattr(api, name, None)
    if fn is None:
        return []
    data = fn()
    if data is None:
        return []
    if isinstance(data, str):
        return [data] if data else []
    if not isinstance(data, IterableABC):
        return []
    return [str(item) for item in data if item is not None]


def _dedupe(values: Sequence[str]) -> list[str]:
    """Preserve order while removing duplicates."""
    return list(dict.fromkeys(values))


def _iter_tf_nodes(api: Any) -> Iterator[int]:
    """Yield all TF node ids available in the loaded API."""
    otype = api.F.otype
    max_node = getattr(otype, "maxNode", None)
    if isinstance(max_node, int) and max_node > 0:
        # Fast path: contiguous node-id range.
        for node in range(1, max_node + 1):
            if otype.v(node) is not None:
                yield node
        return

    n_api = getattr(api, "N", None)
    walk = getattr(n_api, "walk", None) if n_api is not None else None
    if callable(walk):
        walked = walk()
        if not isinstance(walked, IterableABC):
            raise RuntimeError("Text-Fabric N.walk() did not return an iterable.")
        for raw_node in walked:
            yield int(raw_node)
        return

    raise RuntimeError("Unable to iterate Text-Fabric nodes; missing both otype.maxNode and N.walk().")


def _collect_node_otypes(api: Any, node_ids: Sequence[int]) -> dict[int, str]:
    """Collect TF node type (otype) for each node id."""
    otype = api.F.otype
    return {node: str(otype.v(node)) for node in node_ids}


def _group_node_ids_by_otype(
    node_ids: Sequence[int], node_otypes: Mapping[int, str]
) -> dict[str, list[int]]:
    """Group node ids by otype while preserving original order."""
    # Order is important for NEXT/PREVIOUS links.
    grouped: dict[str, list[int]] = {}
    for node_id in node_ids:
        otype = node_otypes.get(node_id)
        if otype is None:
            continue
        grouped.setdefault(otype, []).append(node_id)
    return grouped


def _resolve_previous_next_types(
    configured_types: Sequence[str] | None, available_types: Sequence[str]
) -> tuple[list[str], list[str]]:
    """Resolve requested sequence-link node types against available node types."""
    available_set = set(available_types)
    if configured_types is None:
        return list(available_types), []

    requested = _dedupe([str(t) for t in configured_types if t])
    resolved = [t for t in requested if t in available_set]
    missing = [t for t in requested if t not in available_set]
    return resolved, missing


def _iter_nodes(
    api: Any, node_otypes: Mapping[int, str], selected_features: Sequence[str]
) -> Iterator[dict[str, Any]]:
    """Yield node payloads for Neo4j writes."""
    otype = api.F.otype
    for node, node_type in node_otypes.items():
        props: dict[str, Any] = {"otype": node_type}
        for feature in selected_features:
            if feature == "otype":
                continue
            feature_obj = getattr(api.F, feature, None)
            if feature_obj is None or not hasattr(feature_obj, "v"):
                continue
            try:
                value = feature_obj.v(node)
            except Exception:
                continue
            if value is None:
                continue
            props[feature] = _neo4j_safe(value)
        yield {"tf_id": node, "otype": node_type, "props": props}


def _to_int(value: Any) -> int | None:
    """Safely cast a value to int, returning None if conversion fails."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _frame_role_code(value: Any) -> str | None:
    """Extract a normalized frame role code from a feature value payload."""
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if "=" in raw:
            raw = raw.split("=", 1)[0].strip()
        return raw.upper() if raw else None
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        return _frame_role_code(value[0])
    if isinstance(value, Mapping):
        for key in ("role", "code", "value"):
            if key in value:
                return _frame_role_code(value[key])
        return None
    return str(value).strip().upper() or None


def _iter_semantic_frame_relationships(rows: Iterable[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """Map TF 'frame' edge rows to semantic Neo4j relationship types."""
    for row in rows:
        props = dict(row.get("props", {}))
        role_code = _frame_role_code(props.get("value"))
        rel_type, role_name = FRAME_ROLE_MAP.get(role_code or "", ("HAS_FRAME_ROLE", "frame_role"))
        props["role_code"] = role_code
        props["role_name"] = role_name
        props["edge_feature"] = "frame"
        converted = dict(row)
        converted["relationship_type"] = rel_type
        converted["props"] = props
        yield converted


def _iter_hierarchy_relationships(
    api: Any,
    parent_node_ids: Sequence[int],
    node_otypes: Mapping[int, str],
    parent_otype: str,
    child_otype: str,
) -> Iterator[dict[str, Any]]:
    """Yield hierarchy rows based on Text-Fabric locality API (L.d)."""
    locality = getattr(api, "L", None)
    if locality is None or not hasattr(locality, "d"):
        return

    for parent in parent_node_ids:
        if node_otypes.get(parent) != parent_otype:
            continue
        try:
            # Locality downward lookup: embeddees of requested otype inside parent.
            children = locality.d(parent, otype=child_otype)
        except Exception:
            continue
        if not isinstance(children, IterableABC):
            continue
        for raw_child in children:
            child = _to_int(raw_child)
            if child is None:
                continue
            if node_otypes.get(child) != child_otype:
                continue
            yield {
                "source": parent,
                "target": child,
                "source_otype": parent_otype,
                "target_otype": child_otype,
                "rel_key": f"HIER:{parent_otype}:{child_otype}:{parent}:{child}",
                "props": {
                    "parent_otype": parent_otype,
                    "child_otype": child_otype,
                    # Preserve provenance so relation semantics are explicit in Neo4j.
                    "source_api": "tf.L.d",
                },
            }


def _normalize_target_entry(entry: Any) -> tuple[int, dict[str, Any]] | None:
    """Normalize one TF edge target entry to (target_id, properties)."""
    if isinstance(entry, Mapping):
        if "target" in entry:
            target = _to_int(entry["target"])
            if target is None:
                return None
            props = {str(k): _neo4j_safe(v) for k, v in entry.items() if k != "target" and v is not None}
            return target, props
        if len(entry) == 1:
            only_key = next(iter(entry))
            target = _to_int(only_key)
            if target is None:
                return None
            edge_value = entry[only_key]
            props = {} if edge_value is None else {"value": _neo4j_safe(edge_value)}
            return target, props
        return None

    if isinstance(entry, (list, tuple)):
        if not entry:
            return None
        target = _to_int(entry[0])
        if target is None:
            return None
        if len(entry) == 1:
            return target, {}
        edge_value = entry[1] if len(entry) == 2 else entry[1:]
        props = {} if edge_value is None else {"value": _neo4j_safe(edge_value)}
        return target, props

    target = _to_int(entry)
    if target is None:
        return None
    return target, {}


def _normalize_targets(targets: Any) -> Iterator[tuple[int, dict[str, Any]]]:
    """Normalize all target payload forms produced by TF edge APIs."""
    if targets is None:
        return
    if isinstance(targets, Mapping):
        for target, edge_value in targets.items():
            normalized_target = _to_int(target)
            if normalized_target is None:
                continue
            props = {} if edge_value is None else {"value": _neo4j_safe(edge_value)}
            yield normalized_target, props
        return
    if isinstance(targets, (set, list, tuple)):
        for entry in targets:
            normalized = _normalize_target_entry(entry)
            if normalized is not None:
                yield normalized
        return
    normalized = _normalize_target_entry(targets)
    if normalized is not None:
        yield normalized


def _iter_edge_feature_relationships(
    api: Any, node_ids: Sequence[int], node_otypes: Mapping[int, str], edge_name: str
) -> Iterator[dict[str, Any]]:
    """Yield relationship rows for one TF edge feature."""
    edge_obj = getattr(api.E, edge_name, None)
    if edge_obj is None or not hasattr(edge_obj, "f"):
        return

    for source in node_ids:
        source_otype = node_otypes.get(source)
        if source_otype is None:
            continue
        try:
            targets = edge_obj.f(source)
        except Exception:
            continue
        for target, edge_props in _normalize_targets(targets):
            target_otype = node_otypes.get(target)
            if target_otype is None:
                continue
            yield {
                "source": source,
                "target": target,
                # Carry labels now so MATCH clauses can stay label-selective (faster).
                "source_otype": source_otype,
                "target_otype": target_otype,
                "rel_key": f"{edge_name}:{source}:{target}",
                "props": edge_props,
            }


def _iter_next_relationships(
    node_ids: Sequence[int], relation_props: Mapping[str, Any] | None = None
) -> Iterator[dict[str, Any]]:
    """Yield forward adjacency links based on TF node id order."""
    previous: int | None = None
    props = dict(relation_props or {})
    for current in node_ids:
        if previous is not None:
            yield {
                "source": previous,
                "target": current,
                "rel_key": f"NEXT:{previous}:{current}",
                "props": dict(props),
            }
        previous = current


def _iter_previous_relationships(
    node_ids: Sequence[int], relation_props: Mapping[str, Any] | None = None
) -> Iterator[dict[str, Any]]:
    """Yield backward adjacency links based on TF node id order."""
    previous: int | None = None
    props = dict(relation_props or {})
    for current in node_ids:
        if previous is not None:
            yield {
                "source": current,
                "target": previous,
                "rel_key": f"PREVIOUS:{current}:{previous}",
                "props": dict(props),
            }
        previous = current


def _attach_otypes(
    rows: Iterable[dict[str, Any]], node_otypes: Mapping[int, str]
) -> Iterator[dict[str, Any]]:
    """Attach source/target node types to relationship rows."""
    for row in rows:
        source_otype = node_otypes.get(int(row["source"]))
        target_otype = node_otypes.get(int(row["target"]))
        if source_otype is None or target_otype is None:
            continue
        enriched = dict(row)
        enriched["source_otype"] = source_otype
        enriched["target_otype"] = target_otype
        yield enriched


def _batched(rows: Iterable[dict[str, Any]], batch_size: int) -> Iterator[list[dict[str, Any]]]:
    """Split an iterator of row payloads into fixed-size batches."""
    batch: list[dict[str, Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _escape_cypher_identifier(identifier: str) -> str:
    """Escape backticks for safe dynamic Cypher identifiers."""
    return identifier.replace("`", "``")


def _constraint_name_for_otype(otype: str) -> str:
    """Build a safe, stable Neo4j constraint name for one otype label."""
    sanitized = "".join(ch if ch.isalnum() else "_" for ch in otype).strip("_")
    sanitized = sanitized[:40] or "label"
    suffix = hashlib.md5(otype.encode("utf-8")).hexdigest()[:8]
    return f"tf2neo4j_tf_id_{sanitized}_{suffix}"


def _ensure_otype_constraints(
    session: Any, otypes: Sequence[str], progress: _ProgressReporter | None = None
) -> None:
    """Ensure each TF node label has a unique constraint on tf_id."""
    for otype in sorted(set(otypes)):
        escaped_label = _escape_cypher_identifier(otype)
        constraint_name = _constraint_name_for_otype(otype)
        session.run(
            f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
            f"FOR (n:`{escaped_label}`) REQUIRE n.tf_id IS UNIQUE"
        ).consume()
        if progress is not None:
            progress.log(f"Constraint ready for :{otype}(tf_id)")


def _flush_node_batch(session: Any, otype: str, rows: list[dict[str, Any]], use_merge: bool) -> int:
    """Write one batch of nodes of the same TF type."""
    if not rows:
        return 0
    escaped_label = _escape_cypher_identifier(otype)
    if use_merge:
        # Incremental mode: keep/upsert existing nodes.
        query = f"""
        UNWIND $rows AS row
        MERGE (n:`{escaped_label}` {{tf_id: row.tf_id}})
        SET n += row.props
        REMOVE n:TFNode
        """
    else:
        # Fresh-load mode: CREATE is faster after a full clear.
        query = f"""
        UNWIND $rows AS row
        CREATE (n:`{escaped_label}` {{tf_id: row.tf_id}})
        SET n += row.props
        """
    session.run(query, rows=rows).consume()
    return len(rows)


def _write_nodes(
    session: Any,
    rows: Iterable[dict[str, Any]],
    batch_size: int,
    use_merge: bool,
    progress: _ProgressCounter | None = None,
) -> int:
    """Write all node rows, grouping by TF node type."""
    buffered: dict[str, list[dict[str, Any]]] = {}
    written = 0
    for row in rows:
        otype = str(row["otype"])
        payload = {"tf_id": row["tf_id"], "props": row["props"]}
        bucket = buffered.setdefault(otype, [])
        bucket.append(payload)
        if len(bucket) >= batch_size:
            flushed = _flush_node_batch(session, otype, bucket, use_merge=use_merge)
            written += flushed
            if progress is not None:
                progress.update(flushed)
            buffered[otype] = []
    for otype, pending_rows in buffered.items():
        flushed = _flush_node_batch(session, otype, pending_rows, use_merge=use_merge)
        written += flushed
        if progress is not None:
            progress.update(flushed)
    return written


def _write_relationships(
    session: Any,
    relationship_type: str,
    rows: Iterable[dict[str, Any]],
    batch_size: int,
    use_merge: bool,
    progress: _ProgressCounter | None = None,
) -> int:
    """Write relationships of a single Neo4j relationship type."""
    escaped_rel_type = _escape_cypher_identifier(relationship_type)
    written = 0

    buffered: dict[tuple[str, str], list[dict[str, Any]]] = {}

    def flush_pair(source_otype: str, target_otype: str, pair_rows: list[dict[str, Any]]) -> int:
        if not pair_rows:
            return 0
        source_label = _escape_cypher_identifier(source_otype)
        target_label = _escape_cypher_identifier(target_otype)
        if use_merge:
            # Merge by rel_key so repeated runs are idempotent.
            query = f"""
            UNWIND $rows AS row
            MATCH (s:`{source_label}` {{tf_id: row.source}})
            MATCH (t:`{target_label}` {{tf_id: row.target}})
            MERGE (s)-[r:`{escaped_rel_type}` {{rel_key: row.rel_key}}]->(t)
            SET r += row.props
            """
        else:
            # Fresh-load mode avoids MERGE overhead.
            query = f"""
            UNWIND $rows AS row
            MATCH (s:`{source_label}` {{tf_id: row.source}})
            MATCH (t:`{target_label}` {{tf_id: row.target}})
            CREATE (s)-[r:`{escaped_rel_type}`]->(t)
            SET r.rel_key = row.rel_key
            SET r += row.props
            """
        session.run(query, rows=pair_rows).consume()
        return len(pair_rows)

    for row in rows:
        source_otype = str(row["source_otype"])
        target_otype = str(row["target_otype"])
        key = (source_otype, target_otype)
        payload = {"source": row["source"], "target": row["target"], "rel_key": row["rel_key"], "props": row["props"]}
        bucket = buffered.setdefault(key, [])
        bucket.append(payload)
        if len(bucket) >= batch_size:
            flushed = flush_pair(source_otype, target_otype, bucket)
            written += flushed
            if progress is not None:
                progress.update(flushed)
            buffered[key] = []

    for (source_otype, target_otype), pair_rows in buffered.items():
        flushed = flush_pair(source_otype, target_otype, pair_rows)
        written += flushed
        if progress is not None:
            progress.update(flushed)

    return written


def _write_mixed_relationship_types(
    session: Any,
    rows: Iterable[dict[str, Any]],
    batch_size: int,
    use_merge: bool,
    progress: _ProgressCounter | None = None,
) -> int:
    """Write rows that carry their own relationship type in `relationship_type`."""
    written = 0
    # Group by (relation type + source label + target label) for indexed MATCHes.
    buffered: dict[tuple[str, str, str], list[dict[str, Any]]] = {}

    def flush_group(
        relationship_type: str,
        source_otype: str,
        target_otype: str,
        group_rows: list[dict[str, Any]],
    ) -> int:
        if not group_rows:
            return 0
        escaped_rel_type = _escape_cypher_identifier(relationship_type)
        source_label = _escape_cypher_identifier(source_otype)
        target_label = _escape_cypher_identifier(target_otype)
        if use_merge:
            query = f"""
            UNWIND $rows AS row
            MATCH (s:`{source_label}` {{tf_id: row.source}})
            MATCH (t:`{target_label}` {{tf_id: row.target}})
            MERGE (s)-[r:`{escaped_rel_type}` {{rel_key: row.rel_key}}]->(t)
            SET r += row.props
            """
        else:
            query = f"""
            UNWIND $rows AS row
            MATCH (s:`{source_label}` {{tf_id: row.source}})
            MATCH (t:`{target_label}` {{tf_id: row.target}})
            CREATE (s)-[r:`{escaped_rel_type}`]->(t)
            SET r.rel_key = row.rel_key
            SET r += row.props
            """
        session.run(query, rows=group_rows).consume()
        return len(group_rows)

    for row in rows:
        relationship_type = str(row["relationship_type"])
        source_otype = str(row["source_otype"])
        target_otype = str(row["target_otype"])
        key = (relationship_type, source_otype, target_otype)
        payload = {
            "source": row["source"],
            "target": row["target"],
            "rel_key": row["rel_key"],
            "props": row["props"],
        }
        bucket = buffered.setdefault(key, [])
        bucket.append(payload)
        if len(bucket) >= batch_size:
            flushed = flush_group(relationship_type, source_otype, target_otype, bucket)
            written += flushed
            if progress is not None:
                progress.update(flushed)
            buffered[key] = []

    for (relationship_type, source_otype, target_otype), group_rows in buffered.items():
        flushed = flush_group(relationship_type, source_otype, target_otype, group_rows)
        written += flushed
        if progress is not None:
            progress.update(flushed)

    return written


def _clear_database(session: Any, clear_batch_size: int) -> None:
    """Delete the entire graph in bounded chunks."""
    while True:
        deleted = session.run(
            """
            MATCH (n)
            WITH n LIMIT $limit
            DETACH DELETE n
            RETURN count(*) AS deleted
            """,
            limit=clear_batch_size,
        ).single()["deleted"]
        if deleted == 0:
            break


def _clear_relationship_type(session: Any, relationship_type: str, clear_batch_size: int) -> None:
    """Delete all relationships of a specific type in bounded chunks."""
    escaped_rel_type = _escape_cypher_identifier(relationship_type)
    query = f"""
    MATCH ()-[r:`{escaped_rel_type}`]->()
    WITH r LIMIT $limit
    DELETE r
    RETURN count(*) AS deleted
    """
    while True:
        deleted = session.run(query, limit=clear_batch_size).single()["deleted"]
        if deleted == 0:
            break


def _tf_silent_value(value: str | bool | None) -> str:
    """Translate a permissive config silent value to TF's expected string levels."""
    if value is True:
        return "deep"
    if value is False:
        return "verbose"
    if value is None:
        return "terse"
    if value in {"verbose", "auto", "terse", "deep"}:
        return value
    return "auto"


def _load_tf_api(config: TFExportConfig) -> Any:
    """Load a Text-Fabric API instance using config settings."""
    locations = _as_list(config.tf_locations)
    modules = _as_list(config.tf_modules)
    tf_silent = _tf_silent_value(config.silent)
    tf = Fabric(locations=locations, modules=modules, silent=tf_silent)

    requested_node_features = list(config.node_features or [])
    requested_edge_features = list(config.edge_features or [])
    requested = " ".join(requested_node_features + requested_edge_features).strip()

    if requested:
        # Load only requested features when caller provided a list.
        api = tf.load(requested, silent=tf_silent)
    else:
        load_all = getattr(tf, "loadAll", None)
        if callable(load_all):
            # Preferred full-load API if available in this TF version.
            api = load_all(silent=tf_silent)
        else:
            # Backward-compatible fallback.
            api = tf.load("", silent=tf_silent)

    if not api:
        raise RuntimeError("Unable to load Text-Fabric API from dataset path/modules.")
    return api


def _is_local_uri(uri: str) -> bool:
    """Check whether the URI points to localhost."""
    host = urlparse(uri).hostname
    return host in {"localhost", "127.0.0.1", "::1"}


def _local_bolt_fallback(uri: str) -> str | None:
    """Build a bolt:// fallback URI for local neo4j:// addresses."""
    parsed = urlparse(uri)
    if not parsed.scheme.startswith("neo4j"):
        return None
    if not _is_local_uri(uri):
        return None
    return urlunparse(parsed._replace(scheme="bolt"))


def _connect_driver(config: TFExportConfig):
    """Connect to Neo4j, with local neo4j:// -> bolt:// fallback."""
    auth = (config.neo4j_user, config.neo4j_password)
    uris = [config.neo4j_uri]
    fallback_uri = _local_bolt_fallback(config.neo4j_uri)
    if fallback_uri and fallback_uri not in uris:
        uris.append(fallback_uri)

    last_error: Exception | None = None
    for uri in uris:
        driver = GraphDatabase.driver(uri, auth=auth)
        try:
            driver.verify_connectivity()
            return driver, uri
        except Exception as exc:
            last_error = exc
            driver.close()

    uri_hint = ""
    if config.neo4j_uri.startswith("neo4j://") and _is_local_uri(config.neo4j_uri):
        uri_hint = (
            " For local single-instance Neo4j Desktop/Server, use a direct URI like "
            "'bolt://localhost:7687' instead of 'neo4j://...'."
        )
    raise RuntimeError(
        f"Neo4j connection failed for URI '{config.neo4j_uri}'.{uri_hint}"
    ) from last_error


def export_text_fabric_to_neo4j(config: TFExportConfig) -> ExportStats:
    """Export TF nodes and edge features into Neo4j graph structures."""
    progress = _ProgressReporter(config)
    progress.log("Loading Text-Fabric API")
    api = _load_tf_api(config)

    selected_node_features = _dedupe([str(f) for f in (config.node_features or _call_maybe(api, "Fall")) if f])
    selected_edge_features = _dedupe([str(f) for f in (config.edge_features or _call_maybe(api, "Eall")) if f])
    selected_node_features = [f for f in selected_node_features if f not in {"oslots", "otext"}]
    node_ids = list(_iter_tf_nodes(api))
    node_otypes = _collect_node_otypes(api, node_ids)
    node_ids_by_otype = _group_node_ids_by_otype(node_ids, node_otypes)
    node_rows = _iter_nodes(api, node_otypes, selected_node_features)

    stats = ExportStats()
    progress.log(
        f"Prepared export: {len(node_ids)} nodes, {len(selected_edge_features)} edge features"
    )
    progress.log("Connecting to Neo4j")
    driver, _ = _connect_driver(config)
    with driver:
        with driver.session(database=config.neo4j_database) as session:
            if config.clear_database:
                progress.log("Clearing existing database")
                _clear_database(session, config.clear_batch_size)
            # Cleanup from earlier schema versions that used :TFNode.
            session.run("DROP CONSTRAINT tf_node_id IF EXISTS").consume()
            progress.log("Ensuring per-label tf_id constraints")
            _ensure_otype_constraints(session, list(node_otypes.values()), progress=progress)

            use_merge = not config.clear_database

            node_counter = progress.counter("Writing nodes", total=len(node_ids), unit="nodes")
            try:
                stats.node_count += _write_nodes(
                    session,
                    node_rows,
                    config.batch_size,
                    use_merge=use_merge,
                    progress=node_counter,
                )
            finally:
                node_counter.close()

            for edge_name in selected_edge_features:
                if edge_name == "frame" and config.frame_semantic_relations:
                    progress.log("Refreshing semantic frame relations")
                    frame_rel_types = {"frame", "HAS_FRAME_ROLE"} | {
                        rel_type for rel_type, _ in FRAME_ROLE_MAP.values()
                    }
                    for rel_type in sorted(frame_rel_types):
                        _clear_relationship_type(session, rel_type, config.clear_batch_size)

                    progress.log("Writing semantic frame relations")
                    edge_counter = progress.counter("Edge frame (semantic)", unit="rels")
                    rows = _iter_edge_feature_relationships(api, node_ids, node_otypes, edge_name)
                    semantic_rows = _iter_semantic_frame_relationships(rows)
                    try:
                        stats.relationship_count += _write_mixed_relationship_types(
                            session,
                            semantic_rows,
                            config.batch_size,
                            use_merge=False,
                            progress=edge_counter,
                        )
                    finally:
                        edge_counter.close()
                else:
                    progress.log(f"Writing edge feature '{edge_name}'")
                    edge_counter = progress.counter(f"Edge {edge_name}", unit="rels")
                    rows = _iter_edge_feature_relationships(api, node_ids, node_otypes, edge_name)
                    try:
                        stats.relationship_count += _write_relationships(
                            session,
                            edge_name,
                            rows,
                            config.batch_size,
                            use_merge=use_merge,
                            progress=edge_counter,
                        )
                    finally:
                        edge_counter.close()

            if config.hierarchy_node_types is not None:
                # Ordered hierarchy list drives adjacent parent->child links.
                selected_hierarchy_types, missing_hierarchy_types = _resolve_previous_next_types(
                    config.hierarchy_node_types, list(node_ids_by_otype.keys())
                )
                if missing_hierarchy_types:
                    progress.log(
                        "Skipped missing hierarchy types: " + ", ".join(missing_hierarchy_types)
                    )

                progress.log("Refreshing TF_HIERARCHY relationships")
                _clear_relationship_type(session, "TF_HIERARCHY", config.clear_batch_size)

                if len(selected_hierarchy_types) >= 2:
                    hierarchy_counter = progress.counter("Edge TF_HIERARCHY", unit="rels")
                    try:
                        for index in range(len(selected_hierarchy_types) - 1):
                            parent_type = selected_hierarchy_types[index]
                            child_type = selected_hierarchy_types[index + 1]
                            parent_ids = node_ids_by_otype.get(parent_type, [])
                            if not parent_ids:
                                continue
                            progress.log(f"Hierarchy {parent_type} -> {child_type}")
                            rows = _iter_hierarchy_relationships(
                                api,
                                parent_ids,
                                node_otypes,
                                parent_type,
                                child_type,
                            )
                            stats.relationship_count += _write_relationships(
                                session,
                                "TF_HIERARCHY",
                                rows,
                                config.batch_size,
                                use_merge=False,
                                progress=hierarchy_counter,
                            )
                    finally:
                        hierarchy_counter.close()
                else:
                    progress.log("Hierarchy list has fewer than two valid node types")

            if config.add_previous_next:
                # Build sequences independently per selected otype.
                selected_link_types, missing_link_types = _resolve_previous_next_types(
                    config.previous_next_node_types, list(node_ids_by_otype.keys())
                )
                if missing_link_types:
                    progress.log(
                        "Skipped missing previous/next types: " + ", ".join(missing_link_types)
                    )

                progress.log("Refreshing NEXT/PREVIOUS relationships")
                _clear_relationship_type(session, "NEXT", config.clear_batch_size)
                _clear_relationship_type(session, "PREVIOUS", config.clear_batch_size)

                total_links = sum(max(0, len(node_ids_by_otype.get(t, [])) - 1) for t in selected_link_types)

                progress.log("Writing NEXT relationships")
                next_counter = progress.counter("Edge NEXT", total=total_links, unit="rels")
                try:
                    for node_type in selected_link_types:
                        node_type_ids = node_ids_by_otype.get(node_type, [])
                        if len(node_type_ids) < 2:
                            continue
                        rows = _attach_otypes(
                            _iter_next_relationships(
                                node_type_ids, relation_props={"sequence_otype": node_type}
                            ),
                            node_otypes,
                        )
                        stats.relationship_count += _write_relationships(
                            session,
                            "NEXT",
                            rows,
                            config.batch_size,
                            use_merge=False,
                            progress=next_counter,
                        )
                finally:
                    next_counter.close()

                progress.log("Writing PREVIOUS relationships")
                prev_counter = progress.counter(
                    "Edge PREVIOUS", total=total_links, unit="rels"
                )
                try:
                    for node_type in selected_link_types:
                        node_type_ids = node_ids_by_otype.get(node_type, [])
                        if len(node_type_ids) < 2:
                            continue
                        rows = _attach_otypes(
                            _iter_previous_relationships(
                                node_type_ids, relation_props={"sequence_otype": node_type}
                            ),
                            node_otypes,
                        )
                        stats.relationship_count += _write_relationships(
                            session,
                            "PREVIOUS",
                            rows,
                            config.batch_size,
                            use_merge=False,
                            progress=prev_counter,
                        )
                finally:
                    prev_counter.close()
            else:
                progress.log("Removing NEXT/PREVIOUS relationships")
                _clear_relationship_type(session, "NEXT", config.clear_batch_size)
                _clear_relationship_type(session, "PREVIOUS", config.clear_batch_size)

    progress.log(
        f"Export complete: {stats.node_count} nodes, {stats.relationship_count} relationships"
    )
    return stats
