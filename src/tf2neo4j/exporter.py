"""Export Text-Fabric datasets into Neo4j."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Mapping, Sequence

from neo4j import GraphDatabase
from tf.fabric import Fabric


@dataclass
class ExportStats:
    node_count: int = 0
    relationship_count: int = 0


@dataclass
class TFExportConfig:
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
    silent: bool = True


def _as_list(value: str | Sequence[str] | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    return list(value)


def _neo4j_safe(value: Any) -> Any:
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
    fn = getattr(api, name, None)
    if fn is None:
        return []
    data = fn()
    if not data:
        return []
    return list(data)


def _iter_nodes(api: Any, selected_features: Sequence[str]) -> Iterator[dict[str, Any]]:
    otype = api.F.otype
    for raw_node in otype.s():
        node = int(raw_node)
        props: dict[str, Any] = {"otype": otype.v(node)}
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
        yield {"tf_id": node, "props": props}


def _normalize_targets(targets: Any) -> Iterator[tuple[int, dict[str, Any]]]:
    if targets is None:
        return
    if isinstance(targets, Mapping):
        for target, edge_value in targets.items():
            props = {} if edge_value is None else {"value": _neo4j_safe(edge_value)}
            yield int(target), props
        return
    if isinstance(targets, (set, list, tuple)):
        for target in targets:
            yield int(target), {}
        return
    yield int(targets), {}


def _iter_relationships(api: Any, selected_edge_features: Sequence[str]) -> Iterator[dict[str, Any]]:
    for edge_name in selected_edge_features:
        edge_obj = getattr(api.E, edge_name, None)
        if edge_obj is None or not hasattr(edge_obj, "f"):
            continue
        for raw_source in api.F.otype.s():
            source = int(raw_source)
            try:
                targets = edge_obj.f(source)
            except Exception:
                continue
            for target, edge_props in _normalize_targets(targets):
                rel_props = {"name": edge_name}
                rel_props.update(edge_props)
                yield {
                    "source": source,
                    "target": target,
                    "name": edge_name,
                    "rel_key": f"{edge_name}:{source}:{target}",
                    "props": rel_props,
                }


def _batched(rows: Iterable[dict[str, Any]], batch_size: int) -> Iterator[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _load_tf_api(config: TFExportConfig) -> Any:
    locations = _as_list(config.tf_locations)
    modules = _as_list(config.tf_modules)
    tf = Fabric(locations=locations, modules=modules, silent=config.silent)

    requested_node_features = list(config.node_features or [])
    requested_edge_features = list(config.edge_features or [])
    requested = " ".join(requested_node_features + requested_edge_features).strip()

    if requested:
        api = tf.load(requested, silent=config.silent)
    else:
        load_all = getattr(tf, "loadAll", None)
        if callable(load_all):
            api = load_all(silent=config.silent)
        else:
            api = tf.load("", silent=config.silent)

    if not api:
        raise RuntimeError("Unable to load Text-Fabric API from dataset path/modules.")
    return api


def export_text_fabric_to_neo4j(config: TFExportConfig) -> ExportStats:
    """Copy Text-Fabric nodes and edge features into Neo4j."""
    api = _load_tf_api(config)

    selected_node_features = list(config.node_features or _call_maybe(api, "Fall"))
    selected_edge_features = list(config.edge_features or _call_maybe(api, "Eall"))
    selected_node_features = [f for f in selected_node_features if f not in {"oslots", "otext"}]

    node_rows = _iter_nodes(api, selected_node_features)
    relationship_rows = _iter_relationships(api, selected_edge_features)

    node_query = """
    UNWIND $rows AS row
    MERGE (n:TFNode {tf_id: row.tf_id})
    SET n += row.props
    """
    relationship_query = """
    UNWIND $rows AS row
    MATCH (s:TFNode {tf_id: row.source})
    MATCH (t:TFNode {tf_id: row.target})
    MERGE (s)-[r:TF_EDGE {rel_key: row.rel_key}]->(t)
    SET r += row.props
    """

    stats = ExportStats()
    with GraphDatabase.driver(config.neo4j_uri, auth=(config.neo4j_user, config.neo4j_password)) as driver:
        with driver.session(database=config.neo4j_database) as session:
            if config.clear_database:
                session.run("MATCH (n) DETACH DELETE n")

            session.run(
                "CREATE CONSTRAINT tf_node_id IF NOT EXISTS FOR (n:TFNode) REQUIRE n.tf_id IS UNIQUE"
            )

            for batch in _batched(node_rows, config.batch_size):
                session.run(node_query, rows=batch)
                stats.node_count += len(batch)

            for batch in _batched(relationship_rows, config.batch_size):
                session.run(relationship_query, rows=batch)
                stats.relationship_count += len(batch)

    return stats
