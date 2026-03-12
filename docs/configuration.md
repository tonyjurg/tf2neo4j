# Configuration Reference

`TFExportConfig` controls export behavior.

## Required

- `tf_locations`: path(s) to Text-Fabric data

## Neo4j connection

- `neo4j_uri` (default: `bolt://localhost:7687`)
- `neo4j_user` (default: `neo4j`)
- `neo4j_password`
- `neo4j_database` (default: `neo4j`)

## Feature selection

- `node_features`: `None` means all node features
- `edge_features`: `None` means all edge features

## Performance and write behavior

- `batch_size`: write batch size for nodes/relations
- `clear_database`: full graph clear before import
- `clear_batch_size`: batch size used for clear/delete operations

## Sequence links

- `add_previous_next`: enable `NEXT`/`PREVIOUS` relations
- `previous_next_node_types`: list of node types to sequence (for example `["book", "chapter", "verse", "word"]`)

## Hierarchy links (locality-based)

- `hierarchy_node_types`: ordered type list used to create `TF_HIERARCHY` using Text-Fabric `L.d`
  - Example: `["book", "chapter", "verse", "word"]`
  - Creates pairwise hierarchy levels: `book->chapter`, `chapter->verse`, `verse->word`

## Frame semantics

- `frame_semantic_relations`: when `True`, frame roles are converted:
  - `A0` -> `HAS_AGENT`
  - `A1` -> `HAS_PATIENT`
  - `A2` -> `HAS_RECIPIENT`
  - `AA2` -> `HAS_ADVERBIAL`

## Output verbosity

- `silent`: Text-Fabric verbosity (`"verbose"`, `"auto"`, `"terse"`, `"deep"`)
- `show_progress`: enable progress output
- `progress_use_tqdm`: use `tqdm` when available
- `progress_every`: print fallback interval

