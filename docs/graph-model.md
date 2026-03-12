# Graph Model

## Nodes

- Neo4j nodes use Text-Fabric `otype` labels directly (for example `:book`, `:chapter`, `:verse`, `:word`)
- Node property `tf_id` stores the TF node id
- Additional TF node features are copied as node properties

## Relationships

- Each TF edge feature becomes a Neo4j relationship type
- Relation direction is source TF node -> target TF node

### Optional relation families

- `NEXT` / `PREVIOUS`
  - Sequential links by node type (only for configured types)
- `TF_HIERARCHY`
  - Locality-based hierarchy from ordered type list, built via `L.d`
- Frame semantics
  - `HAS_AGENT`, `HAS_PATIENT`, `HAS_RECIPIENT`, `HAS_ADVERBIAL`

## Suggested validation queries

```cypher
MATCH (n) WHERE n.tf_id IS NOT NULL
UNWIND labels(n) AS l
RETURN l, count(*) AS c
ORDER BY c DESC;
```

```cypher
MATCH ()-[r]->()
RETURN type(r) AS rel_type, count(*) AS c
ORDER BY c DESC
LIMIT 30;
```

