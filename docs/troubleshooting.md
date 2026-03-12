# Troubleshooting

## Slowdown during long imports

Potential causes:

- No useful constraints/index usage
- Very large batch sizes for current machine

Try:

- `CLEAR_DATABASE = True` for a clean first import
- Lower `BATCH_SIZE` (for example `500` or `1000`)
- Lower `CLEAR_BATCH_SIZE` if delete operations hit memory limits

## MemoryPoolOutOfMemoryError

This is usually transaction memory pressure, not a community-edition feature limit.

Try:

- Reduce `BATCH_SIZE`
- Reduce `CLEAR_BATCH_SIZE`
- Increase Neo4j memory settings if needed

## Routing / connectivity errors

If local Neo4j uses a single instance:

- Prefer `bolt://localhost:7687` over `neo4j://...`

## Label warning for `TFNode`

If you see warnings about missing `TFNode`, update old checks/queries.
Current exporter does not rely on a shared `TFNode` label.

