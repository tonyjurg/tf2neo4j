# Quick Start

## 1. Install dependencies

```bash
pip install -r requirements.txt
```

## 2. Start Jupyter

```bash
jupyter notebook
```

## 3. Open notebook

Open:

`notebooks/tf_to_neo4j.ipynb`

## 4. Configure and run

Set at least:

- `TF_LOCATIONS`
- Neo4j credentials (`NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`)

Then run all cells.

## 5. First import recommendation

- Use `CLEAR_DATABASE = True` for a clean first run
- Then switch to `CLEAR_DATABASE = False` for normal incremental reruns

