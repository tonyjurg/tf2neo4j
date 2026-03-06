# tf2neo4j

Jupyter-first tool to export a Text-Fabric dataset into Neo4j.

## What it does

- Creates/updates `(:TFNode {tf_id, otype, ...features})` for TF nodes
- Creates/updates `[:TF_EDGE {name, ...props}]` for TF edge-features
- Supports batched writes and optional full database clear before import

## Quick start

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Start Jupyter:
   ```bash
   jupyter notebook
   ```
3. Open `notebooks/tf_to_neo4j.ipynb`
4. Set `TF_LOCATIONS`, Neo4j credentials, and run all cells

## Python API

```python
from tf2neo4j import TFExportConfig, export_text_fabric_to_neo4j

config = TFExportConfig(
    tf_locations=r"D:\path\to\tf\dataset",
    neo4j_uri="bolt://localhost:7687",
    neo4j_user="neo4j",
    neo4j_password="password",
    neo4j_database="neo4j",
    node_features=None,   # None => all node features
    edge_features=None,   # None => all edge features
    batch_size=2000,
    clear_database=False,
)

stats = export_text_fabric_to_neo4j(config)
print(stats)
```
