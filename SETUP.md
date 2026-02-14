# Setup — Credential Configuration

Before running any scripts, replace the following placeholders with your own credentials.

## Required Credentials

| Placeholder | What It Is | Where to Get It |
|-------------|-----------|-----------------|
| `<YOUR_NEO4J_AURA_URI>` | Neo4j Aura connection URI (e.g., `neo4j+s://xxxxx.databases.neo4j.io`) | [Neo4j Aura Console](https://console.neo4j.io/) |
| `<YOUR_NEO4J_AURA_PASSWORD>` | Neo4j Aura instance password | Set when creating Aura instance |
| `<YOUR_DATABRICKS_TOKEN>` | Databricks personal access token (starts with `dapi`) | Databricks > User Settings > Developer > Access Tokens |
| `<YOUR_DATABRICKS_HOST>` | Databricks workspace URL (e.g., `adb-xxxxx.xx.azuredatabricks.net`) | Your Databricks workspace URL bar |
| `<YOUR_DATABRICKS_SQL_WAREHOUSE>` | SQL warehouse endpoint path (e.g., `/sql/1.0/warehouses/xxxxxxxx`) | Databricks > SQL Warehouses > Connection Details |
| `<YOUR_NEO4J_LOCAL_PASSWORD>` | Password for local Docker Neo4j (only if running locally) | You choose this |

## Quick Find & Replace

From the repo root:

```bash
# Neo4j Aura
grep -rl '<YOUR_NEO4J_AURA_URI>' --include='*.py' --include='*.md' | xargs sed -i 's|<YOUR_NEO4J_AURA_URI>|neo4j+s://YOUR-INSTANCE.databases.neo4j.io|g'
grep -rl '<YOUR_NEO4J_AURA_PASSWORD>' --include='*.py' --include='*.md' | xargs sed -i 's|<YOUR_NEO4J_AURA_PASSWORD>|YOUR_PASSWORD_HERE|g'

# Databricks
grep -rl '<YOUR_DATABRICKS_TOKEN>' --include='*.py' --include='*.md' | xargs sed -i 's|<YOUR_DATABRICKS_TOKEN>|dapiXXXXXXXXXXXXXXXX|g'
grep -rl '<YOUR_DATABRICKS_HOST>' --include='*.py' --include='*.md' | xargs sed -i 's|<YOUR_DATABRICKS_HOST>|adb-XXXXX.XX.azuredatabricks.net|g'
grep -rl '<YOUR_DATABRICKS_SQL_WAREHOUSE>' --include='*.py' --include='*.md' | xargs sed -i 's|<YOUR_DATABRICKS_SQL_WAREHOUSE>|/sql/1.0/warehouses/XXXXXXXX|g'
```

## Files Containing Placeholders

- `CLAUDE.md` — project context (Neo4j + Databricks)
- `01-data-assembly/*.py` — data assembly scripts (Databricks)
- `02-graph-build/*.py` — graph ingestion scripts (Neo4j)
- `03-governance-queries/*.py` — query scripts (Neo4j)
- `04-synthesis/methodology.md` — methodology doc (Neo4j)
- `05-html-artifacts/ministry_lineage_query.py` — visualization (Neo4j)
- `06-validation/statistical_tests.py` — validation (Neo4j)
