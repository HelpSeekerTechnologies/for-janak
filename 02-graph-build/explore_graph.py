import sys; sys.stdout.reconfigure(encoding='utf-8')
from neo4j import GraphDatabase
d = GraphDatabase.driver('<YOUR_NEO4J_AURA_URI>', auth=('neo4j','<YOUR_NEO4J_AURA_PASSWORD>'))
s = d.session()

print("=== RELATIONSHIP TYPES ===")
for rec in s.run('MATCH ()-[r]->() RETURN type(r) AS rel, count(r) AS cnt ORDER BY cnt DESC LIMIT 30'):
    print(f"  {rec['rel']}: {rec['cnt']}")

print("\n=== EXISTING Organization SAMPLE ===")
for rec in s.run('MATCH (o:Organization) RETURN o LIMIT 2'):
    props = dict(rec['o'])
    print(f"  keys: {list(props.keys())}")
    print(f"  bn: {props.get('bn')}, name: {props.get('name')}")

print("\n=== EXISTING Director/Person SAMPLE ===")
for rec in s.run('MATCH (p:Person) RETURN p LIMIT 2'):
    props = dict(rec['p'])
    print(f"  keys: {list(props.keys())}")

print("\n=== OrgEntity canonical_id list ===")
for rec in s.run('MATCH (n:OrgEntity) RETURN n.canonical_id AS cid, n.name AS name ORDER BY n.canonical_id LIMIT 10'):
    print(f"  {rec['cid']}: {rec['name']}")

print("\n=== Existing RECEIVED_GRANT? ===")
cnt = s.run('MATCH ()-[r:RECEIVED_GRANT]->() RETURN count(r) AS c').single()['c']
print(f"  RECEIVED_GRANT: {cnt}")

cnt = s.run('MATCH ()-[r:SITS_ON]->() RETURN count(r) AS c').single()['c']
print(f"  SITS_ON: {cnt}")

cnt = s.run('MATCH ()-[r:FLAGGED_AS]->() RETURN count(r) AS c').single()['c']
print(f"  FLAGGED_AS: {cnt}")

cnt = s.run('MATCH ()-[r:LOCATED_IN]->() RETURN count(r) AS c').single()['c']
print(f"  LOCATED_IN: {cnt}")

cnt = s.run('MATCH ()-[r:SHARED_DIRECTORS]->() RETURN count(r) AS c').single()['c']
print(f"  SHARED_DIRECTORS: {cnt}")

print("\n=== Existing RiskFlag nodes ===")
for rec in s.run('MATCH (f:RiskFlag) RETURN f.flag_type AS ft LIMIT 20'):
    print(f"  {rec['ft']}")

d.close()
