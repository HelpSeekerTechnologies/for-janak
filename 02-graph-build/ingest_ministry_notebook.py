# Databricks notebook source
# MAGIC %md
# MAGIC # Ministry Genealogy Graph — Neo4j Ingestion (Merged)
# MAGIC
# MAGIC Loads merged ministry genealogy data from Unity Catalog Volume into Neo4j.
# MAGIC
# MAGIC **Graph:** 264 nodes, 317 relationships
# MAGIC - 42 SourceDocument (⟦ record)
# MAGIC - 142 OrgEntity (▣ program)
# MAGIC - 66 TransformEvent (⧫ event)
# MAGIC - 14 ResourceAllocation (◉ resource)
# MAGIC
# MAGIC **Pre-requisites:**
# MAGIC 1. Upload all CSVs from `merged/data/` to a Unity Catalog Volume
# MAGIC 2. Store Neo4j credentials in Databricks Secrets
# MAGIC 3. Cluster has internet access to reach Neo4j Aura

# COMMAND ----------

# MAGIC %pip install neo4j
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set the Volume path where the merged CSVs are stored, and the Databricks
# MAGIC secret scope/keys for Neo4j credentials.

# COMMAND ----------

import pandas as pd
import json
import os
import re
import time
from typing import Optional, List, Dict, Any
from collections import defaultdict
from neo4j import GraphDatabase
from neo4j.exceptions import TransientError

# COMMAND ----------

# --- EDIT THESE ---
VOLUME_PATH = "/Volumes/dbw_unitycatalog_test/uploads/uploaded_files/Ministry Data/"  # Unity Catalog Volume

# Neo4j Aura connection (set via environment or Databricks secrets)
AURA_URI = os.getenv("AURA_URI", "<YOUR_NEO4J_AURA_URI>")
AURA_USER = os.getenv("AURA_USER", "neo4j")
AURA_PASSWORD = os.getenv("AURA_PASSWORD", "<YOUR_NEO4J_AURA_PASSWORD>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Credentials

# COMMAND ----------

NEO4J_URI = AURA_URI
NEO4J_USER = AURA_USER
NEO4J_PASSWORD = AURA_PASSWORD
        

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load CSV Data from Volume

# COMMAND ----------

import csv
import os
from collections import defaultdict

# KGL glyph/handle mapping per NG1/NG2
KGL_MAP = {
    'OrgEntity':          ('▣', 'program'),
    'TransformEvent':     ('⧫', 'event'),
    'SourceDocument':     ('⟦', 'record'),
    'ResourceAllocation': ('◉', 'resource'),
}


def build_source_kgl_sequence(doc_type, jurisdiction='alberta'):
    """Build KGL sequence for SourceDocument from doc_type.
    Pattern: ⟦ -> Ϡ:{doc_type} -> ᚪ:{jurisdiction}
    """
    dt = doc_type.strip().lower().replace(' ', '_') if doc_type else 'unknown'
    return f"⟦ -> Ϡ:{dt} -> ᚪ:{jurisdiction}"


def read_csv_from_volume(filename):
    """Read a CSV from the Unity Catalog Volume. Returns list of dicts."""
    path = os.path.join(VOLUME_PATH, filename)
    with open(path, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def load_data():
    """Load all merged CSV files from Volume."""
    data = {}
    data['org_entities'] = read_csv_from_volume('org_entities.csv')
    data['transform_events'] = read_csv_from_volume('transform_events.csv')
    data['sources'] = read_csv_from_volume('source-registry.csv')
    data['resource_allocations'] = read_csv_from_volume('resource_allocations.csv')
    data['edges_source_of'] = read_csv_from_volume('edges_source_of.csv')
    data['edges_target_of'] = read_csv_from_volume('edges_target_of.csv')
    data['edges_evidenced_by'] = read_csv_from_volume('edges_evidenced_by.csv')
    data['edges_parent_of'] = read_csv_from_volume('edges_parent_of.csv')
    return data


print("Loading merged data from Volume...")
data = load_data()

print(f"  OrgEntity nodes:           {len(data['org_entities'])}")
print(f"  TransformEvent nodes:      {len(data['transform_events'])}")
print(f"  SourceDocument nodes:      {len(data['sources'])}")
print(f"  ResourceAllocation nodes:  {len(data['resource_allocations'])}")
print(f"  SOURCE_OF edges:           {len(data['edges_source_of'])}")
print(f"  TARGET_OF edges:           {len(data['edges_target_of'])}")
print(f"  EVIDENCED_BY edges:        {len(data['edges_evidenced_by'])}")
print(f"  PARENT_OF edges:           {len(data['edges_parent_of'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate Referential Integrity

# COMMAND ----------

def validate(data):
    """Check referential integrity across all datasets."""
    errors = []
    warnings = []

    entity_ids = {row['canonical_id'] for row in data['org_entities']}
    event_ids = {row['event_id'] for row in data['transform_events']}
    source_ids = {row['source_id'] for row in data['sources']}

    # SOURCE_OF edges
    for edge in data['edges_source_of']:
        if edge['source_entity_id'] not in entity_ids:
            errors.append(f"edges_source_of: entity '{edge['source_entity_id']}' not in org_entities")
        if edge['event_id'] not in event_ids:
            errors.append(f"edges_source_of: event '{edge['event_id']}' not in transform_events")

    # TARGET_OF edges
    for edge in data['edges_target_of']:
        if edge['target_entity_id'] not in entity_ids:
            errors.append(f"edges_target_of: entity '{edge['target_entity_id']}' not in org_entities")
        if edge['event_id'] not in event_ids:
            errors.append(f"edges_target_of: event '{edge['event_id']}' not in transform_events")

    # EVIDENCED_BY edges
    for edge in data['edges_evidenced_by']:
        if edge['event_id'] not in event_ids:
            errors.append(f"edges_evidenced_by: event '{edge['event_id']}' not in transform_events")
        if edge['doc_id'] not in source_ids:
            errors.append(f"edges_evidenced_by: doc '{edge['doc_id']}' not in source-registry")

    # PARENT_OF edges
    for edge in data['edges_parent_of']:
        if edge['parent_id'] not in entity_ids:
            errors.append(f"edges_parent_of: parent '{edge['parent_id']}' not in org_entities")
        if edge['child_id'] not in entity_ids:
            errors.append(f"edges_parent_of: child '{edge['child_id']}' not in org_entities")

    # ResourceAllocation program_id
    for ra in data['resource_allocations']:
        if ra['program_id'] and ra['program_id'] not in entity_ids:
            errors.append(f"resource_allocations: program '{ra['program_id']}' not in org_entities")

    # Event coverage checks
    events_with_source = {e['event_id'] for e in data['edges_source_of']}
    events_with_target = {e['event_id'] for e in data['edges_target_of']}
    events_with_evidence = {e['event_id'] for e in data['edges_evidenced_by']}
    for evid in event_ids:
        if evid not in events_with_source and evid not in events_with_target:
            warnings.append(f"Event '{evid}' has no SOURCE_OF or TARGET_OF edges")
        if evid not in events_with_evidence:
            warnings.append(f"Event '{evid}' has no EVIDENCED_BY edge")

    # Cardinality checks
    source_counts = defaultdict(int)
    target_counts = defaultdict(int)
    for e in data['edges_source_of']:
        source_counts[e['event_id']] += 1
    for e in data['edges_target_of']:
        target_counts[e['event_id']] += 1

    event_type_map = {e['event_id']: e['event_type'] for e in data['transform_events']}
    for evid, etype in event_type_map.items():
        sc = source_counts.get(evid, 0)
        tc = target_counts.get(evid, 0)
        if etype == 'CREATE' and sc > 0:
            warnings.append(f"CREATE event '{evid}' has {sc} source entities (expected 0)")
        if etype == 'DISSOLVE' and tc > 0:
            warnings.append(f"DISSOLVE event '{evid}' has {tc} target entities (expected 0)")
        if etype == 'RENAME' and (sc != 1 or tc != 1):
            warnings.append(f"RENAME event '{evid}' has {sc} sources, {tc} targets (expected 1:1)")
        if etype == 'SPLIT' and sc != 1:
            warnings.append(f"SPLIT event '{evid}' has {sc} sources (expected 1)")
        if etype == 'MERGE' and tc != 1:
            warnings.append(f"MERGE event '{evid}' has {tc} targets (expected 1)")

    return errors, warnings


errors, warnings = validate(data)

if errors:
    print(f"\nERRORS ({len(errors)}):")
    for e in errors:
        print(f"  [ERROR] {e}")
    raise ValueError(f"{len(errors)} referential integrity errors found — fix before ingestion")

if warnings:
    print(f"\nWARNINGS ({len(warnings)}):")
    for w in warnings:
        print(f"  [WARN] {w}")

print(f"\nValidation PASSED ({len(warnings)} warnings)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Connect to Neo4j and Run Schema

# COMMAND ----------

from neo4j import GraphDatabase

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Verify connectivity
with driver.session() as session:
    result = session.run("MATCH (n) RETURN count(n) AS cnt")
    cnt = result.single()['cnt']
    print(f"Connected to Neo4j: {cnt} existing nodes")

# COMMAND ----------

# Run schema constraints and indexes
SCHEMA_STATEMENTS = [
    # Constraints
    "CREATE CONSTRAINT org_entity_canonical_id IF NOT EXISTS FOR (o:OrgEntity) REQUIRE o.canonical_id IS UNIQUE",
    "CREATE CONSTRAINT transform_event_id IF NOT EXISTS FOR (e:TransformEvent) REQUIRE e.event_id IS UNIQUE",
    "CREATE CONSTRAINT source_document_id IF NOT EXISTS FOR (d:SourceDocument) REQUIRE d.doc_id IS UNIQUE",
    "CREATE CONSTRAINT resource_allocation_id IF NOT EXISTS FOR (r:ResourceAllocation) REQUIRE r.allocation_id IS UNIQUE",
    # Indexes
    "CREATE INDEX org_entity_name IF NOT EXISTS FOR (o:OrgEntity) ON (o.name)",
    "CREATE INDEX org_entity_level IF NOT EXISTS FOR (o:OrgEntity) ON (o.level)",
    "CREATE INDEX org_entity_status IF NOT EXISTS FOR (o:OrgEntity) ON (o.status)",
    "CREATE INDEX transform_event_type IF NOT EXISTS FOR (e:TransformEvent) ON (e.event_type)",
    "CREATE INDEX transform_event_date IF NOT EXISTS FOR (e:TransformEvent) ON (e.event_date)",
    # NG6: KGL cross-ontology indexes
    "CREATE INDEX kgl_glyph_org IF NOT EXISTS FOR (o:OrgEntity) ON (o.kgl)",
    "CREATE INDEX kgl_handle_org IF NOT EXISTS FOR (o:OrgEntity) ON (o.kgl_handle)",
    "CREATE INDEX kgl_glyph_event IF NOT EXISTS FOR (e:TransformEvent) ON (e.kgl)",
    "CREATE INDEX kgl_handle_event IF NOT EXISTS FOR (e:TransformEvent) ON (e.kgl_handle)",
    "CREATE INDEX kgl_glyph_doc IF NOT EXISTS FOR (d:SourceDocument) ON (d.kgl)",
    "CREATE INDEX kgl_handle_doc IF NOT EXISTS FOR (d:SourceDocument) ON (d.kgl_handle)",
    "CREATE INDEX kgl_glyph_resource IF NOT EXISTS FOR (r:ResourceAllocation) ON (r.kgl)",
    "CREATE INDEX kgl_handle_resource IF NOT EXISTS FOR (r:ResourceAllocation) ON (r.kgl_handle)",
]

with driver.session() as session:
    for stmt in SCHEMA_STATEMENTS:
        session.run(stmt)
print(f"Schema applied: {len(SCHEMA_STATEMENTS)} constraints/indexes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingest Nodes

# COMMAND ----------

kgl_org, handle_org = KGL_MAP['OrgEntity']
kgl_evt, handle_evt = KGL_MAP['TransformEvent']
kgl_doc, handle_doc = KGL_MAP['SourceDocument']
kgl_res, handle_res = KGL_MAP['ResourceAllocation']

executed = 0
errors_count = 0

# --- SourceDocument nodes ---
print("Ingesting SourceDocument nodes...")
with driver.session() as session:
    for src in data['sources']:
        params = {
            'doc_id': src['source_id'],
            'doc_type': src.get('document_type', ''),
            'url': src.get('url', ''),
            'title': src.get('title', ''),
            'summary': src.get('summary', ''),
            'issuing_body': src.get('issuing_body', ''),
            'authority': src.get('authority_type', ''),
            'pub_date': src.get('document_date', '').strip() or None,
            'effective_date': src.get('effective_date', '').strip() or None,
            'kgl': kgl_doc,
            'kgl_handle': handle_doc,
            'kgl_sequence': build_source_kgl_sequence(src.get('document_type', '')),
        }
        try:
            cypher = (
                "MERGE (d:SourceDocument {doc_id: $doc_id}) "
                "ON CREATE SET d.doc_type=$doc_type, d.url=$url, d.title=$title, "
                "d.summary=$summary, d.issuing_body=$issuing_body, d.authority=$authority, "
                "d.kgl=$kgl, d.kgl_handle=$kgl_handle, d.kgl_sequence=$kgl_sequence"
            )
            if params['pub_date']:
                cypher += ", d.pub_date=date($pub_date)"
            if params['effective_date']:
                cypher += ", d.effective_date=date($effective_date)"
            session.run(cypher, params)
            executed += 1
        except Exception as e:
            errors_count += 1
            print(f"  ERROR [{src['source_id']}]: {e}")

print(f"  {len(data['sources'])} SourceDocument nodes processed")

# --- OrgEntity nodes ---
print("Ingesting OrgEntity nodes...")
with driver.session() as session:
    for ent in data['org_entities']:
        params = {
            'canonical_id': ent['canonical_id'],
            'name': ent['name'],
            'level': ent['level'],
            'status': ent['status'],
            'normalized_name': ent.get('normalized_name', ''),
            'aliases': ent.get('aliases', ''),
            'jurisdiction': ent.get('jurisdiction', 'alberta'),
            'kgl_sequence': ent.get('kgl_sequence', ''),
            'start_date': ent.get('start_date', '').strip() or None,
            'end_date': ent.get('end_date', '').strip() or None,
            'kgl': kgl_org,
            'kgl_handle': handle_org,
        }
        try:
            cypher = (
                "MERGE (e:OrgEntity {canonical_id: $canonical_id}) "
                "ON CREATE SET e.name=$name, e.level=$level, e.status=$status, "
                "e.normalized_name=$normalized_name, e.aliases=$aliases, "
                "e.jurisdiction=$jurisdiction, e.kgl_sequence=$kgl_sequence, "
                "e.kgl=$kgl, e.kgl_handle=$kgl_handle"
            )
            if params['start_date']:
                cypher += ", e.start_date=date($start_date)"
            if params['end_date']:
                cypher += ", e.end_date=date($end_date)"
            cypher += (
                " ON MATCH SET e.name=coalesce($name, e.name), "
                "e.status=coalesce($status, e.status)"
            )
            if params['end_date']:
                cypher += ", e.end_date=coalesce(date($end_date), e.end_date)"
            session.run(cypher, params)
            executed += 1
        except Exception as e:
            errors_count += 1
            print(f"  ERROR [{ent['canonical_id']}]: {e}")

print(f"  {len(data['org_entities'])} OrgEntity nodes processed")

# --- TransformEvent nodes ---
print("Ingesting TransformEvent nodes...")
with driver.session() as session:
    for evt in data['transform_events']:
        params = {
            'event_id': evt['event_id'],
            'event_type': evt['event_type'],
            'event_date': evt.get('event_date', '').strip() or None,
            'effective_fy': evt.get('effective_fy', ''),
            'confidence': evt.get('confidence', ''),
            'evidence_basis': evt.get('evidence_basis', ''),
            'political_context': evt.get('political_context', ''),
            'notes': evt.get('notes', ''),
            'kgl_sequence': evt.get('kgl_sequence', ''),
            'kgl': kgl_evt,
            'kgl_handle': handle_evt,
        }
        try:
            cypher = (
                "MERGE (evt:TransformEvent {event_id: $event_id}) "
                "ON CREATE SET evt.event_type=$event_type, "
                "evt.effective_fy=$effective_fy, evt.confidence=$confidence, "
                "evt.evidence_basis=$evidence_basis, "
                "evt.political_context=$political_context, evt.notes=$notes, "
                "evt.kgl=$kgl, evt.kgl_handle=$kgl_handle, "
                "evt.kgl_sequence=$kgl_sequence"
            )
            if params['event_date']:
                cypher += ", evt.event_date=date($event_date)"
            session.run(cypher, params)
            executed += 1
        except Exception as e:
            errors_count += 1
            print(f"  ERROR [{evt['event_id']}]: {e}")

print(f"  {len(data['transform_events'])} TransformEvent nodes processed")

# --- ResourceAllocation nodes ---
print("Ingesting ResourceAllocation nodes...")
with driver.session() as session:
    for ra in data['resource_allocations']:
        aid = ra['allocation_id']
        if not aid:
            continue
        params = {
            'allocation_id': aid,
            'program_id': ra.get('program_id', ''),
            'fiscal_year': ra.get('fiscal_year', ''),
            'stream': ra.get('stream', ''),
            'kgl_sequence': ra.get('kgl_sequence', ''),
            'cost_centre': ra.get('cost_centre', ''),
            'source_file': ra.get('source_file', ''),
            'kgl': kgl_res,
            'kgl_handle': handle_res,
        }
        for field in ['base_funding', 'mandate_commitment', 'other_operational',
                      'total_funding', 'funded_beds_units', 'unfunded_beds_units', 'recipients']:
            val = ra.get(field, '').strip()
            params[field] = int(val) if val else 0

        try:
            session.run(
                "MERGE (ra:ResourceAllocation {allocation_id: $allocation_id}) "
                "ON CREATE SET ra.program_id=$program_id, ra.fiscal_year=$fiscal_year, "
                "ra.stream=$stream, ra.base_funding=$base_funding, "
                "ra.mandate_commitment=$mandate_commitment, "
                "ra.other_operational=$other_operational, "
                "ra.total_funding=$total_funding, ra.funded_beds_units=$funded_beds_units, "
                "ra.unfunded_beds_units=$unfunded_beds_units, ra.recipients=$recipients, "
                "ra.cost_centre=$cost_centre, ra.source_file=$source_file, "
                "ra.kgl=$kgl, ra.kgl_handle=$kgl_handle, ra.kgl_sequence=$kgl_sequence",
                params)
            executed += 1
        except Exception as e:
            errors_count += 1
            print(f"  ERROR [ResourceAllocation {aid}]: {e}")

print(f"  {len(data['resource_allocations'])} ResourceAllocation nodes processed")
print(f"\nNodes total: {executed} operations, {errors_count} errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ingest Relationships

# COMMAND ----------

rel_executed = 0
rel_errors = 0

# --- SOURCE_OF ---
print("Ingesting SOURCE_OF relationships...")
with driver.session() as session:
    for edge in data['edges_source_of']:
        try:
            session.run(
                "MATCH (src:OrgEntity {canonical_id: $src_id}) "
                "MATCH (evt:TransformEvent {event_id: $evt_id}) "
                "MERGE (src)-[:SOURCE_OF]->(evt)",
                {'src_id': edge['source_entity_id'], 'evt_id': edge['event_id']})
            rel_executed += 1
        except Exception as e:
            rel_errors += 1
            print(f"  ERROR [SOURCE_OF {edge['source_entity_id']}->{edge['event_id']}]: {e}")
print(f"  {len(data['edges_source_of'])} SOURCE_OF processed")

# --- TARGET_OF ---
print("Ingesting TARGET_OF relationships...")
with driver.session() as session:
    for edge in data['edges_target_of']:
        try:
            session.run(
                "MATCH (evt:TransformEvent {event_id: $evt_id}) "
                "MATCH (tgt:OrgEntity {canonical_id: $tgt_id}) "
                "MERGE (evt)-[:TARGET_OF]->(tgt)",
                {'evt_id': edge['event_id'], 'tgt_id': edge['target_entity_id']})
            rel_executed += 1
        except Exception as e:
            rel_errors += 1
            print(f"  ERROR [TARGET_OF {edge['event_id']}->{edge['target_entity_id']}]: {e}")
print(f"  {len(data['edges_target_of'])} TARGET_OF processed")

# --- EVIDENCED_BY ---
print("Ingesting EVIDENCED_BY relationships...")
with driver.session() as session:
    for edge in data['edges_evidenced_by']:
        try:
            session.run(
                "MATCH (evt:TransformEvent {event_id: $evt_id}) "
                "MATCH (doc:SourceDocument {doc_id: $doc_id}) "
                "MERGE (evt)-[:EVIDENCED_BY]->(doc)",
                {'evt_id': edge['event_id'], 'doc_id': edge['doc_id']})
            rel_executed += 1
        except Exception as e:
            rel_errors += 1
            print(f"  ERROR [EVIDENCED_BY {edge['event_id']}->{edge['doc_id']}]: {e}")
print(f"  {len(data['edges_evidenced_by'])} EVIDENCED_BY processed")

# --- PARENT_OF ---
print("Ingesting PARENT_OF relationships...")
with driver.session() as session:
    for edge in data['edges_parent_of']:
        params = {'parent_id': edge['parent_id'], 'child_id': edge['child_id']}
        start = edge.get('start_date', '').strip()
        end = edge.get('end_date', '').strip()

        cypher = (
            "MATCH (parent:OrgEntity {canonical_id: $parent_id}) "
            "MATCH (child:OrgEntity {canonical_id: $child_id}) "
            "MERGE (parent)-[r:PARENT_OF]->(child)"
        )
        if start and end:
            params['start'] = start
            params['end'] = end
            cypher += " ON CREATE SET r.start_date=date($start), r.end_date=date($end)"
        elif start:
            params['start'] = start
            cypher += " ON CREATE SET r.start_date=date($start)"

        try:
            session.run(cypher, params)
            rel_executed += 1
        except Exception as e:
            rel_errors += 1
            print(f"  ERROR [PARENT_OF {edge['parent_id']}->{edge['child_id']}]: {e}")
print(f"  {len(data['edges_parent_of'])} PARENT_OF processed")

# --- FUNDED_BY ---
print("Ingesting FUNDED_BY relationships...")
funded_count = 0
with driver.session() as session:
    for ra in data['resource_allocations']:
        pid = ra.get('program_id', '')
        aid = ra.get('allocation_id', '')
        if pid and aid:
            try:
                session.run(
                    "MATCH (prog:OrgEntity {canonical_id: $program_id}) "
                    "MATCH (ra:ResourceAllocation {allocation_id: $allocation_id}) "
                    "MERGE (prog)-[:FUNDED_BY]->(ra)",
                    {'program_id': pid, 'allocation_id': aid})
                rel_executed += 1
                funded_count += 1
            except Exception as e:
                rel_errors += 1
                print(f"  ERROR [FUNDED_BY {pid}->{aid}]: {e}")
print(f"  {funded_count} FUNDED_BY processed")

print(f"\nRelationships total: {rel_executed} operations, {rel_errors} errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verification & Statistics

# COMMAND ----------

with driver.session() as session:
    # Node counts
    result = session.run("""
        MATCH (n)
        RETURN labels(n)[0] AS label, count(n) AS cnt
        ORDER BY label
    """)
    node_counts = {r['label']: r['cnt'] for r in result}

    # Relationship counts
    result = session.run("""
        MATCH ()-[r]->()
        RETURN type(r) AS rel_type, count(r) AS cnt
        ORDER BY rel_type
    """)
    rel_counts = {r['rel_type']: r['cnt'] for r in result}

    # KGL compliance: check all nodes have kgl + kgl_handle
    result = session.run("""
        MATCH (n)
        WHERE n.kgl IS NULL OR n.kgl_handle IS NULL
        RETURN labels(n)[0] AS label, count(n) AS missing
    """)
    kgl_missing = {r['label']: r['missing'] for r in result}

print("=" * 50)
print("GRAPH VERIFICATION")
print("=" * 50)

print("\nNodes:")
total_nodes = 0
for label in ['OrgEntity', 'TransformEvent', 'SourceDocument', 'ResourceAllocation']:
    cnt = node_counts.get(label, 0)
    total_nodes += cnt
    print(f"  {label:25s} {cnt:>5}")
print(f"  {'TOTAL':25s} {total_nodes:>5}")

print("\nRelationships:")
total_rels = 0
for rtype in ['SOURCE_OF', 'TARGET_OF', 'EVIDENCED_BY', 'PARENT_OF', 'FUNDED_BY']:
    cnt = rel_counts.get(rtype, 0)
    total_rels += cnt
    print(f"  {rtype:25s} {cnt:>5}")
print(f"  {'TOTAL':25s} {total_rels:>5}")

print("\nNG1/NG2 KGL Compliance:")
if kgl_missing:
    for label, missing in kgl_missing.items():
        print(f"  WARNING: {label} has {missing} nodes missing kgl/kgl_handle")
else:
    print("  PASS — all nodes have kgl + kgl_handle properties")

# Expected counts
expected_nodes = len(data['sources']) + len(data['org_entities']) + \
                 len(data['transform_events']) + len(data['resource_allocations'])
expected_rels = len(data['edges_source_of']) + len(data['edges_target_of']) + \
                len(data['edges_evidenced_by']) + len(data['edges_parent_of']) + \
                len([r for r in data['resource_allocations'] if r.get('program_id')])

print(f"\nExpected: {expected_nodes} nodes, {expected_rels} relationships")
if total_nodes >= expected_nodes and total_rels >= expected_rels:
    print("INGESTION COMPLETE — all counts match or exceed expected")
else:
    print(f"WARNING: Counts below expected (nodes: {total_nodes}/{expected_nodes}, rels: {total_rels}/{expected_rels})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Quick Smoke Test Queries

# COMMAND ----------

with driver.session() as session:
    # Q1: PFVA hierarchy chain
    print("Q1: PFVA Hierarchy (current)")
    result = session.run("""
        MATCH path = (m:OrgEntity {level: 'ministry'})-[:PARENT_OF*]->(leaf:OrgEntity)
        WHERE m.name CONTAINS 'CHILDREN AND FAMILY'
        RETURN [n IN nodes(path) | n.name + ' (' + n.level + ')'] AS chain
        LIMIT 10
    """)
    for r in result:
        print(f"  {' → '.join(r['chain'])}")

    # Q2: Transformation event timeline
    print("\nQ2: Transformation Events (first 10 by date)")
    result = session.run("""
        MATCH (src:OrgEntity)-[:SOURCE_OF]->(evt:TransformEvent)-[:TARGET_OF]->(tgt:OrgEntity)
        RETURN evt.event_id AS id, evt.event_type AS type,
               evt.event_date AS date, src.name AS source, tgt.name AS target
        ORDER BY evt.event_date
        LIMIT 10
    """)
    for r in result:
        print(f"  {r['id']} {r['type']:10s} {r['date']}  {r['source']} → {r['target']}")

# COMMAND ----------

# Close driver
driver.close()
print("Neo4j connection closed.")