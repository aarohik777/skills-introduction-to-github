"""
load_neo4j.py
Medicare fraud detection graph — Step 5.

Loads nodes.csv and edges.csv into a fresh Neo4j database called 'scoped20k'.
Connects to bolt://localhost:7687.
"""

import csv
import sys
import time

from neo4j import GraphDatabase

NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"
NEO4J_DATABASE = "scoped20k"

EXPORT_DIR  = r"C:\Users\aaroh\medicare-fraud\data\processed"
NODES_CSV   = EXPORT_DIR + r"\nodes.csv"
EDGES_CSV   = EXPORT_DIR + r"\edges.csv"

BATCH_SIZE = 500


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return list(reader)


def batches(rows, size):
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def run_query(session, cypher, **params):
    session.run(cypher, **params)


# ---------------------------------------------------------------------------
# database setup
# ---------------------------------------------------------------------------

def ensure_database(driver):
    """
    Attempt to create the 'scoped20k' database.
    If it already exists, clear all nodes and relationships.
    Falls back gracefully if the server is Community Edition (no multi-db).
    """
    try:
        with driver.session(database="system") as sys_session:
            existing = [
                r["name"]
                for r in sys_session.run("SHOW DATABASES").data()
            ]
            if NEO4J_DATABASE not in existing:
                print(f"Creating database '{NEO4J_DATABASE}'...")
                sys_session.run(f"CREATE DATABASE {NEO4J_DATABASE} IF NOT EXISTS")
                # wait for it to come online
                for _ in range(30):
                    time.sleep(1)
                    row = sys_session.run(
                        f"SHOW DATABASE {NEO4J_DATABASE}"
                    ).single()
                    if row and row.get("currentStatus") == "online":
                        print(f"Database '{NEO4J_DATABASE}' is online.")
                        break
                else:
                    print(f"WARNING: '{NEO4J_DATABASE}' may not be fully online yet — continuing anyway")
            else:
                print(f"Database '{NEO4J_DATABASE}' already exists — clearing all data...")
                with driver.session(database=NEO4J_DATABASE) as db_session:
                    db_session.run("MATCH (n) DETACH DELETE n")
                print("Cleared.")
    except Exception as exc:
        print(f"NOTE: Could not manage named database (Community Edition?): {exc}")
        print(f"Falling back to default database — clearing all existing data...")
        with driver.session() as db_session:
            db_session.run("MATCH (n) DETACH DELETE n")


def create_indexes(session):
    session.run(
        "CREATE INDEX provider_npi IF NOT EXISTS FOR (p:Provider) ON (p.npi)"
    )
    session.run(
        "CREATE INDEX provider_fraud IF NOT EXISTS FOR (p:Provider) ON (p.is_fraud)"
    )
    print("Indexes created (or already existed).")


# ---------------------------------------------------------------------------
# loading
# ---------------------------------------------------------------------------

def load_nodes(session, rows):
    total = len(rows)
    loaded = 0

    for batch in batches(rows, BATCH_SIZE):
        session.run(
            """
            UNWIND $batch AS row
            MERGE (p:Provider {npi: row.npi})
            SET p.provider_name  = row.provider_name,
                p.state          = row.state,
                p.entity_type    = toInteger(row.entity_type),
                p.taxonomy_code  = row.taxonomy_code,
                p.group_label    = row.group_label,
                p.is_fraud       = toInteger(row.is_fraud)
            """,
            batch=batch,
        )
        loaded += len(batch)
        print(f"  Nodes loaded: {loaded:,} / {total:,}")

    return loaded


def load_edges(session, rows):
    total = len(rows)
    loaded = 0

    for batch in batches(rows, BATCH_SIZE):
        session.run(
            """
            UNWIND $batch AS row
            MATCH (a:Provider {npi: row.npi_1})
            MATCH (b:Provider {npi: row.npi_2})
            MERGE (a)-[r:CONNECTED_TO]-(b)
            SET r.has_phone        = (row.has_phone    = 'true'),
                r.has_address      = (row.has_address  = 'true'),
                r.has_official     = (row.has_official = 'true'),
                r.combined_weight  = toInteger(row.combined_weight),
                r.connection_count = toInteger(row.connection_count)
            """,
            batch=batch,
        )
        loaded += len(batch)
        print(f"  Edges loaded: {loaded:,} / {total:,}")

    return loaded


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    print(f"Reading {NODES_CSV}...")
    node_rows = read_csv(NODES_CSV)
    print(f"  {len(node_rows):,} node rows read.")

    print(f"Reading {EDGES_CSV}...")
    edge_rows = read_csv(EDGES_CSV)
    print(f"  {len(edge_rows):,} edge rows read.")

    print(f"\nConnecting to Neo4j at {NEO4J_URI} ...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    print("Connected.")

    ensure_database(driver)

    # determine which session to use (named db if supported, else default)
    try:
        session_kwargs = {"database": NEO4J_DATABASE}
        with driver.session(**session_kwargs) as _:
            pass
    except Exception:
        print(f"Could not open '{NEO4J_DATABASE}' — using default database.")
        session_kwargs = {}

    with driver.session(**session_kwargs) as session:
        create_indexes(session)

        print(f"\nLoading {len(node_rows):,} nodes in batches of {BATCH_SIZE}...")
        n_nodes = load_nodes(session, node_rows)

        print(f"\nLoading {len(edge_rows):,} edges in batches of {BATCH_SIZE}...")
        n_edges = load_edges(session, edge_rows)

    driver.close()
    print(f"\nNodes loaded: {n_nodes:,} | Edges loaded: {n_edges:,}")


if __name__ == "__main__":
    main()
