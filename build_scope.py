"""
build_scope.py
Medicare fraud detection graph — Steps 0 through 4.

Steps:
  0. Environment verification
  1. Select ~20,000 providers into scope_providers
  2. Build scoped_edges
  3. Validation
  4. Export CSVs
"""

import sys
import duckdb

DB_PATH = r"C:\Users\aaroh\medicare-fraud\data\processed\medicare.db"
EXPORT_DIR = r"C:\Users\aaroh\medicare-fraud\data\processed"

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def run(con, sql, label=None):
    result = con.execute(sql).fetchall()
    if label:
        print(f"  {label}: {result}")
    return result


def scalar(con, sql):
    return con.execute(sql).fetchone()[0]


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# STEP 0 — environment verification
# ---------------------------------------------------------------------------

def step0(con):
    section("STEP 0 — Environment Verification")

    # 1. tables
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    print(f"Tables: {tables}")
    for required in ("nppes", "nppes_clean_v2", "oig_core_fraud"):
        assert required in tables, f"MISSING TABLE: {required}"

    # 2. row counts
    counts = {
        "nppes":          scalar(con, "SELECT COUNT(*) FROM nppes"),
        "nppes_clean_v2": scalar(con, "SELECT COUNT(*) FROM nppes_clean_v2"),
        "oig_core_fraud": scalar(con, "SELECT COUNT(*) FROM oig_core_fraud"),
    }
    for tbl, cnt in counts.items():
        print(f"  {tbl}: {cnt:,}")
    assert counts["nppes"]          ==  9_551_447, f"nppes count mismatch: {counts['nppes']}"
    assert counts["nppes_clean_v2"] ==  9_208_126, f"nppes_clean_v2 count mismatch: {counts['nppes_clean_v2']}"
    assert counts["oig_core_fraud"] ==       4_264, f"oig_core_fraud count mismatch: {counts['oig_core_fraud']}"

    # 3. schema of nppes_clean_v2
    schema = con.execute("DESCRIBE nppes_clean_v2").fetchall()
    print("\n  nppes_clean_v2 schema:")
    for col in schema:
        print(f"    {col}")

    # 4. NPI types
    t_nppes   = con.execute("SELECT typeof(npi) FROM nppes LIMIT 1").fetchone()[0]
    t_clean   = con.execute("SELECT typeof(npi) FROM nppes_clean_v2 LIMIT 1").fetchone()[0]
    t_oig     = con.execute("SELECT typeof(npi) FROM oig_core_fraud LIMIT 1").fetchone()[0]
    print(f"\n  NPI typeof — nppes: {t_nppes} | nppes_clean_v2: {t_clean} | oig_core_fraud: {t_oig}")
    if not (t_nppes == t_clean == t_oig):
        print("  NOTE: NPI type mismatch detected — will CAST to VARCHAR on every join")

    # 5. fraud seed coverage
    coverage = scalar(con, """
        SELECT COUNT(*) FROM oig_core_fraud f
        WHERE CAST(f.npi AS VARCHAR)
              IN (SELECT CAST(npi AS VARCHAR) FROM nppes_clean_v2)
    """)
    gap = counts["oig_core_fraud"] - coverage
    print(f"\n  Fraud seed coverage: {coverage:,} of {counts['oig_core_fraud']:,} in nppes_clean_v2 (gap={gap})")
    if coverage < 3_800:
        print("  WARNING: coverage below expected ~3,898 — check oig_core_fraud loading")

    # 6. Drive-In Pharmacy spot check
    known = ('1134119043', '1912985821', '1700962925', '1306989769')
    found = [r[0] for r in con.execute(f"""
        SELECT CAST(npi AS VARCHAR) FROM nppes_clean_v2
        WHERE CAST(npi AS VARCHAR) IN ({','.join("'" + n + "'" for n in known)})
    """).fetchall()]
    print(f"\n  Drive-In Pharmacy NPIs present in nppes_clean_v2: {found}")
    missing = set(known) - set(found)
    if missing:
        print(f"  NOTE: Missing from nppes_clean_v2: {missing}")

    print("\nStep 0 complete — all checks passed.")


# ---------------------------------------------------------------------------
# STEP 1 — select ~20,000 providers into scope_providers
# ---------------------------------------------------------------------------

def step1(con):
    section("STEP 1 — Select ~20,000 Providers into scope_providers")

    con.execute("DROP TABLE IF EXISTS scope_providers")
    con.execute("""
        CREATE TABLE scope_providers (
            npi         VARCHAR,
            group_label VARCHAR
        )
    """)

    # ── GROUP 1: Fraud seeds ────────────────────────────────────────────────
    con.execute("""
        INSERT INTO scope_providers (npi, group_label)
        SELECT DISTINCT CAST(f.npi AS VARCHAR), 'fraud_seed'
        FROM oig_core_fraud f
        WHERE CAST(f.npi AS VARCHAR)
              IN (SELECT CAST(npi AS VARCHAR) FROM nppes_clean_v2)
    """)
    n_seeds = scalar(con, "SELECT COUNT(*) FROM scope_providers")
    print(f"\nFraud seeds inserted: {n_seeds:,}")

    # ── GROUP 2: Suspicious neighbors (top 10,000 by weighted score) ────────
    # Compute weighted_score per candidate from three attribute joins
    con.execute("""
        CREATE OR REPLACE TEMP TABLE neighbor_scores AS
        WITH fraud_npis AS (
            SELECT CAST(npi AS VARCHAR) AS npi FROM oig_core_fraud
        ),
        -- official connections  weight=3
        official_hits AS (
            SELECT
                CAST(c.npi AS VARCHAR)  AS candidate_npi,
                COUNT(DISTINCT CAST(s.npi AS VARCHAR)) AS cnt
            FROM nppes_clean_v2 c
            JOIN nppes_clean_v2 s
              ON  c.auth_official_last  IS NOT NULL AND c.auth_official_last  != ''
              AND c.auth_official_first IS NOT NULL AND c.auth_official_first != ''
              AND s.auth_official_last  IS NOT NULL AND s.auth_official_last  != ''
              AND s.auth_official_first IS NOT NULL AND s.auth_official_first != ''
              AND c.auth_official_last  = s.auth_official_last
              AND c.auth_official_first = s.auth_official_first
            WHERE CAST(s.npi AS VARCHAR) IN (SELECT npi FROM fraud_npis)
              AND CAST(c.npi AS VARCHAR) NOT IN (SELECT npi FROM fraud_npis)
            GROUP BY CAST(c.npi AS VARCHAR)
        ),
        -- phone connections  weight=2
        phone_hits AS (
            SELECT
                CAST(c.npi AS VARCHAR) AS candidate_npi,
                COUNT(DISTINCT CAST(s.npi AS VARCHAR)) AS cnt
            FROM nppes_clean_v2 c
            JOIN nppes_clean_v2 s
              ON  c.phone IS NOT NULL AND c.phone != ''
              AND s.phone IS NOT NULL AND s.phone != ''
              AND c.phone = s.phone
            WHERE CAST(s.npi AS VARCHAR) IN (SELECT npi FROM fraud_npis)
              AND CAST(c.npi AS VARCHAR) NOT IN (SELECT npi FROM fraud_npis)
            GROUP BY CAST(c.npi AS VARCHAR)
        ),
        -- address connections  weight=1  (refined rule)
        addr_hits AS (
            SELECT
                CAST(c.npi AS VARCHAR) AS candidate_npi,
                COUNT(DISTINCT CAST(s.npi AS VARCHAR)) AS cnt
            FROM nppes_clean_v2 c
            JOIN nppes_clean_v2 s
              ON  c.address_clean IS NOT NULL AND c.address_clean != ''
              AND c.zip5          IS NOT NULL AND c.zip5          != ''
              AND s.address_clean IS NOT NULL AND s.address_clean != ''
              AND s.zip5          IS NOT NULL AND s.zip5          != ''
              AND c.address_clean = s.address_clean
              AND c.zip5          = s.zip5
              -- refined address rule
              AND (
                    c.entity_type = 1 OR s.entity_type = 1
                    OR (c.entity_type = 2 AND s.entity_type = 2
                        AND c.taxonomy_code = s.taxonomy_code)
                  )
            WHERE CAST(s.npi AS VARCHAR) IN (SELECT npi FROM fraud_npis)
              AND CAST(c.npi AS VARCHAR) NOT IN (SELECT npi FROM fraud_npis)
            GROUP BY CAST(c.npi AS VARCHAR)
        ),
        all_candidates AS (
            SELECT candidate_npi FROM official_hits
            UNION
            SELECT candidate_npi FROM phone_hits
            UNION
            SELECT candidate_npi FROM addr_hits
        )
        SELECT
            ac.candidate_npi                                           AS npi,
            COALESCE(oh.cnt, 0) * 3
              + COALESCE(ph.cnt, 0) * 2
              + COALESCE(ah.cnt, 0) * 1                               AS weighted_score,
            (CASE WHEN oh.cnt > 0 THEN 1 ELSE 0 END
             + CASE WHEN ph.cnt > 0 THEN 1 ELSE 0 END
             + CASE WHEN ah.cnt > 0 THEN 1 ELSE 0 END)                AS connection_types,
            COALESCE(oh.cnt, 0)
              + COALESCE(ph.cnt, 0)
              + COALESCE(ah.cnt, 0)                                   AS fraud_connection_count
        FROM all_candidates ac
        LEFT JOIN official_hits oh ON oh.candidate_npi = ac.candidate_npi
        LEFT JOIN phone_hits    ph ON ph.candidate_npi = ac.candidate_npi
        LEFT JOIN addr_hits     ah ON ah.candidate_npi = ac.candidate_npi
        ORDER BY weighted_score DESC
        LIMIT 10000
    """)

    con.execute("""
        INSERT INTO scope_providers (npi, group_label)
        SELECT npi, 'suspicious_neighbor'
        FROM neighbor_scores
    """)
    n_neighbors = scalar(con, """
        SELECT COUNT(*) FROM scope_providers WHERE group_label = 'suspicious_neighbor'
    """)
    score_stats = con.execute("""
        SELECT MIN(weighted_score), MAX(weighted_score), AVG(weighted_score),
               MIN(fraud_connection_count), MAX(fraud_connection_count)
        FROM neighbor_scores
    """).fetchone()
    print(f"Suspicious neighbors inserted: {n_neighbors:,}")
    print(f"  weighted_score range: min={score_stats[0]} max={score_stats[1]} avg={score_stats[2]:.2f}")
    print(f"  fraud_connection_count range: min={score_stats[3]} max={score_stats[4]}")

    # ── GROUP 3: Bridge providers ───────────────────────────────────────────
    # Pass A: adjacency among current scope members
    con.execute("DROP TABLE IF EXISTS temp_scope_edges")
    con.execute("""
        CREATE TEMP TABLE temp_scope_edges AS
        -- phone edges
        SELECT CAST(a.npi AS VARCHAR) AS npi_a, CAST(b.npi AS VARCHAR) AS npi_b
        FROM nppes_clean_v2 a
        JOIN nppes_clean_v2 b
          ON  a.phone IS NOT NULL AND a.phone != ''
          AND b.phone IS NOT NULL AND b.phone != ''
          AND a.phone = b.phone
          AND CAST(a.npi AS VARCHAR) < CAST(b.npi AS VARCHAR)
        WHERE CAST(a.npi AS VARCHAR) IN (SELECT npi FROM scope_providers)
          AND CAST(b.npi AS VARCHAR) IN (SELECT npi FROM scope_providers)

        UNION

        -- address edges (refined rule)
        SELECT CAST(a.npi AS VARCHAR), CAST(b.npi AS VARCHAR)
        FROM nppes_clean_v2 a
        JOIN nppes_clean_v2 b
          ON  a.address_clean IS NOT NULL AND a.address_clean != ''
          AND a.zip5          IS NOT NULL AND a.zip5          != ''
          AND b.address_clean IS NOT NULL AND b.address_clean != ''
          AND b.zip5          IS NOT NULL AND b.zip5          != ''
          AND a.address_clean = b.address_clean
          AND a.zip5          = b.zip5
          AND CAST(a.npi AS VARCHAR) < CAST(b.npi AS VARCHAR)
          AND (
                a.entity_type = 1 OR b.entity_type = 1
                OR (a.entity_type = 2 AND b.entity_type = 2
                    AND a.taxonomy_code = b.taxonomy_code)
              )
        WHERE CAST(a.npi AS VARCHAR) IN (SELECT npi FROM scope_providers)
          AND CAST(b.npi AS VARCHAR) IN (SELECT npi FROM scope_providers)

        UNION

        -- official edges
        SELECT CAST(a.npi AS VARCHAR), CAST(b.npi AS VARCHAR)
        FROM nppes_clean_v2 a
        JOIN nppes_clean_v2 b
          ON  a.auth_official_last  IS NOT NULL AND a.auth_official_last  != ''
          AND a.auth_official_first IS NOT NULL AND a.auth_official_first != ''
          AND b.auth_official_last  IS NOT NULL AND b.auth_official_last  != ''
          AND b.auth_official_first IS NOT NULL AND b.auth_official_first != ''
          AND a.auth_official_last  = b.auth_official_last
          AND a.auth_official_first = b.auth_official_first
          AND CAST(a.npi AS VARCHAR) < CAST(b.npi AS VARCHAR)
        WHERE CAST(a.npi AS VARCHAR) IN (SELECT npi FROM scope_providers)
          AND CAST(b.npi AS VARCHAR) IN (SELECT npi FROM scope_providers)
    """)

    # Pass B: find bridges
    # For each outside candidate, gather scope members they connect to,
    # then check if any pair of those members is NOT in temp_scope_edges.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE bridge_candidates AS
        WITH outside_connections AS (
            -- phone
            SELECT CAST(c.npi AS VARCHAR) AS candidate_npi,
                   CAST(s.npi AS VARCHAR) AS scope_npi
            FROM nppes_clean_v2 c
            JOIN nppes_clean_v2 s
              ON  c.phone IS NOT NULL AND c.phone != ''
              AND s.phone IS NOT NULL AND s.phone != ''
              AND c.phone = s.phone
            WHERE CAST(c.npi AS VARCHAR) NOT IN (SELECT npi FROM scope_providers)
              AND CAST(s.npi AS VARCHAR)     IN (SELECT npi FROM scope_providers)

            UNION ALL

            -- address (refined rule)
            SELECT CAST(c.npi AS VARCHAR), CAST(s.npi AS VARCHAR)
            FROM nppes_clean_v2 c
            JOIN nppes_clean_v2 s
              ON  c.address_clean IS NOT NULL AND c.address_clean != ''
              AND c.zip5          IS NOT NULL AND c.zip5          != ''
              AND s.address_clean IS NOT NULL AND s.address_clean != ''
              AND s.zip5          IS NOT NULL AND s.zip5          != ''
              AND c.address_clean = s.address_clean
              AND c.zip5          = s.zip5
              AND (
                    c.entity_type = 1 OR s.entity_type = 1
                    OR (c.entity_type = 2 AND s.entity_type = 2
                        AND c.taxonomy_code = s.taxonomy_code)
                  )
            WHERE CAST(c.npi AS VARCHAR) NOT IN (SELECT npi FROM scope_providers)
              AND CAST(s.npi AS VARCHAR)     IN (SELECT npi FROM scope_providers)

            UNION ALL

            -- official
            SELECT CAST(c.npi AS VARCHAR), CAST(s.npi AS VARCHAR)
            FROM nppes_clean_v2 c
            JOIN nppes_clean_v2 s
              ON  c.auth_official_last  IS NOT NULL AND c.auth_official_last  != ''
              AND c.auth_official_first IS NOT NULL AND c.auth_official_first != ''
              AND s.auth_official_last  IS NOT NULL AND s.auth_official_last  != ''
              AND s.auth_official_first IS NOT NULL AND s.auth_official_first != ''
              AND c.auth_official_last  = s.auth_official_last
              AND c.auth_official_first = s.auth_official_first
            WHERE CAST(c.npi AS VARCHAR) NOT IN (SELECT npi FROM scope_providers)
              AND CAST(s.npi AS VARCHAR)     IN (SELECT npi FROM scope_providers)
        ),
        -- deduplicate scope connections per candidate
        deduped AS (
            SELECT DISTINCT candidate_npi, scope_npi FROM outside_connections
        ),
        -- pairs of scope members each candidate connects to
        candidate_pairs AS (
            SELECT
                a.candidate_npi,
                CASE WHEN a.scope_npi < b.scope_npi
                     THEN a.scope_npi ELSE b.scope_npi END AS scope_a,
                CASE WHEN a.scope_npi < b.scope_npi
                     THEN b.scope_npi ELSE a.scope_npi END AS scope_b
            FROM deduped a
            JOIN deduped b
              ON a.candidate_npi = b.candidate_npi
             AND a.scope_npi    != b.scope_npi
        ),
        -- keep pairs where the two scope members are NOT already connected
        bridging_pairs AS (
            SELECT cp.candidate_npi, COUNT(*) AS gap_pairs
            FROM candidate_pairs cp
            LEFT JOIN temp_scope_edges tse
              ON tse.npi_a = cp.scope_a AND tse.npi_b = cp.scope_b
            WHERE tse.npi_a IS NULL   -- not directly connected
            GROUP BY cp.candidate_npi
            HAVING gap_pairs > 0
        ),
        -- also count total scope members they touch
        touch_counts AS (
            SELECT candidate_npi, COUNT(DISTINCT scope_npi) AS scope_touches
            FROM deduped
            GROUP BY candidate_npi
        )
        SELECT bp.candidate_npi, bp.gap_pairs, tc.scope_touches
        FROM bridging_pairs bp
        JOIN touch_counts tc ON tc.candidate_npi = bp.candidate_npi
        ORDER BY tc.scope_touches DESC
    """)

    total_qualifying = scalar(con, "SELECT COUNT(*) FROM bridge_candidates")
    take = min(total_qualifying, 1_000)

    con.execute(f"""
        INSERT INTO scope_providers (npi, group_label)
        SELECT candidate_npi, 'bridge_provider'
        FROM bridge_candidates
        LIMIT {take}
    """)
    n_bridges = scalar(con, "SELECT COUNT(*) FROM scope_providers WHERE group_label = 'bridge_provider'")
    print(f"\nBridge providers qualifying: {total_qualifying:,} (took top {n_bridges})")
    if total_qualifying < 1_000:
        print(f"  NOTE: fewer than 1,000 bridges qualified — took all {total_qualifying}")

    # ── GROUP 4: Matched control (~5,000) ───────────────────────────────────
    # Build state/entity_type distribution from currently selected providers
    con.execute("""
        CREATE OR REPLACE TEMP TABLE scope_distribution AS
        SELECT n.state, n.entity_type,
               COUNT(*) AS cnt,
               ROUND(COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (), 4) AS frac
        FROM scope_providers sp
        JOIN nppes n ON CAST(n.npi AS VARCHAR) = sp.npi
        GROUP BY n.state, n.entity_type
    """)

    # Eligible control candidates: no phone/address/official link to any fraud seed
    con.execute("""
        CREATE OR REPLACE TEMP TABLE control_eligible AS
        SELECT CAST(c.npi AS VARCHAR) AS npi
        FROM nppes_clean_v2 c
        WHERE CAST(c.npi AS VARCHAR) NOT IN (SELECT npi FROM scope_providers)
          AND CAST(c.npi AS VARCHAR) NOT IN (
              SELECT CAST(npi AS VARCHAR) FROM oig_core_fraud
          )
          -- exclude providers with any phone link to fraud seed
          AND (c.phone IS NULL OR c.phone = ''
               OR c.phone NOT IN (
                   SELECT f.phone FROM nppes_clean_v2 f
                   WHERE CAST(f.npi AS VARCHAR) IN (
                       SELECT CAST(npi AS VARCHAR) FROM oig_core_fraud
                   )
                   AND f.phone IS NOT NULL AND f.phone != ''
               ))
          -- exclude providers with any address link (refined rule) to fraud seed
          AND NOT EXISTS (
              SELECT 1 FROM nppes_clean_v2 s
              WHERE CAST(s.npi AS VARCHAR) IN (
                      SELECT CAST(npi AS VARCHAR) FROM oig_core_fraud
                  )
                AND s.address_clean IS NOT NULL AND s.address_clean != ''
                AND s.zip5          IS NOT NULL AND s.zip5          != ''
                AND c.address_clean IS NOT NULL AND c.address_clean != ''
                AND c.zip5          IS NOT NULL AND c.zip5          != ''
                AND c.address_clean = s.address_clean
                AND c.zip5          = s.zip5
                AND (
                      c.entity_type = 1 OR s.entity_type = 1
                      OR (c.entity_type = 2 AND s.entity_type = 2
                          AND c.taxonomy_code = s.taxonomy_code)
                    )
          )
          -- exclude providers with any official link to fraud seed
          AND (
               c.auth_official_last  IS NULL OR c.auth_official_last  = ''
               OR c.auth_official_first IS NULL OR c.auth_official_first = ''
               OR NOT EXISTS (
                   SELECT 1 FROM nppes_clean_v2 s
                   WHERE CAST(s.npi AS VARCHAR) IN (
                           SELECT CAST(npi AS VARCHAR) FROM oig_core_fraud
                       )
                     AND s.auth_official_last  = c.auth_official_last
                     AND s.auth_official_first = c.auth_official_first
                     AND s.auth_official_last  IS NOT NULL AND s.auth_official_last  != ''
                     AND s.auth_official_first IS NOT NULL AND s.auth_official_first != ''
               )
          )
    """)

    # Stratified sample matching state/entity_type distribution, up to 5,000
    con.execute("""
        INSERT INTO scope_providers (npi, group_label)
        WITH ranked AS (
            SELECT
                ce.npi,
                n.state,
                n.entity_type,
                ROW_NUMBER() OVER (
                    PARTITION BY n.state, n.entity_type
                    ORDER BY CAST(ce.npi AS INTEGER) -- deterministic ordering
                ) AS rn,
                ROUND(d.frac * 5000) AS target
            FROM control_eligible ce
            JOIN nppes n     ON CAST(n.npi AS VARCHAR) = ce.npi
            JOIN scope_distribution d
              ON d.state = n.state AND d.entity_type = n.entity_type
        )
        SELECT npi, 'control'
        FROM ranked
        WHERE rn <= target
        LIMIT 5000
    """)

    n_controls = scalar(con, "SELECT COUNT(*) FROM scope_providers WHERE group_label = 'control'")
    print(f"\nControl providers inserted: {n_controls:,}")

    # State/entity_type distribution comparison
    print("\n  State/entity_type distribution (controls vs fraud-connected):")
    dist = con.execute("""
        SELECT
            n.state,
            n.entity_type,
            SUM(CASE WHEN sp.group_label = 'control' THEN 1 ELSE 0 END) AS control_n,
            SUM(CASE WHEN sp.group_label != 'control' THEN 1 ELSE 0 END) AS fraud_connected_n
        FROM scope_providers sp
        JOIN nppes n ON CAST(n.npi AS VARCHAR) = sp.npi
        GROUP BY n.state, n.entity_type
        ORDER BY fraud_connected_n DESC
        LIMIT 20
    """).fetchall()
    print(f"  {'state':6} {'etype':6} {'control':>10} {'fraud-connected':>16}")
    for row in dist:
        print(f"  {str(row[0]):6} {str(row[1]):6} {row[2]:>10,} {row[3]:>16,}")

    # Final summary
    totals = con.execute("""
        SELECT group_label, COUNT(*) AS n
        FROM scope_providers
        GROUP BY group_label
        ORDER BY group_label
    """).fetchall()
    total = scalar(con, "SELECT COUNT(*) FROM scope_providers")
    print(f"\nTotal in scope_providers: {total:,}")
    for row in totals:
        print(f"  {row[0]:25}: {row[1]:,}")


# ---------------------------------------------------------------------------
# STEP 2 — build scoped_edges
# ---------------------------------------------------------------------------

def step2(con):
    section("STEP 2 — Build scoped_edges")

    con.execute("DROP TABLE IF EXISTS scoped_edges")
    con.execute("""
        CREATE TABLE scoped_edges (
            npi_1            VARCHAR,
            npi_2            VARCHAR,
            has_phone        BOOLEAN,
            has_address      BOOLEAN,
            has_official     BOOLEAN,
            combined_weight  INTEGER,
            connection_count INTEGER
        )
    """)

    con.execute("""
        INSERT INTO scoped_edges
        WITH phone_pairs AS (
            SELECT
                CASE WHEN CAST(a.npi AS VARCHAR) < CAST(b.npi AS VARCHAR)
                     THEN CAST(a.npi AS VARCHAR) ELSE CAST(b.npi AS VARCHAR) END AS npi_1,
                CASE WHEN CAST(a.npi AS VARCHAR) < CAST(b.npi AS VARCHAR)
                     THEN CAST(b.npi AS VARCHAR) ELSE CAST(a.npi AS VARCHAR) END AS npi_2,
                'phone' AS edge_type,
                2       AS weight
            FROM nppes_clean_v2 a
            JOIN nppes_clean_v2 b
              ON  a.phone IS NOT NULL AND a.phone != ''
              AND b.phone IS NOT NULL AND b.phone != ''
              AND a.phone = b.phone
              AND CAST(a.npi AS VARCHAR) != CAST(b.npi AS VARCHAR)
            WHERE CAST(a.npi AS VARCHAR) IN (SELECT npi FROM scope_providers)
              AND CAST(b.npi AS VARCHAR) IN (SELECT npi FROM scope_providers)
        ),
        addr_pairs AS (
            SELECT
                CASE WHEN CAST(a.npi AS VARCHAR) < CAST(b.npi AS VARCHAR)
                     THEN CAST(a.npi AS VARCHAR) ELSE CAST(b.npi AS VARCHAR) END,
                CASE WHEN CAST(a.npi AS VARCHAR) < CAST(b.npi AS VARCHAR)
                     THEN CAST(b.npi AS VARCHAR) ELSE CAST(a.npi AS VARCHAR) END,
                'address' AS edge_type,
                1         AS weight
            FROM nppes_clean_v2 a
            JOIN nppes_clean_v2 b
              ON  a.address_clean IS NOT NULL AND a.address_clean != ''
              AND a.zip5          IS NOT NULL AND a.zip5          != ''
              AND b.address_clean IS NOT NULL AND b.address_clean != ''
              AND b.zip5          IS NOT NULL AND b.zip5          != ''
              AND a.address_clean = b.address_clean
              AND a.zip5          = b.zip5
              AND CAST(a.npi AS VARCHAR) != CAST(b.npi AS VARCHAR)
              AND (
                    a.entity_type = 1 OR b.entity_type = 1
                    OR (a.entity_type = 2 AND b.entity_type = 2
                        AND a.taxonomy_code = b.taxonomy_code)
                  )
            WHERE CAST(a.npi AS VARCHAR) IN (SELECT npi FROM scope_providers)
              AND CAST(b.npi AS VARCHAR) IN (SELECT npi FROM scope_providers)
        ),
        official_pairs AS (
            SELECT
                CASE WHEN CAST(a.npi AS VARCHAR) < CAST(b.npi AS VARCHAR)
                     THEN CAST(a.npi AS VARCHAR) ELSE CAST(b.npi AS VARCHAR) END,
                CASE WHEN CAST(a.npi AS VARCHAR) < CAST(b.npi AS VARCHAR)
                     THEN CAST(b.npi AS VARCHAR) ELSE CAST(a.npi AS VARCHAR) END,
                'official' AS edge_type,
                3          AS weight
            FROM nppes_clean_v2 a
            JOIN nppes_clean_v2 b
              ON  a.auth_official_last  IS NOT NULL AND a.auth_official_last  != ''
              AND a.auth_official_first IS NOT NULL AND a.auth_official_first != ''
              AND b.auth_official_last  IS NOT NULL AND b.auth_official_last  != ''
              AND b.auth_official_first IS NOT NULL AND b.auth_official_first != ''
              AND a.auth_official_last  = b.auth_official_last
              AND a.auth_official_first = b.auth_official_first
              AND CAST(a.npi AS VARCHAR) != CAST(b.npi AS VARCHAR)
            WHERE CAST(a.npi AS VARCHAR) IN (SELECT npi FROM scope_providers)
              AND CAST(b.npi AS VARCHAR) IN (SELECT npi FROM scope_providers)
        ),
        all_raw AS (
            SELECT * FROM phone_pairs
            UNION ALL
            SELECT * FROM addr_pairs
            UNION ALL
            SELECT * FROM official_pairs
        )
        SELECT
            npi_1,
            npi_2,
            MAX(edge_type = 'phone')    AS has_phone,
            MAX(edge_type = 'address')  AS has_address,
            MAX(edge_type = 'official') AS has_official,
            SUM(weight)                 AS combined_weight,
            COUNT(DISTINCT edge_type)   AS connection_count
        FROM all_raw
        GROUP BY npi_1, npi_2
    """)

    # Edge count breakdown
    stats = con.execute("""
        SELECT
            COUNT(*)                                                              AS total,
            SUM(CASE WHEN has_phone AND NOT has_address AND NOT has_official
                     THEN 1 ELSE 0 END)                                           AS phone_only,
            SUM(CASE WHEN has_address AND NOT has_phone AND NOT has_official
                     THEN 1 ELSE 0 END)                                           AS address_only,
            SUM(CASE WHEN has_official AND NOT has_phone AND NOT has_address
                     THEN 1 ELSE 0 END)                                           AS official_only,
            SUM(CASE WHEN has_phone AND has_address AND NOT has_official
                     THEN 1 ELSE 0 END)                                           AS phone_address,
            SUM(CASE WHEN has_phone AND has_official AND NOT has_address
                     THEN 1 ELSE 0 END)                                           AS phone_official,
            SUM(CASE WHEN has_address AND has_official AND NOT has_phone
                     THEN 1 ELSE 0 END)                                           AS address_official,
            SUM(CASE WHEN has_phone AND has_address AND has_official
                     THEN 1 ELSE 0 END)                                           AS triple
        FROM scoped_edges
    """).fetchone()

    labels = ["total", "phone only", "address only", "official only",
              "phone + address", "phone + official", "address + official",
              "all three (TRIPLE-CONNECTED)"]
    for lbl, val in zip(labels, stats):
        marker = "  *** " if "TRIPLE" in lbl and val > 0 else "  "
        print(f"{marker}{lbl}: {val:,}")

    total_edges = stats[0]
    if total_edges > 500_000:
        print(f"\n  WARNING: {total_edges:,} edges exceeds 500,000 — likely a clearinghouse hub survived scope filter")

    # Hub check — top 10 by degree
    print("\n  Top 10 providers by degree:")
    hubs = con.execute("""
        SELECT npi, COUNT(*) AS degree
        FROM (
            SELECT npi_1 AS npi FROM scoped_edges
            UNION ALL
            SELECT npi_2 FROM scoped_edges
        )
        GROUP BY npi
        ORDER BY degree DESC
        LIMIT 10
    """).fetchall()
    for npi, deg in hubs:
        warn = "  <-- WARNING: possible clearinghouse hub" if deg > 500 else ""
        print(f"    NPI {npi}: degree {deg:,}{warn}")


# ---------------------------------------------------------------------------
# STEP 3 — validation
# ---------------------------------------------------------------------------

def step3(con):
    section("STEP 3 — Validation")

    total_providers = scalar(con, "SELECT COUNT(*) FROM scope_providers")
    fraud_seeds = scalar(con, "SELECT COUNT(*) FROM scope_providers WHERE group_label = 'fraud_seed'")
    pct_fraud = fraud_seeds / total_providers * 100 if total_providers else 0
    print(f"\n1. Fraud density in scope_providers: {pct_fraud:.1f}% ({fraud_seeds:,}/{total_providers:,})")

    edge_with_fraud = scalar(con, """
        SELECT COUNT(*) FROM scoped_edges e
        WHERE e.npi_1 IN (SELECT npi FROM scope_providers WHERE group_label = 'fraud_seed')
           OR e.npi_2 IN (SELECT npi FROM scope_providers WHERE group_label = 'fraud_seed')
    """)
    total_edges = scalar(con, "SELECT COUNT(*) FROM scoped_edges")
    pct_fraud_edge = edge_with_fraud / total_edges * 100 if total_edges else 0
    print(f"   Edges with at least one fraud_seed endpoint: {pct_fraud_edge:.1f}% ({edge_with_fraud:,}/{total_edges:,})")

    # Triple-connection priority list
    print("\n2. Triple-connection priority list (top 20 non-fraud suspects connected to fraud seeds):")
    triples = con.execute("""
        SELECT
            CASE WHEN e.npi_1 IN (SELECT npi FROM scope_providers WHERE group_label = 'fraud_seed')
                 THEN e.npi_2 ELSE e.npi_1 END AS suspect_npi,
            COALESCE(n.org_name, n.last_name || ' ' || n.first_name) AS provider_name,
            n.state,
            e.combined_weight,
            e.connection_count
        FROM scoped_edges e
        JOIN scope_providers sp1 ON sp1.npi = e.npi_1
        JOIN scope_providers sp2 ON sp2.npi = e.npi_2
        JOIN nppes n ON CAST(n.npi AS VARCHAR) = (
            CASE WHEN e.npi_1 IN (SELECT npi FROM scope_providers WHERE group_label = 'fraud_seed')
                 THEN e.npi_2 ELSE e.npi_1 END
        )
        WHERE e.connection_count = 3
          AND (
              e.npi_1 IN (SELECT npi FROM scope_providers WHERE group_label = 'fraud_seed')
           OR e.npi_2 IN (SELECT npi FROM scope_providers WHERE group_label = 'fraud_seed')
          )
          AND (
              e.npi_1 NOT IN (SELECT npi FROM scope_providers WHERE group_label = 'fraud_seed')
           OR e.npi_2 NOT IN (SELECT npi FROM scope_providers WHERE group_label = 'fraud_seed')
          )
        ORDER BY e.combined_weight DESC
        LIMIT 20
    """).fetchall()
    if triples:
        print(f"  {'NPI':15} {'Name':40} {'State':6} {'Weight':8} {'Connections':12}")
        for row in triples:
            print(f"  {str(row[0]):15} {str(row[1])[:40]:40} {str(row[2]):6} {row[3]:8} {row[4]:12}")
    else:
        print("  No triple-connected fraud-seed pairs found.")

    # Drive-In Pharmacy ring check
    print("\n3. Drive-In Pharmacy ring check:")
    ring_npis = ('1134119043', '1912985821', '1700962925', '1306989769')
    in_scope = con.execute(f"""
        SELECT npi, group_label FROM scope_providers
        WHERE npi IN ({','.join("'" + n + "'" for n in ring_npis)})
    """).fetchall()
    if in_scope:
        for npi, lbl in in_scope:
            print(f"    NPI {npi}: {lbl}")
    else:
        print("    NONE of the Drive-In Pharmacy NPIs are in scope_providers — FLAG: investigate scope selection")

    # mini adjacency among these 4
    print("  Mini adjacency (Drive-In Pharmacy ring):")
    ring_edges = con.execute(f"""
        SELECT npi_1, npi_2, has_phone, has_address, has_official, combined_weight, connection_count
        FROM scoped_edges
        WHERE npi_1 IN ({','.join("'" + n + "'" for n in ring_npis)})
          AND npi_2 IN ({','.join("'" + n + "'" for n in ring_npis)})
    """).fetchall()
    if ring_edges:
        for e in ring_edges:
            print(f"    {e[0]} <-> {e[1]} | phone={e[2]} addr={e[3]} official={e[4]} weight={e[5]} conn={e[6]}")
        all_triple = all(e[6] == 3 for e in ring_edges)
        if not all_triple:
            print("    FLAG: Some ring edges are NOT triple-connected — check address rule / scope selection")
    else:
        print("    No edges found among Drive-In Pharmacy NPIs — FLAG: address rule or scope selection issue")

    # Las Vegas behavioral health check
    print("\n4. Las Vegas behavioral health check:")
    lv_npis = ('1144787813','1407473416','1295368041','1205588233',
               '1588322796','1013497452','1295279131','1649949124')
    lv_found = con.execute(f"""
        SELECT npi, group_label FROM scope_providers
        WHERE npi IN ({','.join("'" + n + "'" for n in lv_npis)})
    """).fetchall()
    if lv_found:
        for npi, lbl in lv_found:
            print(f"    NPI {npi}: {lbl}")
    else:
        print("    None of the Las Vegas behavioral health NPIs found in scope_providers")
    missing_lv = set(lv_npis) - {r[0] for r in lv_found}
    if missing_lv:
        print(f"    Missing: {missing_lv}")

    # Edge count sanity
    print("\n5. Edge count sanity:")
    if total_edges > 500_000:
        print(f"  WARNING: {total_edges:,} edges > 500,000 — listing top 5 high-degree providers:")
        top5 = con.execute("""
            SELECT npi, COUNT(*) AS degree
            FROM (
                SELECT npi_1 AS npi FROM scoped_edges
                UNION ALL
                SELECT npi_2 FROM scoped_edges
            )
            GROUP BY npi ORDER BY degree DESC LIMIT 5
        """).fetchall()
        for npi, deg in top5:
            print(f"    NPI {npi}: degree {deg:,}")
    else:
        print(f"  OK: {total_edges:,} edges")


# ---------------------------------------------------------------------------
# STEP 4 — export CSVs
# ---------------------------------------------------------------------------

def step4(con):
    section("STEP 4 — Export CSVs")

    nodes_path = EXPORT_DIR + r"\nodes.csv"
    edges_path = EXPORT_DIR + r"\edges.csv"

    # nodes.csv
    con.execute(f"""
        COPY (
            SELECT
                sp.npi,
                COALESCE(n.org_name,
                         n.last_name || ' ' || n.first_name) AS provider_name,
                n.state,
                n.entity_type,
                nc.taxonomy_code,
                sp.group_label,
                CASE WHEN CAST(n.npi AS VARCHAR) IN (
                         SELECT CAST(npi AS VARCHAR) FROM oig_core_fraud
                     ) THEN 1 ELSE 0 END AS is_fraud
            FROM scope_providers sp
            JOIN nppes n         ON CAST(n.npi AS VARCHAR) = sp.npi
            JOIN nppes_clean_v2 nc ON CAST(nc.npi AS VARCHAR) = sp.npi
        ) TO '{nodes_path}' (FORMAT CSV, HEADER TRUE)
    """)
    n_nodes = scalar(con, "SELECT COUNT(*) FROM scope_providers")
    print(f"  nodes.csv  → {nodes_path}  ({n_nodes:,} rows)")

    # edges.csv
    con.execute(f"""
        COPY (
            SELECT npi_1, npi_2, has_phone, has_address, has_official,
                   combined_weight, connection_count
            FROM scoped_edges
        ) TO '{edges_path}' (FORMAT CSV, HEADER TRUE)
    """)
    n_edges = scalar(con, "SELECT COUNT(*) FROM scoped_edges")
    print(f"  edges.csv  → {edges_path}  ({n_edges:,} rows)")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    print(f"Connecting to DuckDB: {DB_PATH}")
    con = duckdb.connect(DB_PATH)

    try:
        step0(con)
        step1(con)
        step2(con)
        step3(con)
        step4(con)
        print("\nAll steps complete.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
