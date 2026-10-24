"""Rebuild officer links in re-LLM and push re-LLM-owned link types to LaMap and LBI (replace by type, no truncate)."""
import json, os
import psycopg2

conn = psycopg2.connect(os.environ['RE_LLM_DB_URL'], options='-c statement_timeout=3600000')
with conn, conn.cursor() as c:
    c.execute('SELECT silver_ch.refresh_officer_relationships()')
    print('shares_officer_with links:', c.fetchone()[0])
    c.execute("SELECT gold_ch.push_network_types(ARRAY['shares_officer_with'])")
    print('pushed:', json.dumps(c.fetchone()[0]))
