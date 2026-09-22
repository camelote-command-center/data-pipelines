"""Deliver reviewed historical site observations through the registered private FDW.

These are source-table scenarios, not current capacity or geographic sectors.
No row is promoted to a released PDCom layer or commune completion status.
"""
from psycopg2 import sql

KEYS = ('document_id', 'site_id', 'scenario')
SOURCE = 'v_vd_pdcom_review_site_scenarios'
TARGET = 'vd_pdcom_review_site_scenarios'


def validate_rows(rows):
    """Fail closed on missing, duplicate, unreviewed or wrongly attributed rows."""
    if len(rows) != 102:
        raise ValueError('site_scenario_expected_102_rows')
    keys = set()
    for row in rows:
        key = tuple(row[k] for k in KEYS)
        if key in keys:
            raise ValueError('site_scenario_duplicate')
        keys.add(key)
        if row['scenario'] not in (1, 2) or not 2 <= row['page_number'] <= 5:
            raise ValueError('site_scenario_identity_invalid')
        if row['review_status'] != 'historical_scenario_not_current_capacity' or row['publication_status'] != 'internal_review_only':
            raise ValueError('site_scenario_review_required')
        if not row['source_evidence'].get('limits'):
            raise ValueError('site_scenario_limits_missing')
        record = row['source_evidence']['record']
        if record['commune_bfs'] != row['commune_bfs'] or record['site_id'] != row['site_id'] or record['scenario'] != row['scenario']:
            raise ValueError('site_scenario_source_identity_mismatch')
    if len({r['site_id'] for r in rows}) != 51:
        raise ValueError('site_scenario_site_inventory_invalid')


def deliver(conn):
    from psycopg2.extras import RealDictCursor
    src = sql.Identifier('gold_ch', SOURCE)
    dst = sql.Identifier('lamap_db_foreign', TARGET)
    with conn, conn.cursor(cursor_factory=RealDictCursor) as c:
        c.execute("SET LOCAL statement_timeout='90s'")
        c.execute('SELECT pg_advisory_xact_lock(572500301)')
        c.execute(sql.SQL('SELECT * FROM {}').format(src))
        rows = c.fetchall()
        validate_rows(rows)
        columns = list(rows[0])
        condition = sql.SQL(' AND ').join(sql.SQL('s.{}=t.{}').format(sql.Identifier(k), sql.Identifier(k)) for k in KEYS)
        def fields(alias):
            return sql.SQL(',').join(sql.SQL('{}.{}').format(sql.Identifier(alias), sql.Identifier(k)) for k in columns)
        updates = sql.SQL(',').join(sql.SQL('{}=s.{}').format(sql.Identifier(k), sql.Identifier(k)) for k in columns if k not in KEYS)
        c.execute(sql.SQL('UPDATE {} t SET {} FROM {} s WHERE {} AND ROW({}) IS DISTINCT FROM ROW({})').format(dst, updates, src, condition, fields('t'), fields('s')))
        c.execute(sql.SQL('INSERT INTO {} ({}) SELECT {} FROM {} s WHERE NOT EXISTS(SELECT 1 FROM {} t WHERE {})').format(dst, sql.SQL(',').join(map(sql.Identifier, columns)), fields('s'), src, dst, condition))
        c.execute(sql.SQL('SELECT count(*) n FROM {} s FULL JOIN {} t ON {} WHERE s.document_id IS NULL OR t.document_id IS NULL OR to_jsonb(s) IS DISTINCT FROM to_jsonb(t)').format(src, dst, condition))
        if c.fetchone()['n']:
            raise ValueError('site_scenario_receiver_mismatch')
    return {'rows': len(rows), 'sites': 51, 'mismatches': 0,
            'publication_status': 'internal_review_only',
            'interpretation': 'historical_scenarios_not_current_capacity',
            'new_validated_sectors': 0}
