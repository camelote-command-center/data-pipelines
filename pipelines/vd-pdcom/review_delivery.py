"""Scoped FDW upsert and row-hash verification of private review geometry.

Never writes released PDCom tables or advances commune delivery_status. Uses the
existing registered Lamap server; keeps all validation flags and source evidence.
"""
import json
import os
from pathlib import Path
import uuid
import psycopg2
from psycopg2 import sql

TABLES=(('v_vd_pdcom_review_sectors','vd_pdcom_review_sectors',('id',)),
        ('v_vd_pdcom_review_parcels','vd_pdcom_review_parcels',('sector_id','egrid')))


def validate_attribution(cursor):
    # No empty geographic attribution may reach a receiver. Document membership
    # remains in bronze_ch.vd_pdcom_document_communes as separate source scope.
    cursor.execute("SELECT count(*) FROM gold_ch.v_vd_pdcom_review_sectors WHERE COALESCE(cardinality(commune_bfs),0)=0")
    if cursor.fetchone()[0]:raise ValueError('review_sector_commune_unresolved')
    cursor.execute("""SELECT count(*) FROM gold_ch.v_vd_pdcom_review_sectors s
      WHERE ST_Dimension(s.geom)<>2 AND (
        (CASE WHEN GeometryType(s.geom) IN ('LINESTRING','MULTILINESTRING')
          THEN s.validation_evidence->>'feature_kind'='source_native_line'
          WHEN GeometryType(s.geom) IN ('POINT','MULTIPOINT')
          THEN s.validation_evidence->>'feature_kind'='source_native_point'
          ELSE false END) IS NOT TRUE
        OR s.validation_evidence->'native_attribution' IS NULL
        OR EXISTS(SELECT 1 FROM bronze_ch.vd_pdcom_parcel_candidates p WHERE p.sector_id=s.id))""")
    if cursor.fetchone()[0]:raise ValueError('native_review_kind_or_parcel_association_invalid')


def deliver(conn):
    result={'publication_status':'internal_review_only','receiver':'lamap_db','tables':{}}
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='90s'")
        c.execute('SELECT pg_advisory_xact_lock(572500301)')
        validate_attribution(c)
        for source,target,keys in TABLES:
            c.execute('''SELECT column_name FROM information_schema.columns WHERE table_schema='gold_ch' AND table_name=%s ORDER BY ordinal_position''',(source,))
            columns=[r[0] for r in c.fetchall()]
            if not columns:raise ValueError('source_view_missing')
            src=sql.Identifier('gold_ch',source);dst=sql.Identifier('lamap_db_foreign',target)
            c.execute(sql.SQL('SELECT count(*) FROM {}').format(src));before=c.fetchone()[0]
            if not before:raise ValueError('empty_review_source_requires_attention')
            condition=sql.SQL(' AND ').join(sql.SQL('t.{}=s.{}').format(sql.Identifier(k),sql.Identifier(k)) for k in keys)
            cols=sql.SQL(',').join(map(sql.Identifier,columns))
            updates=sql.SQL(',').join(sql.SQL('{}=s.{}').format(sql.Identifier(col),sql.Identifier(col)) for col in columns if col not in keys)
            def comparable(alias):
                return sql.SQL(',').join(
                    sql.SQL('ST_AsEWKB({}.{})').format(sql.Identifier(alias),sql.Identifier(col)) if col=='geom'
                    else sql.SQL('{}.{}').format(sql.Identifier(alias),sql.Identifier(col)) for col in columns)
            # Explicit row fields avoid postgres_fdw's UPDATE whole-row record type
            # mismatch; EWKB compares coordinates rather than a geometry envelope.
            c.execute(sql.SQL('UPDATE {} t SET {} FROM {} s WHERE {} AND ROW({}) IS DISTINCT FROM ROW({})').format(
                dst,updates,src,condition,comparable('t'),comparable('s')))
            c.execute(sql.SQL('INSERT INTO {} ({}) SELECT {} FROM {} s WHERE NOT EXISTS(SELECT 1 FROM {} t WHERE {})').format(
                dst,cols,sql.SQL(',').join(sql.SQL('s.{}').format(sql.Identifier(col)) for col in columns),src,dst,condition))
            # Full matching-key comparison, not rows_affected. Extra rows fail verification.
            c.execute(sql.SQL('SELECT count(*) FROM {} s FULL JOIN {} t ON {} WHERE s.{} IS NULL OR t.{} IS NULL OR to_jsonb(s) IS DISTINCT FROM to_jsonb(t)').format(src,dst,condition,sql.Identifier(keys[0]),sql.Identifier(keys[0])))
            mismatch=c.fetchone()[0]
            if mismatch:raise ValueError('receiver_row_mismatch_'+target)
            c.execute(sql.SQL("SELECT count(*),bool_and(ST_IsValid(geom) AND ST_SRID(geom)=4326 AND publication_status='internal_review_only') FROM {}").format(dst));count,valid=c.fetchone()
            if count!=before or not valid:raise ValueError('receiver_geometry_or_count_mismatch')
            result['tables'][target]={'rows':count,'mismatches':0,'valid_4326':True}
    return result

if __name__=='__main__':
    from parser import Monitor
    monitor=Monitor(code='vd_pdcom_review_delivery')
    operation=uuid.uuid4();monitor.begin(operation)
    conn=None
    try:
        conn=psycopg2.connect(os.environ['RE_LLM_DB_URL'],connect_timeout=15)
        result=deliver(conn)
        from site_scenario_delivery import deliver as deliver_site_scenarios
        result['site_scenarios']=deliver_site_scenarios(conn)
        Path('vd-pdcom-output').mkdir(exist_ok=True)
        Path('vd-pdcom-output/review-delivery.json').write_text(json.dumps(result,indent=2))
        monitor.finish({'record_count':result['tables']['vd_pdcom_review_sectors']['rows'],'review_delivery':result},True)
    except Exception as exc:
        monitor.finish({'error_type':type(exc).__name__},False)
        raise
    finally:
        if conn:conn.close()
