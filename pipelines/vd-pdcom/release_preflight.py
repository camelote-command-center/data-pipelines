"""Read-only release audit. A clean private copy is never a validated release.

This reports necessary existing gates, not sufficient publication approval.
It intentionally cannot advance sector or commune state or authorize a release.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path


def sector_blockers(row):
    blockers = []
    if row.get('review_status') != 'validated':
        blockers.append('sector_validation_not_recorded')
    if not row.get('validated_by'):
        blockers.append('validator_missing')
    precision = row.get('source_precision_m')
    if precision is None or precision <= 0:
        blockers.append('source_precision_unresolved')
    if row.get('valid_geometry') is not True:
        blockers.append('geometry_invalid_or_not_4326')
    if row.get('plan_status') not in ('approved', 'approved_with_reservation'):
        blockers.append('exact_version_approval_unresolved')
    if not row.get('private_receiver_matches'):
        blockers.append('private_receiver_missing_or_different')
    if row.get('boundary_review_pairs', 0):
        blockers.append('parcel_boundary_review_pending')
    return blockers


def audit(conn):
    from psycopg2.extras import RealDictCursor
    # Caller must supply a dedicated connection. Read-only is enforced by PG,
    # including all foreign-server access; no data mutation statements exist here.
    conn.set_session(readonly=True)
    with conn.cursor(cursor_factory=RealDictCursor) as c:
        c.execute("SET LOCAL statement_timeout='60s'")
        c.execute('''SELECT s.id,s.document_id,s.page_number,s.label,
            s.review_status,s.validated_by,s.source_precision_m,s.alignment_rmse_m,
            d.title,d.plan_status,d.sha256,
            ST_IsValid(s.geom) AND ST_SRID(s.geom)=4326 AS valid_geometry,
            v.commune_bfs,
            COALESCE(to_jsonb(v)=to_jsonb(t),false) AS private_receiver_matches,
            (SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p
             WHERE p.sector_id=s.id AND p.boundary_review_required) AS boundary_review_pairs
            FROM bronze_ch.vd_pdcom_sectors s
            JOIN bronze_ch.vd_pdcom_documents d ON d.id=s.document_id
            LEFT JOIN gold_ch.v_vd_pdcom_review_sectors v ON v.id=s.id
            LEFT JOIN lamap_db_foreign.vd_pdcom_review_sectors t ON t.id=s.id
            ORDER BY d.title,s.page_number,s.id''')
        sectors = [dict(r) for r in c.fetchall()]
        for row in sectors:
            row['blocking_gates'] = sector_blockers(row)
        c.execute('''SELECT commune_bfs,commune_name,discovery_status,
            extraction_status,delivery_status,resolved,document_count
            FROM bronze_ch.vd_pdcom_coverage WHERE is_current ORDER BY commune_bfs''')
        communes = [dict(r) for r in c.fetchall()]
    return {
        'checked_at': datetime.now(timezone.utc).isoformat(),
        'read_only': True, 'release_authorized': False,
        'release_route': 'not_implemented_by_this_preflight',
        'limitation': 'Necessary gate audit only. Also require independent positional and semantic QA, exact current-version/reservations review, complete commune map/category inventory, registered release receiver and full-row readback. Private parity cannot satisfy released delivery.',
        'counts': {'sectors':len(sectors),'communes':len(communes),
                   'resolved_communes':sum(bool(c['resolved']) for c in communes),
                   'sectors_without_listed_blockers':sum(not s['blocking_gates'] for s in sectors)},
        'blocking_gate_counts':dict(Counter(b for s in sectors for b in s['blocking_gates'])),
        'sectors':sectors,'communes':communes}


if __name__ == '__main__':
    import psycopg2
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    with psycopg2.connect(os.environ['RE_LLM_DB_URL'], connect_timeout=15) as conn:
        report = audit(conn)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, default=str, indent=2)+'\n')
    print(json.dumps({'counts':report['counts'],'blocking_gate_counts':report['blocking_gate_counts'],'release_authorized':False}))
