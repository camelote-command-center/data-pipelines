"""Read-only, sector-only preview of the existing live qualification projection.

This is a point-in-time private review artifact, not a release/receiver surface.
No cached preview can establish eligibility on a later read: regenerate it.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path

from active_qualifications import project


def unique(rows, name):
    result = {}
    for row in rows:
        key = str(row['id'])
        if key in result:
            raise ValueError('duplicate_' + name + '_id')
        result[key] = row
    return result


def preview(sectors, qualifications, *, today):
    """Join only projected active manifests; retain every active review separately.

    Geometry is exact database EWKB, not rounded/reconstructed GeoJSON. Inputs
    must come from one live snapshot; audit() enforces that and read-only access.
    Duplicate identities reject the snapshot rather than choosing a winner.
    """
    sectors = list(sectors)
    qualifications = list(qualifications)
    by_sector = unique(sectors, 'sector')
    by_manifest = unique(qualifications, 'manifest')
    active = project(sectors, qualifications, today=today)
    payload = []
    geometry_holds = []
    for state in active['sectors']:
        if not state['eligible_sector_only']:
            continue
        sector = by_sector[state['sector_id']]
        try:
            ewkb = bytes.fromhex(sector['geometry_ewkb_hex'])
            exact_geometry = bool(ewkb) and hashlib.sha256(ewkb).hexdigest() == sector['geometry_sha256']
        except (KeyError, TypeError, ValueError):
            exact_geometry = False
        if not exact_geometry:
            geometry_holds.append({'sector_id': state['sector_id'], 'blocking_gate': 'exact_geometry_payload_hash_mismatch'})
            continue
        reviews = []
        for manifest_id in state['active_manifest_ids']:
            row = by_manifest[manifest_id]
            m = row['manifest']
            # Whitelist reviewed fields; arbitrary manifest extras are not rights.
            reviews.append({
                'manifest_id': manifest_id, 'manifest_sha256': row['manifest_sha256'],
                'reviewer': m['reviewer'], 'reviewed_on': m['reviewed_on'], 'valid_until': m['valid_until'],
                'version': {k: copy.deepcopy(m['version'][k]) for k in (
                    'status', 'currentness_verified', 'reservations', 'evidence_uri', 'review_summary')},
                'geography': {k: copy.deepcopy(m['geography'][k]) for k in (
                    'method', 'source_precision_m', 'maximum_error_m', 'accepted_error_limit_m',
                    'precision_basis', 'tolerance_basis', 'independent_review', 'whole_outline_reviewed',
                    'evidence_uri', 'review_summary', 'controls_held_out_of_fit', 'control_identity_reviewed')
                    if k in m['geography']},
                'semantics': {k: m['semantics'][k] for k in (
                    'source_category', 'meaning', 'limits', 'evidence_uri', 'review_summary', 'grants_parcel_rights')},
            })
        payload.append({
            'sector_id': state['sector_id'], 'document_id': str(sector['document_id']),
            'source_sha256': sector['source_sha256'], 'geometry_sha256': sector['geometry_sha256'],
            'geometry': {'encoding': 'EWKB_hex', 'srid': 4326, 'value': ewkb.hex()},
            'active_qualifications': reviews, 'scope': 'sector_only',
            'publication_authorized': False, 'parcel_release': False,
            'commune_completion_authorized': False,
        })
    return {
        'schema_version': 1, 'artifact_kind': 'qualified_sector_preview',
        'as_of': today.isoformat(), 'read_only': True, 'scope': 'sector_only',
        'publication_authorized': False, 'parcel_release': False,
        'commune_completion_authorized': False, 'receiver_registered': False,
        'requires_live_reprojection_before_use': True,
        'limitation': 'Point-in-time contract eligibility only, not verification of reviewer assertions. No receiver, transfer, parcel rights, capacity or commune-completion contract. Expiry, source/geometry changes, supersession and currentness changes require live reprojection; this file cannot authorize later use.',
        'counts': {'input_sectors': len(sectors), 'input_manifests': len(qualifications),
                   'projected_eligible_sectors': active['counts']['eligible_sectors'],
                   'preview_sectors': len(payload)},
        'sectors': payload, 'active_projection': active, 'payload_holds': geometry_holds,
    }


def audit(conn):
    """Use a dedicated connection, one repeatable-read snapshot and server date.

    No manifest recording, private receiver access, status updates or commits.
    Caller owns closing/rolling back its dedicated connection, including errors.
    """
    from psycopg2.extras import RealDictCursor
    conn.set_session(readonly=True, isolation_level='REPEATABLE READ')
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SET LOCAL statement_timeout='60s'")
        cur.execute("SELECT current_setting('transaction_read_only') AS read_only, "
                    "(transaction_timestamp() AT TIME ZONE 'UTC')::date AS today, "
                    "transaction_timestamp() AS snapshot_at")
        snapshot = cur.fetchone()
        if snapshot['read_only'] != 'on':
            raise ValueError('readonly_snapshot_required')
        cur.execute('''SELECT s.id,s.document_id,s.review_status,s.validation_evidence,
            d.plan_status,d.sha256 AS source_sha256,
            ST_IsValid(s.geom) AND ST_SRID(s.geom)=4326
                AND NOT ST_IsEmpty(s.geom) AS valid_geometry,
            encode(sha256(ST_AsEWKB(s.geom)),'hex') AS geometry_sha256,
            encode(ST_AsEWKB(s.geom),'hex') AS geometry_ewkb_hex
            FROM bronze_ch.vd_pdcom_sectors s
            JOIN bronze_ch.vd_pdcom_documents d ON d.id=s.document_id
            ORDER BY s.id''')
        sectors = [dict(row) for row in cur.fetchall()]
        cur.execute('''SELECT id,sector_id,document_id,source_sha256,geometry_sha256,
            manifest_sha256,manifest FROM bronze_ch.vd_pdcom_sector_qualifications
            ORDER BY id''')
        result = preview(sectors, [dict(row) for row in cur.fetchall()], today=snapshot['today'])
        result['snapshot_at'] = snapshot['snapshot_at'].isoformat()
        return result


def main():
    import psycopg2
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    conn = psycopg2.connect(os.environ['RE_LLM_DB_URL'], connect_timeout=15)
    try:
        result = audit(conn)
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()
    # Refuse to overwrite a prior snapshot or receipt.
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps({'counts': result['counts'], 'read_only': True, 'publication_authorized': False}))


if __name__ == '__main__':
    main()
