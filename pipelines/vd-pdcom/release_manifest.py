"""Fail-closed, append-only sector qualification; never publishes or completes a commune."""
import hashlib
import json
import math
import re
from datetime import date
from uuid import UUID


def canonical(manifest):
    return json.dumps(manifest, sort_keys=True, separators=(',', ':'), allow_nan=False)


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def text(value):
    return isinstance(value, str) and bool(value.strip())


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0


def validate(manifest, sector, today=None):
    """Validate explicit reviewer assertions against pinned live source and geometry.

    This checks the contract, not the truth of a reviewer's supporting evidence.
    Evidence must be substantively reviewed before a manifest is submitted.
    """
    today = today or date.today()
    require(isinstance(manifest, dict), 'manifest_object_required')
    require(manifest.get('schema_version') == 1, 'unsupported_schema')
    for key in ('manifest_id', 'sector_id', 'document_id'):
        try: UUID(manifest[key])
        except (KeyError, TypeError, ValueError, AttributeError): raise ValueError('invalid_'+key)
    require(manifest['sector_id'] == str(sector['id']), 'sector_id_mismatch')
    require(manifest['document_id'] == str(sector['document_id']), 'document_id_mismatch')
    for key in ('source_sha256', 'geometry_sha256'):
        require(isinstance(manifest.get(key), str) and re.fullmatch('[0-9a-f]{64}', manifest[key]) is not None, 'invalid_'+key)
        require(manifest[key] == sector[key], key+'_mismatch')
    require(sector.get('valid_geometry') is True, 'invalid_geometry')
    require(sector.get('review_status') != 'superseded', 'superseded_sector')
    require(text(manifest.get('reviewer')), 'named_reviewer_required')
    try:
        reviewed = date.fromisoformat(manifest['reviewed_on'])
        expires = date.fromisoformat(manifest['valid_until'])
    except (KeyError, TypeError, ValueError): raise ValueError('review_dates_required')
    require(reviewed <= today <= expires and reviewed <= expires, 'review_not_current')
    for name in ('version', 'geography', 'semantics'):
        section = manifest.get(name)
        require(isinstance(section, dict), name+'_required')
        require(text(section.get('evidence_uri')) and text(section.get('review_summary')), name+'_evidence_required')
    version = manifest['version']
    require(version.get('status') in ('approved', 'approved_with_reservation'), 'unapproved_version')
    require(version['status'] == sector['plan_status'], 'approval_status_mismatch')
    require(version.get('currentness_verified') is True, 'currentness_unresolved')
    require(isinstance(version.get('reservations'), list), 'reservations_review_required')
    require(all(text(x) for x in version['reservations']), 'invalid_reservation')
    require(version['status'] != 'approved_with_reservation' or bool(version['reservations']), 'reserved_source_requires_reservations')
    geo = manifest['geography']
    require(geo.get('independent_review') is True, 'independent_geography_required')
    require(geo.get('method') in ('native_geospatial_coordinates', 'independent_controls'), 'geography_method_required')
    for key in ('source_precision_m', 'maximum_error_m', 'accepted_error_limit_m'):
        require(number(geo.get(key)), 'invalid_'+key)
    require(geo['maximum_error_m'] <= geo['accepted_error_limit_m'], 'positional_tolerance_exceeded')
    require(text(geo.get('precision_basis')) and text(geo.get('tolerance_basis')), 'precision_and_tolerance_basis_required')
    require(geo.get('whole_outline_reviewed') is True, 'outline_review_required')
    if geo['method'] == 'independent_controls':
        require(geo.get('controls_held_out_of_fit') is True and geo.get('control_identity_reviewed') is True, 'independent_control_identity_required')
    sem = manifest['semantics']
    require(text(sem.get('source_category')) and text(sem.get('meaning')) and text(sem.get('limits')), 'semantics_incomplete')
    require(sem.get('grants_parcel_rights') is False, 'parcel_rights_not_established_by_pdcom')
    require(manifest.get('scope') == 'sector_only', 'commune_completion_not_authorized')
    require(manifest.get('parcel_release') is False, 'parcel_release_requires_separate_review')
    require(manifest.get('publication_authorized') is False, 'publication_requires_registered_release_route')
    body = canonical(manifest)
    return hashlib.sha256(body.encode()).hexdigest()


def record(conn, manifest):
    """Persist one immutable qualification after live checks; exact replay is a no-op."""
    from psycopg2.extras import RealDictCursor, Json
    with conn, conn.cursor(cursor_factory=RealDictCursor) as c:
        c.execute("SET LOCAL statement_timeout='30s'")
        c.execute('''SELECT s.id,s.document_id,s.review_status,d.plan_status,
            d.sha256 AS source_sha256,
            ST_IsValid(s.geom) AND ST_SRID(s.geom)=4326 AS valid_geometry,
            encode(sha256(ST_AsEWKB(s.geom)),'hex') AS geometry_sha256
            FROM bronze_ch.vd_pdcom_sectors s
            JOIN bronze_ch.vd_pdcom_documents d ON d.id=s.document_id
            WHERE s.id=%s FOR SHARE OF s,d''',(manifest.get('sector_id'),))
        sector = c.fetchone()
        require(sector is not None, 'sector_missing')
        digest = validate(manifest, sector)
        c.execute('''INSERT INTO bronze_ch.vd_pdcom_sector_qualifications
            (id,sector_id,document_id,source_sha256,geometry_sha256,manifest_sha256,manifest)
            VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO NOTHING''',
            (manifest['manifest_id'],manifest['sector_id'],manifest['document_id'],manifest['source_sha256'],manifest['geometry_sha256'],digest,Json(manifest)))
        c.execute('SELECT manifest,manifest_sha256 FROM bronze_ch.vd_pdcom_sector_qualifications WHERE id=%s',(manifest['manifest_id'],))
        saved = c.fetchone()
        require(saved['manifest'] == manifest and saved['manifest_sha256'] == digest, 'immutable_manifest_conflict')
        return {'manifest_id':manifest['manifest_id'],'manifest_sha256':digest,'status':'qualified_sector_only','published':False,'commune_completed':False}
