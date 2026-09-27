"""Read-only projection of immutable manifests against the current sector snapshot.

Contract eligibility is sector-only, never publication, parcel release or commune
completion. Evidence assertions still require substantive independent review.
"""
from collections import Counter
from datetime import date
from release_manifest import validate


def currentness_blockers(sector):
    evidence = sector.get('validation_evidence')
    if evidence is None:
        evidence = {}
    if not isinstance(evidence, dict):
        return ['live_currentness_evidence_malformed']
    currentness = evidence.get('municipal_currentness')
    if currentness is None:
        return []
    if not isinstance(currentness, dict):
        return ['live_currentness_evidence_malformed']
    if currentness.get('classification') == 'historical_not_current_municipal_reference':
        return ['live_source_not_current_municipal_reference']
    # No other classification is currently defined by an approved reconciliation
    # contract. A manifest assertion cannot silently override live source evidence.
    return ['live_currentness_policy_reconciliation_required']


def project(sectors, qualifications, today=None):
    today = today or date.today()
    live = {str(s['id']): s for s in sectors}
    by_sector = {sid: [] for sid in live}
    records = []
    for row in sorted(qualifications, key=lambda r: str(r['id'])):
        sid = str(row['sector_id'])
        blockers = []
        sector = live.get(sid)
        if sector is None:
            blockers.append('sector_missing')
        else:
            blockers.extend(currentness_blockers(sector))
        manifest = row.get('manifest')
        if not isinstance(manifest, dict):
            blockers.append('manifest_object_required')
        else:
            for stored, body in [('id', 'manifest_id'), ('sector_id', 'sector_id'),
                                 ('document_id', 'document_id'),
                                 ('source_sha256', 'source_sha256'),
                                 ('geometry_sha256', 'geometry_sha256')]:
                if str(row.get(stored)) != manifest.get(body):
                    blockers.append('stored_' + stored + '_mismatch')
            if sector is not None:
                try:
                    digest = validate(manifest, sector, today=today)
                    if digest != row.get('manifest_sha256'):
                        blockers.append('manifest_sha256_mismatch')
                except (ValueError, TypeError, KeyError) as exc:
                    blockers.append(str(exc) if isinstance(exc, ValueError) else 'malformed_manifest_contract')
        result = {'manifest_id': str(row['id']), 'sector_id': sid,
                  'eligible_sector_only': not blockers, 'blocking_gates': blockers}
        records.append(result)
        if sid in by_sector:
            by_sector[sid].append(result)
    summaries = []
    for sid, rows in sorted(by_sector.items()):
        active = [r['manifest_id'] for r in rows if r['eligible_sector_only']]
        blockers = [] if active else sorted(set(
            b for r in rows for b in r['blocking_gates']))
        if not rows:
            blockers = ['qualification_not_recorded'] + currentness_blockers(live[sid])
        summaries.append({'sector_id': sid, 'eligible_sector_only': bool(active),
                          'active_manifest_ids': active, 'blocking_gates': blockers})
    return {'as_of': today.isoformat(), 'scope': 'sector_only', 'read_only': True,
            'publication_authorized': False, 'parcel_release': False,
            'commune_completion_authorized': False,
            'limitation': 'Live contract eligibility only; not verification of reviewer assertions or a release route. Existing private review and parcel gates remain separate.',
            'counts': {'manifests': len(records), 'eligible_manifests': sum(r['eligible_sector_only'] for r in records),
                       'eligible_sectors': sum(s['eligible_sector_only'] for s in summaries)},
            'blocking_gate_counts': dict(Counter(b for s in summaries for b in s['blocking_gates'])),
            'sectors': summaries, 'manifests': records}
