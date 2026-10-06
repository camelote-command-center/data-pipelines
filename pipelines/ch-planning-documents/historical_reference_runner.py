"""Guarded two-document historical-reference batch. Default mode is read-only.

Rehearsal owns one RE-LLM transaction and always rolls it back. Commit requires
the exact rehearsal receipt, fixtures, code and unchanged pre-existing rows.
No source-status, commune-mapping, spatial or national-freshness mutation.
"""
import argparse
import hashlib
import json
from pathlib import Path
import uuid

import psycopg2
from psycopg2 import sql

import historical_reference as reference
import vd_pdcom_text as bridge

SLUGS = ('blonay', 'puidoux')
SAFE_ROOT = Path('/Users/a/LLM_Work/re-llm').resolve()
BASELINE = (13, 1272)
FINAL = (15, 1345)


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, default=str)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def save(path, data):
    """Never silently overwrite evidence from an earlier operation."""
    with path.open('x') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write('\n')


def registered_entry(value, project):
    if isinstance(value, dict):
        if project in value and isinstance(value[project], dict):
            return value[project]
        if any(value.get(k) == project for k in ('slug', 'name', 'id')):
            return value
        for child in value.values():
            result = registered_entry(child, project)
            if result:
                return result
    if isinstance(value, list):
        for child in value:
            result = registered_entry(child, project)
            if result:
                return result


def connect(registry, project):
    entry = registered_entry(registry, project)
    require(entry and entry.get('session_pooler_uri'), 'registered_session_pooler_missing:' + project)
    conn = psycopg2.connect(entry['session_pooler_uri'], connect_timeout=15)
    conn.set_session(readonly=True)
    with conn.cursor() as cur:
        cur.execute("SELECT 1,current_setting('transaction_read_only')")
        require(cur.fetchone() == (1, 'on'), 'readonly_connection_check_failed:' + project)
    conn.rollback()
    return conn


class OwnedTransaction:
    def __init__(self, conn):
        self.conn = conn

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def commit(self):
        raise RuntimeError('Only the batch owner may commit')


def rollback_quietly(conn):
    """A lost connection must not hide a commit-uncertainty receipt."""
    try:
        conn.rollback()
    except psycopg2.Error:
        pass


def uncertain_commit_receipt(operations, bundles, hashes):
    return {'commit_attempted': True, 'commit_outcome': 'unknown',
            'rolled_back': False, 'automatic_retry_permitted': False,
            'required_action': 'Reconcile exact operation/document IDs through fresh source and direct receiver reads before any retry.',
            'operations': operations, 'document_ids': [b['document_id'] for b in bundles],
            'artifact_hashes': hashes}


def load_bundles(evidence_dir):
    bundles = []
    for slug in SLUGS:
        bundle = json.loads((reference.ROOT / (slug + '-bundle.json')).read_text())
        rebuilt = bridge.build_bundle(evidence_dir / (slug + '.pdf'), bundle['review'])
        require(rebuilt == bundle, 'bundle_not_exact_reviewed_pdf:' + slug)
        reference.validate_delivery(bundle['metadata'], bundle['chunks'])
        bundles.append(bundle)
    require(len(bundles) == 2 and sum(len(b['chunks']) for b in bundles) == 73, 'exact_two_document_batch_required')
    return bundles


def artifact_hashes(bundles):
    code = Path(__file__).parent
    return {'bundles': [digest(b) for b in bundles],
            'implementation': {n: hashlib.sha256((code / n).read_bytes()).hexdigest()
                               for n in ('historical_reference.py', 'historical_reference_runner.py', 'vd_pdcom_text.py')},
            'source_kinds': list(bridge.TEXT_SOURCES)}


def corpus_ids(cur):
    cur.execute('SELECT id::text FROM knowledge_ch.documents WHERE source=ANY(%s) ORDER BY id', (list(bridge.TEXT_SOURCES),))
    return [row[0] for row in cur.fetchall()]


def corpus_parity(conn, receiver=None):
    """Compare every synced field of all five text source kinds, not just the new two."""
    result = {}
    with conn.cursor() as cur:
        ids = corpus_ids(cur)
        cur.execute('SELECT id::text FROM lamap_db_foreign.knowledge_documents WHERE source=ANY(%s) ORDER BY id', (list(bridge.TEXT_SOURCES),))
        require([row[0] for row in cur.fetchall()] == ids, 'all_corpus_fdw_document_set_mismatch')
        if receiver is not None:
            with receiver.cursor() as rq:
                rq.execute('SELECT id::text FROM lia.knowledge_documents WHERE source=ANY(%s) ORDER BY id', (list(bridge.TEXT_SOURCES),))
                require([row[0] for row in rq.fetchall()] == ids, 'all_corpus_direct_document_set_mismatch')
        for view, target, key in [('v_documents_sync', 'knowledge_documents', 'id'), ('v_chunks_sync', 'knowledge_chunks', 'document_id')]:
            cur.execute(sql.SQL('SELECT to_jsonb(t) FROM knowledge_ch.{} t WHERE {}=ANY(%s::uuid[]) ORDER BY id').format(sql.Identifier(view), sql.Identifier(key)), (ids,))
            source = [row[0] for row in cur.fetchall()]
            cur.execute(sql.SQL('SELECT to_jsonb(t) FROM lamap_db_foreign.{} t WHERE {}=ANY(%s::uuid[]) ORDER BY id').format(sql.Identifier(target), sql.Identifier(key)), (ids,))
            require(source == [row[0] for row in cur.fetchall()], 'all_corpus_fdw_mismatch:' + target)
            if receiver is not None:
                with receiver.cursor() as rq:
                    rq.execute(sql.SQL('SELECT to_jsonb(t) FROM lia.{} t WHERE {}=ANY(%s::uuid[]) ORDER BY id').format(sql.Identifier(target), sql.Identifier(key)), (ids,))
                    require(source == [row[0] for row in rq.fetchall()], 'all_corpus_direct_receiver_mismatch:' + target)
            result[target] = {'rows': len(source), 'sha256': digest(source)}
    return result


def totals(parity):
    return parity['knowledge_documents']['rows'], parity['knowledge_chunks']['rows']


def fingerprint(conn, bundles, owned_operations=()):
    """Protect old text, exact registered sources/mappings and all delivered spatial rows."""
    doc_ids = [b['document_id'] for b in bundles]
    source_ids = [b['source_id'] for b in bundles]
    version_ids = [b['version_id'] for b in bundles]
    out = {}
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout='120s'")
        existing = [i for i in corpus_ids(cur) if i not in doc_ids]
        tables = {
            'knowledge_ch.documents': ('id=ANY(%s::uuid[])', (existing,)),
            'knowledge_ch.chunks': ('document_id=ANY(%s::uuid[])', (existing,)),
            'lamap_db_foreign.knowledge_documents': ('id=ANY(%s::uuid[])', (existing,)),
            'lamap_db_foreign.knowledge_chunks': ('document_id=ANY(%s::uuid[])', (existing,)),
            'bronze_ch.planning_document_sources': ('source=ANY(%s) AND NOT(id=ANY(%s::uuid[]))', (list(bridge.TEXT_SOURCES), source_ids)),
            'bronze_ch.planning_document_versions': ('source_id IN (SELECT id FROM bronze_ch.planning_document_sources WHERE source=ANY(%s)) AND NOT(id=ANY(%s::uuid[]))', (list(bridge.TEXT_SOURCES), version_ids)),
            'bronze_ch.planning_document_runs': ("scope->>'document_id' IN (SELECT source_key FROM bronze_ch.planning_document_sources WHERE source=ANY(%s)) AND NOT(id=ANY(%s::uuid[]))", (list(bridge.TEXT_SOURCES), list(owned_operations))),
        }
        for table in ('bronze_ch.vd_pdcom_documents', 'bronze_ch.vd_pdcom_document_communes',
                      'bronze_ch.vd_pdcom_coverage',
                      'bronze_ch.vd_pdcom_communes', 'bronze_ch.vd_pdcom_sector_qualifications',
                      'bronze_ch.vd_pdcom_sectors', 'bronze_ch.vd_pdcom_parcel_candidates',
                      'gold_ch.v_vd_pdcom_review_sectors', 'gold_ch.v_vd_pdcom_review_parcels',
                      'gold_ch.v_vd_pdcom_review_site_scenarios',
                      'lamap_db_foreign.vd_pdcom_review_sectors', 'lamap_db_foreign.vd_pdcom_review_parcels',
                      'lamap_db_foreign.vd_pdcom_review_site_scenarios'):
            tables[table] = ('TRUE', ())
        for table, (where, args) in tables.items():
            cur.execute('SELECT md5(to_jsonb(t)::text) FROM ' + table + ' t WHERE ' + where + ' ORDER BY 1', args)
            rows = [row[0] for row in cur.fetchall()]
            out[table] = {'rows': len(rows), 'sha256': digest(rows)}
        cur.execute("SELECT tgrelid::regclass::text,tgenabled FROM pg_trigger WHERE tgname='classify_on_insert' AND tgrelid IN ('knowledge_ch.documents'::regclass,'knowledge_ch.chunks'::regclass) ORDER BY 1")
        out['classifiers'] = [list(row) for row in cur.fetchall()]
        require(out['classifiers'] == [['knowledge_ch.chunks', 'O'], ['knowledge_ch.documents', 'O']], 'classifier_state_changed')
    require(out['gold_ch.v_vd_pdcom_review_sectors']['rows'] == 212 and out['gold_ch.v_vd_pdcom_review_parcels']['rows'] == 8776, 'spatial_baseline_changed')
    return out


def national(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT to_jsonb(t) FROM public.datasets t WHERE code IN ('ch_planning_documents','ch_planning_document_text') ORDER BY code")
        rows = [row[0] for row in cur.fetchall()]
    require(len(rows) == 2, 'national_dataset_registration_changed')
    return rows


def apply(conn, bundles, operations, phase, evidence_dir):
    reports = []
    owned = OwnedTransaction(conn)
    for slug, bundle in zip(SLUGS, bundles):
        operation = operations[slug][phase]
        first = bridge.persist(owned, bundle, operation, evidence_dir / (slug + '.pdf'))
        replay = bridge.persist(owned, bundle, operation, evidence_dir / (slug + '.pdf'))
        require(first['documents_new'] == 1 and replay['documents_new'] == replay['chunks_new'] == 0, 'batch_idempotency_changed')
        delivery = bridge.deliver(owned, bundle['document_id'])
        require(bridge.deliver(owned, bundle['document_id']) == delivery == {'knowledge_documents': 1, 'knowledge_chunks': len(bundle['chunks'])}, 'batch_delivery_replay_changed')
        report = dict(first, receiver=delivery, double_apply=True, double_delivery=True, operation_id=operation)
        bridge.finish_run(owned, operation, report)
        reports.append(report)
    return reports


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('inspect', 'rehearse', 'commit'), default='inspect')
    parser.add_argument('--evidence-dir', type=Path, required=True, help='Approved local folder containing blonay.pdf and puidoux.pdf')
    parser.add_argument('--registry', type=Path, default=Path('/Users/a/supabase-registry/supabase-projects.json'))
    args = parser.parse_args()
    root = args.evidence_dir.resolve()
    require(root.is_relative_to(SAFE_ROOT), 'evidence_output_outside_approved_root')
    bundles = load_bundles(root)
    hashes = artifact_hashes(bundles)
    ops_path = root / 'historical-reference-operations.json'
    if not ops_path.exists():
        save(ops_path, {**{slug: {phase: str(uuid.uuid4()) for phase in ('rehearse', 'commit')} for slug in SLUGS}, 'monitor': str(uuid.uuid4())})
    operations = json.loads(ops_path.read_text())
    registry = json.loads(args.registry.read_text())
    conn = connect(registry, 're-llm')
    receiver = connect(registry, 'lamap-db')
    monitor = connect(registry, 'pixxels-data')
    committed = False
    commit_attempted = False
    monitor_started = False
    try:
        before = fingerprint(conn, bundles)
        baseline = corpus_parity(conn, receiver)
        require(totals(baseline) == BASELINE, 'expected_13_documents_1272_chunks_before_batch')
        datasets = national(monitor)
        for bundle in bundles:
            with conn.cursor() as cur:
                reference.validate_registered_mapping(cur, bundle['review'])
        conn.rollback(); receiver.rollback(); monitor.rollback()
        common = {'artifact_hashes': hashes, 'operations': operations, 'before': before, 'national_before': datasets, 'baseline_corpus': baseline}
        if args.mode == 'inspect':
            save(root / ('historical-reference-inspection-' + digest(hashes)[:12] + '.json'), dict(common, read_only=True, expected_final=list(FINAL)))
            print('Read-only inspection passed: 13 documents / 1272 chunks; exact two-document batch prepared.')
            return
        receipt_path = root / 'historical-reference-rollback.json'
        if args.mode == 'commit':
            rehearsal = json.loads(receipt_path.read_text())
            require(rehearsal['rolled_back'] is True and all(rehearsal[k] == v for k, v in common.items()), 'rehearsal_or_baseline_changed')
            monitor.set_session(readonly=False)
            bridge.monitor(monitor, operations['monitor'], 'running', {})
            monitor.commit()
            monitor_started = True
        conn.set_session(readonly=False)
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('blonay-puidoux-historical-reference'))")
        reports = apply(conn, bundles, operations, args.mode, root)
        owned_ids = [operations[slug][args.mode] for slug in SLUGS]
        inside = fingerprint(conn, bundles, owned_ids)
        require(inside == before, 'preexisting_rows_or_source_or_spatial_state_changed')
        all_rows = corpus_parity(conn)
        require(totals(all_rows) == FINAL, 'expected_15_documents_1345_chunks_after_batch')
        report = dict(common, reports=reports, inside_excluding_owned=inside, final_corpus=all_rows,
                      documents_new=2, chunks_new=73, physical_pages=219, selected_physical_pages=44,
                      spatial_increment=0, national_complete=False, fully_certified_pages=0)
        if args.mode == 'rehearse':
            conn.rollback(); conn.set_session(readonly=True)
            require(fingerprint(conn, bundles) == before and corpus_parity(conn, receiver) == baseline, 'rollback_not_exact')
            require(national(monitor) == datasets, 'national_fields_changed')
            save(receipt_path, dict(report, rolled_back=True, committed=False))
            print('Owned rollback passed, including old corpus and exact source/FDW/direct receiver parity.')
        else:
            commit_attempted = True
            conn.commit(); committed = True
            save(root / 'historical-reference-committed.json', dict(report, committed=True))
            bridge.monitor(monitor, operations['monitor'], 'partial', report)
            monitor.commit()
            conn.set_session(readonly=True)
            receiver.close()
            receiver = connect(registry, 'lamap-db')
            require(fingerprint(conn, bundles, owned_ids) == before, 'postcommit_baseline_mismatch')
            require(corpus_parity(conn, receiver) == all_rows and national(monitor) == datasets, 'postcommit_parity_or_national_mismatch')
            save(root / 'historical-reference-verified.json', dict(committed=True, complete_corpus_parity=True,
                 corpus=all_rows, national_unchanged=True, classifiers_enabled=True, operations=operations))
            print('Verified 15 documents / 1345 chunks; existing corpus, source registry, spatial state and national fields unchanged.')
    except Exception:
        rollback_quietly(conn)
        if commit_attempted and not committed:
            save(root / 'historical-reference-commit-outcome-unknown.json', uncertain_commit_receipt(operations, bundles, hashes))
            print('Commit acknowledgement failed; outcome unknown. Reconcile stable IDs with fresh source/receiver reads before any retry.')
        elif monitor_started and not committed:
            monitor.rollback()
            bridge.monitor(monitor, operations['monitor'], 'failed', {'documents_new': 0, 'error': 'Historical reference batch rolled back; inspect local evidence', 'national_complete': False})
            monitor.commit()
        if committed:
            print('Source batch committed; inspect saved committed receipt before retrying anything.')
        raise
    finally:
        for connection in (conn, receiver, monitor):
            rollback_quietly(connection)
            connection.close()


if __name__ == '__main__':
    main()
