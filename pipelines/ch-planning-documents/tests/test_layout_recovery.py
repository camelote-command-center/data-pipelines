import copy
import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
import _pipeline_path  # noqa: F401
import layout_recovery as layout
import layout_recovery_runner as runner
import vd_pdcom_text as bridge


class LayoutRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.extensions=[layout.assemble(s) for s in layout.SOURCE_IDS]

    def test_exact_base_identity_and_append_indices(self):
        self.assertTrue(self.extensions)
        manifest=layout.load()
        total=0
        for e in self.extensions:
            base=e['base'];n=len(base['chunks']);total+=len(e['new_chunks'])
            self.assertEqual(e['chunks'][:n],base['chunks'])
            self.assertEqual((e['document_id'],e['version_id'],e['source_id']),(base['document_id'],base['version_id'],base['source_id']))
            self.assertEqual([x['chunk_index'] for x in e['chunks']],list(range(len(e['chunks']))))
            self.assertEqual(len({x['id'] for x in e['chunks']}),len(e['chunks']))
            layout.validate_delivery(e['metadata'],e['chunks'])
            for b in e['blocks']:
                self.assertNotIn(b['physical_page'],base['review']['historical_reference']['selected_pages'])
                raw=''.join(c['content'].split('\n\n',1)[1] for c in e['new_chunks'] if c['metadata']['block_provenance']['block_id']==b['block_id'])
                self.assertEqual(raw,b['text'])
                self.assertIn(b['block_id'],manifest['accepted_block_ids'])
        self.assertEqual(runner.BASELINE,(26,1510));self.assertEqual(runner.FINAL,(26,1510+total))

    def test_manifest_and_review_hash_fail_closed(self):
        original=Path.read_bytes
        for name in ['manifest.json','independent-content-review.json']:
            def changed(p):return b'{}' if p.name==name else original(p)
            with patch.object(Path,'read_bytes',new=changed),self.assertRaisesRegex(ValueError,'layout_'):
                layout.load()

    def test_invalid_block_mutations_rejected(self):
        m=layout.load();sid=layout.SOURCE_IDS[0]
        mutations=[('source_sha256','0'*64),('text','modified raw text'),('physical_page',self.extensions[0]['base']['review']['historical_reference']['selected_pages'][0]),('bbox_pdf_points',[-1,0,10,10]),('confidence',99),('whole_page_certified',True),('omitted_region_policy','')]
        for key,value in mutations:
            mutated=copy.deepcopy(m);block=next(x for x in mutated['blocks'] if x['source_document_id']==sid);block[key]=value
            with self.subTest(key=key),patch.object(layout,'load',return_value=mutated),self.assertRaises(ValueError):layout.assemble(sid)
        mutated=copy.deepcopy(m);mutated['blocks'].append(copy.deepcopy(mutated['blocks'][0]))
        with patch.object(layout,'load',return_value=mutated),self.assertRaisesRegex(ValueError,'layout_duplicate'):layout.assemble(sid)

    def test_delivery_rejects_old_chunk_missing_extra_changed_metadata(self):
        e=self.extensions[0]
        for chunks in [e['chunks'][1:],e['chunks']+[e['chunks'][0]],e['base']['chunks']]:
            with self.assertRaisesRegex(ValueError,'layout_chunk_union'):layout.validate_delivery(e['metadata'],chunks)
        for index in [0,len(e['base']['chunks'])]:
            chunks=copy.deepcopy(e['chunks']);chunks[index]['content']='changed'
            with self.assertRaisesRegex(ValueError,'layout_chunk_union'):layout.validate_delivery(e['metadata'],chunks)
        meta=copy.deepcopy(e['metadata']);meta['layout_recovery']['whole_pages_certified']=1
        with self.assertRaisesRegex(ValueError,'layout_document_metadata'):layout.validate_delivery(meta,e['chunks'])

    def test_base_replay_accepts_only_exact_cumulative_state_without_writes(self):
        e=self.extensions[0];cur=MagicMock()
        cur.fetchone.side_effect=[(e['metadata'],),(bridge.HISTORICAL_REFERENCE_SOURCE,len(e['chunks']),e['metadata'])]
        cur.fetchall.return_value=[(x,) for x in e['chunks']]
        with patch.object(layout,'verify_bronze'):self.assertTrue(layout.base_replay(cur,e['base']))
        self.assertTrue(all(str(c.args[0]).startswith('SELECT') for c in cur.execute.call_args_list))
        cur=MagicMock();bad=copy.deepcopy(e['metadata']);bad['layout_recovery']['revision']='unknown'
        cur.fetchone.side_effect=[(bad,),(bridge.HISTORICAL_REFERENCE_SOURCE,len(e['chunks']),bad)]
        cur.fetchall.return_value=[(x,) for x in e['chunks']]
        with patch.object(layout,'verify_bronze'),self.assertRaisesRegex(ValueError,'layout_state_not_exact'):layout.base_replay(cur,e['base'])

    def test_exact_append_replay_zero_and_no_nested_commit(self):
        e=self.extensions[0];conn=MagicMock();cur=conn.cursor.return_value.__enter__.return_value;r=e['review']
        cur.fetchone.return_value=(r['source_sha256'],'unverified',r['page_count'],r['source_url'])
        with patch.object(layout.historical,'validate_registered_mapping'),patch.object(layout,'verify_bronze'),patch.object(layout,'read_state',return_value='extended'),patch.object(bridge,'checked_insert') as ins:
            result=layout.persist(conn,e,'op')
        self.assertEqual((result['documents_new'],result['chunks_new']),(0,0))
        self.assertEqual([x.args[2] for x in ins.call_args_list],['planning_document_runs'])
        self.assertFalse(any(str(x.args[0]).startswith(('UPDATE','ALTER','INSERT')) for x in cur.execute.call_args_list))
        conn.commit.assert_not_called()
        with self.assertRaisesRegex(RuntimeError,'batch owner'):runner.OwnedTransaction(conn).commit()

    def test_append_updates_only_metadata_count_and_inserts_delta(self):
        e=self.extensions[0];conn=MagicMock();cur=conn.cursor.return_value.__enter__.return_value;r=e['review'];cur.rowcount=1
        cur.fetchone.return_value=(r['source_sha256'],'unverified',r['page_count'],r['source_url'])
        cur.fetchall.return_value=[('knowledge_ch.chunks','O'),('knowledge_ch.documents','O')]
        with patch.object(layout.historical,'validate_registered_mapping'),patch.object(layout,'verify_bronze'),patch.object(layout,'read_state',side_effect=['base','extended']),patch.object(bridge,'checked_insert') as ins:
            result=layout.persist(conn,e,'op')
        self.assertEqual(result['chunks_new'],len(e['new_chunks']))
        self.assertEqual([x.args[3]['id'] for x in ins.call_args_list if x.args[2]=='chunks'],[x['id'] for x in e['new_chunks']])
        self.assertFalse(any(x.args[2] in ('planning_document_sources','planning_document_versions','documents') for x in ins.call_args_list))
        updates=[x for x in cur.execute.call_args_list if str(x.args[0]).startswith('UPDATE')]
        self.assertEqual(len(updates),1);self.assertIn('SET chunk_count=%s,raw_metadata=%s',updates[0].args[0]);self.assertIn('AND chunk_count=%s AND raw_metadata=%s',updates[0].args[0])
        self.assertFalse(any('DELETE' in str(x.args[0]) for x in cur.execute.call_args_list))

    def test_bad_cas_and_unknown_state_reject(self):
        e=self.extensions[0];conn=MagicMock();cur=conn.cursor.return_value.__enter__.return_value;r=e['review'];cur.rowcount=0
        cur.fetchone.return_value=(r['source_sha256'],'unverified',r['page_count'],r['source_url']);cur.fetchall.return_value=[('knowledge_ch.chunks','O'),('knowledge_ch.documents','O')]
        with patch.object(layout.historical,'validate_registered_mapping'),patch.object(layout,'verify_bronze'),patch.object(layout,'read_state',return_value='base'),patch.object(bridge,'checked_insert'),self.assertRaisesRegex(ValueError,'compare_and_swap'):layout.persist(conn,e,'op')
        cur=MagicMock();cur.fetchone.return_value=(bridge.HISTORICAL_REFERENCE_SOURCE,999,{})
        cur.fetchall.return_value=[]
        with self.assertRaisesRegex(ValueError,'layout_state_not_exact'):layout.read_state(cur,e)

    def test_all_five_kinds_and_implementation_are_guarded(self):
        hashes=runner.artifact_hashes(self.extensions)
        self.assertEqual(hashes['source_kinds'],list(bridge.TEXT_SOURCES))
        self.assertEqual(set(hashes['implementation']),{'historical_reference.py','layout_recovery.py','layout_recovery_runner.py','vd_pdcom_text.py'})

    def test_receiver_rejects_old_row_drift_before_mutation(self):
        e=self.extensions[0];conn=MagicMock();cur=conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value=(e['metadata'],)
        source={'id':e['document_id'],'raw_metadata':e['metadata'],'chunk_count':len(e['chunks']),'title':'unchanged'}
        target=copy.deepcopy(source);target['title']='unauthorized change'
        cur.fetchall.side_effect=[[('knowledge_ch','v_documents_sync','lamap_db_foreign','lamap_db_server','lamap_db',True)],[('knowledge_ch','v_chunks_sync','lamap_db_foreign','lamap_db_server','lamap_db',True)],[(source,)],[(target,)]]
        with patch.object(layout,'read_state',return_value='extended'),self.assertRaisesRegex(ValueError,'layout_receiver_document_drift'):layout.deliver(conn,e['document_id'])
        self.assertFalse(any(str(x.args[0]).startswith(('UPDATE','INSERT','DELETE')) for x in cur.execute.call_args_list))


    def test_receiver_append_never_updates_or_deletes_old_chunks(self):
        e=self.extensions[0];conn=MagicMock();cur=conn.cursor.return_value.__enter__.return_value;cur.rowcount=1
        cur.fetchone.return_value=(e['metadata'],)
        doc={'id':e['document_id'],'raw_metadata':e['metadata'],'chunk_count':len(e['chunks']),'title':'unchanged'}
        olddoc=copy.deepcopy(doc);olddoc.update(raw_metadata=e['base']['metadata'],chunk_count=len(e['base']['chunks']))
        chunks=sorted(e['chunks'],key=lambda c:c['id']);basechunks=sorted(e['base']['chunks'],key=lambda c:c['id']);columns=list(chunks[0])
        cur.fetchall.side_effect=[[('knowledge_ch','v_documents_sync','lamap_db_foreign','lamap_db_server','lamap_db',True)],[('knowledge_ch','v_chunks_sync','lamap_db_foreign','lamap_db_server','lamap_db',True)],[(doc,)],[(olddoc,)],[(x,) for x in chunks],[(x,) for x in basechunks],[(x,) for x in columns],[(x,) for x in columns],[(doc,)],[(x,) for x in chunks]]
        with patch.object(layout,'read_state',return_value='extended'):
            self.assertEqual(layout.deliver(conn,e['document_id']),{'knowledge_documents':1,'knowledge_chunks':len(chunks)})
        updates=[x for x in cur.execute.call_args_list if str(x.args[0]).startswith('UPDATE')]
        self.assertEqual(len(updates),1);self.assertTrue(updates[0].args[0].startswith('UPDATE lamap_db_foreign.knowledge_documents SET chunk_count=%s,raw_metadata=%s'))
        self.assertFalse(any('UPDATE lamap_db_foreign.knowledge_chunks' in str(x.args[0]) or 'DELETE' in str(x.args[0]) for x in cur.execute.call_args_list))
        last_insert=next(x for x in cur.execute.call_args_list if 'INSERT INTO lamap_db_foreign.knowledge_chunks' in str(x.args[0]))
        self.assertEqual(last_insert.args[1][1],[x['id'] for x in e['new_chunks']])

    def test_bridge_base_replay_returns_before_immutable_insert(self):
        e=self.extensions[0];b=e['base'];r=b['review'];conn=MagicMock();cur=conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value=(r['source_sha256'],r['plan_status'],r['page_count'],r['source_url']);cur.fetchall.return_value=[]
        with patch.object(bridge,'build_bundle',return_value=b),patch.object(layout.historical,'validate_registered_mapping'),patch.object(layout,'base_replay',return_value=len(e['chunks'])),patch.object(bridge,'checked_insert') as ins:
            result=bridge.persist(conn,b,'same-existing-op',Path('unused.pdf'))
        self.assertEqual((result['documents_new'],result['chunks_new']),(0,0));self.assertTrue(result['layout_extension_preserved']);self.assertEqual(result['searchable_chunks'],len(e['chunks']));ins.assert_not_called()

    def test_unknown_commit_outcome_never_claims_rollback(self):
        import psycopg2
        root=Path('/Users/a/LLM_Work/re-llm/vaud-pdcom/oct6-layout-recovery')
        ops={s:{'commit':s+'-commit','rehearse':s+'-rehearse'} for s in runner.SLUGS};ops['monitor']='monitor'
        before={'protected':'unchanged'};hashes={'code':'frozen'};datasets=[{'national':'unchanged'}]
        baseline={'knowledge_documents':{'rows':26},'knowledge_chunks':{'rows':1510}}
        final={'knowledge_documents':{'rows':26},'knowledge_chunks':{'rows':runner.FINAL[1]}}
        receipt={'rolled_back':True,'operations':ops,'before':before,'artifact_hashes':hashes,'national_before':datasets,'baseline_corpus':baseline}
        source,receiver,monitor=MagicMock(),MagicMock(),MagicMock();source.commit.side_effect=psycopg2.OperationalError('lost ack')
        def read(path,*args,**kwargs):
            if path.name=='layout-recovery-operations.json':return json.dumps(ops)
            if path.name=='layout-recovery-rollback.json':return json.dumps(receipt)
            return '{}'
        with patch('sys.argv',['runner','--mode','commit','--evidence-dir',str(root)]),patch.object(Path,'exists',return_value=True),patch.object(Path,'read_text',new=read),patch.object(runner,'load_bundles',return_value=self.extensions),patch.object(runner,'artifact_hashes',return_value=hashes),patch.object(runner,'connect',side_effect=[source,receiver,monitor]),patch.object(runner,'fingerprint',return_value=before),patch.object(runner,'corpus_parity',side_effect=[baseline,final]),patch.object(runner,'national',return_value=datasets),patch.object(runner,'apply',return_value=[]),patch.object(layout.historical,'validate_registered_mapping'),patch.object(bridge,'monitor') as log,patch.object(runner,'save') as saved:
            with self.assertRaises(psycopg2.OperationalError):runner.main()
        self.assertEqual([x.args[2] for x in log.call_args_list],['running'])
        path,proof=saved.call_args.args;self.assertEqual(path.name,'layout-recovery-commit-outcome-unknown.json');self.assertFalse(proof['rolled_back']);self.assertFalse(proof['automatic_retry_permitted'])


if __name__=='__main__':unittest.main()
