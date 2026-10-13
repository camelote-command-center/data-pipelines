import copy
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import struct
import sys
import unittest
from unittest.mock import MagicMock, patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_release_manifest as fixtures
import qualified_preview as qp
from release_manifest import validate

TODAY = date(2026, 9, 22)

class QualifiedPreviewTests(unittest.TestCase):
    def setUp(self):
        f = fixtures.ReleaseManifestTests(); f.setUp()
        self.sector, self.manifest = f.sector, f.m
        # Synthetic valid EWKB polygon, not acquired municipal evidence.
        ring = [(6.,46.), (6.01,46.), (6.01,46.01), (6.,46.)]
        raw = struct.pack('<BIIII', 1, 0x20000003, 4326, 1, len(ring))
        raw += b''.join(struct.pack('<dd', *p) for p in ring)
        self.sector['geometry_ewkb_hex'] = raw.hex()
        self.sector['geometry_sha256'] = self.manifest['geometry_sha256'] = hashlib.sha256(raw).hexdigest()
        self.row = {k: self.manifest[k] for k in ('sector_id','document_id','source_sha256','geometry_sha256')}
        self.row.update(id=self.manifest['manifest_id'], manifest=self.manifest,
                        manifest_sha256=validate(self.manifest,self.sector,TODAY))

    def run_preview(self, sectors=None, rows=None, today=TODAY):
        return qp.preview([self.sector] if sectors is None else sectors,
                          [self.row] if rows is None else rows, today=today)

    def test_exact_payload_preserves_reviews_and_does_not_mutate_inputs(self):
        before=copy.deepcopy((self.sector,self.row)); p=self.run_preview(); s=p['sectors'][0]
        self.assertEqual(s['geometry']['value'],self.sector['geometry_ewkb_hex'])
        self.assertEqual(s['active_qualifications'][0]['geography'],self.manifest['geography'])
        self.assertEqual(s['active_qualifications'][0]['semantics'],self.manifest['semantics'])
        self.assertEqual(before,(self.sector,self.row))
        for key in ('publication_authorized','parcel_release','commune_completion_authorized'):
            self.assertIs(p[key],False); self.assertIs(s[key],False)
        self.assertTrue(p['requires_live_reprojection_before_use'])

    def test_empty_eligible_set_never_falls_back_to_private_rows(self):
        for sectors,rows in [([],[]),([self.sector],[]),([],[self.row])]:
            p=self.run_preview(sectors,rows);self.assertEqual(p['sectors'],[])
            self.assertEqual(p['counts']['preview_sectors'],0)

    def test_live_projection_reused(self):
        with patch.object(qp,'project', wraps=qp.project) as project:
            self.run_preview();project.assert_called_once_with([self.sector],[self.row],today=TODAY)

    def test_source_geometry_status_and_revocation_invalidate_fresh_payload(self):
        self.assertEqual(len(self.run_preview()['sectors']),1)
        for key,value in [('source_sha256','f'*64),('geometry_sha256','f'*64),('review_status','superseded'),('plan_status','unverified'),('valid_geometry',False)]:
            changed={**self.sector,key:value}
            with self.subTest(key=key):self.assertEqual(self.run_preview([changed])['sectors'],[])
        self.assertEqual(self.run_preview(rows=[])['sectors'],[])
        self.assertEqual(self.run_preview(today=date(2026,10,21))['sectors'],[])

    def test_exact_geometry_body_independently_hash_checked(self):
        for body in [None,'not hex','',self.sector['geometry_ewkb_hex']+'00']:
            p=self.run_preview([{**self.sector,'geometry_ewkb_hex':body}])
            self.assertEqual(p['sectors'],[]); self.assertEqual(len(p['payload_holds']),1)

    def test_unknown_or_negative_currentness_and_missing_precision_fail_closed(self):
        for classification in ['historical_not_current_municipal_reference','unestablished','current_but_unrecognized']:
            s={**self.sector,'validation_evidence':{'municipal_currentness':{'classification':classification}}}
            self.assertEqual(self.run_preview([s])['sectors'],[])
        for field in ['source_precision_m','accepted_error_limit_m','precision_basis']:
            row=copy.deepcopy(self.row);row['manifest']['geography'].pop(field)
            self.assertEqual(self.run_preview(rows=[row])['sectors'],[])

    def test_digest_identity_and_unknown_manifest_fail_closed(self):
        for key,value in [('manifest_sha256','f'*64),('source_sha256','f'*64),('manifest',None)]:
            self.assertEqual(self.run_preview(rows=[{**self.row,key:value}])['sectors'],[])
        changed=copy.deepcopy(self.row);changed['manifest']['reviewer']='changed'
        self.assertEqual(self.run_preview(rows=[changed])['sectors'],[])

    def test_duplicate_input_ids_rejected_not_silently_overwritten(self):
        with self.assertRaisesRegex(ValueError,'duplicate_sector'):self.run_preview([self.sector,self.sector])
        with self.assertRaisesRegex(ValueError,'duplicate_manifest'):self.run_preview(rows=[self.row,self.row])

    def test_multiple_active_reviews_retained_separately_no_duplicate_geometry(self):
        row=copy.deepcopy(self.row);row['id']=row['manifest']['manifest_id']='00000000-0000-0000-0000-000000000004'
        row['manifest']['version']['reservations']=['Synthetic reservation retained']
        row['manifest_sha256']=validate(row['manifest'],self.sector,TODAY)
        p=self.run_preview(rows=[row,self.row]);self.assertEqual(len(p['sectors']),1)
        self.assertEqual(len(p['sectors'][0]['active_qualifications']),2)
        self.assertEqual(p,self.run_preview(rows=[self.row,row]))
        self.assertEqual(p['sectors'][0]['active_qualifications'][1]['version']['reservations'],['Synthetic reservation retained'])

    def test_unreviewed_extra_claims_not_exported(self):
        self.manifest['capacity']=1000;self.manifest['semantics']['parcel_rights']=['invented']
        self.row['manifest_sha256']=validate(self.manifest,self.sector,TODAY)
        s=self.run_preview()['sectors'][0]
        self.assertNotIn('capacity',json.dumps(s));self.assertNotIn('invented',json.dumps(s))

    def test_database_audit_is_one_readonly_snapshot_without_commit(self):
        conn=MagicMock();c=conn.cursor.return_value.__enter__.return_value
        c.fetchone.return_value={'read_only':'on','today':TODAY,'snapshot_at':datetime(2026,9,22,tzinfo=timezone.utc)}
        c.fetchall.side_effect=[[self.sector],[self.row]]
        p=qp.audit(conn)
        self.assertEqual(p['counts']['preview_sectors'],1)
        conn.set_session.assert_called_once_with(readonly=True,isolation_level='REPEATABLE READ')
        conn.commit.assert_not_called()
        self.assertTrue(all(call.args[0].startswith(('SELECT','SET LOCAL')) for call in c.execute.call_args_list))
        self.assertFalse(any('lamap' in call.args[0] for call in c.execute.call_args_list))

    def test_cli_failure_rolls_back_closes_and_emits_no_snapshot(self):
        import os
        conn=MagicMock()
        output='/Users/a/LLM_Work/re-llm/vaud-pdcom/oct6-qualified-preview/never-created-test.json'
        with patch('sys.argv',['qualified_preview','--output',output]), patch.dict(os.environ,{'RE_LLM_DB_URL':'synthetic://not-a-real-connection'}), patch('psycopg2.connect',return_value=conn), patch.object(qp,'audit',side_effect=ValueError('invalid live snapshot')), patch.object(Path,'open') as opened:
            with self.assertRaisesRegex(ValueError,'invalid live snapshot'):qp.main()
        conn.rollback.assert_called_once();conn.close.assert_called_once()
        conn.commit.assert_not_called();opened.assert_not_called()

    def test_database_refuses_non_readonly_session(self):
        conn=MagicMock();c=conn.cursor.return_value.__enter__.return_value;c.fetchone.return_value={'read_only':'off'}
        with self.assertRaisesRegex(ValueError,'readonly_snapshot'):qp.audit(conn)
        c.fetchall.assert_not_called();conn.commit.assert_not_called()

if __name__=='__main__':unittest.main()
