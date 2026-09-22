import copy
from datetime import date
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from release_manifest import validate

class ReleaseManifestTests(unittest.TestCase):
    def setUp(self):
        self.sector={'id':'00000000-0000-0000-0000-000000000001','document_id':'00000000-0000-0000-0000-000000000002','source_sha256':'a'*64,'geometry_sha256':'b'*64,'plan_status':'approved','valid_geometry':True,'review_status':'review_required'}
        self.m={'schema_version':1,'manifest_id':'00000000-0000-0000-0000-000000000003','sector_id':self.sector['id'],'document_id':self.sector['document_id'],'source_sha256':'a'*64,'geometry_sha256':'b'*64,'reviewer':'Test reviewer (synthetic fixture)','reviewed_on':'2026-09-20','valid_until':'2026-10-20','scope':'sector_only','parcel_release':False,'publication_authorized':False,'version':{'status':'approved','currentness_verified':True,'reservations':[],'evidence_uri':'fixture://version','review_summary':'Synthetic approved version'},'geography':{'evidence_uri':'fixture://controls','review_summary':'Synthetic independent review','independent_review':True,'method':'independent_controls','source_precision_m':2,'maximum_error_m':3,'accepted_error_limit_m':5,'precision_basis':'Synthetic source precision assessment','tolerance_basis':'Synthetic intended-use assessment','whole_outline_reviewed':True,'controls_held_out_of_fit':True,'control_identity_reviewed':True},'semantics':{'evidence_uri':'fixture://semantics','review_summary':'Synthetic category review','source_category':'indicative densification','meaning':'planning intention','limits':'No parcel rights','grants_parcel_rights':False}}
    def check(self,m=None,s=None):return validate(m or self.m,s or self.sector,today=date(2026,9,22))
    def test_complete_synthetic_manifest_hash_stable(self):
        self.assertEqual(self.check(),self.check(dict(reversed(list(self.m.items())))))
    def test_changed_source_geometry_or_identity_rejected(self):
        for key in ('source_sha256','geometry_sha256','sector_id','document_id'):
            m=copy.deepcopy(self.m);m[key]=('c'*64 if 'sha' in key else '00000000-0000-0000-0000-000000000009')
            with self.subTest(key=key),self.assertRaises(ValueError):self.check(m)
    def test_each_required_review_section_rejected_when_missing(self):
        for key in ('version','geography','semantics','reviewer','reviewed_on','valid_until'):
            m=copy.deepcopy(self.m);del m[key]
            with self.subTest(key=key),self.assertRaises(ValueError):self.check(m)
    def test_nonfinite_and_invalid_numeric_review_rejected(self):
        for key in ('source_precision_m','maximum_error_m','accepted_error_limit_m'):
            for value in (float('nan'),float('inf'),-1,0,True,None):
                m=copy.deepcopy(self.m);m['geography'][key]=value
                with self.subTest(key=key,value=value),self.assertRaises(ValueError):self.check(m)
    def test_expired_future_or_outside_tolerance_rejected(self):
        for key,value in [('valid_until','2026-09-21'),('reviewed_on','2026-09-23')]:
            m=copy.deepcopy(self.m);m[key]=value
            with self.assertRaises(ValueError):self.check(m)
        self.m['geography']['maximum_error_m']=6
        with self.assertRaises(ValueError):self.check()
    def test_reservations_must_be_preserved(self):
        self.sector['plan_status']='approved_with_reservation';self.m['version']['status']='approved_with_reservation'
        with self.assertRaises(ValueError):self.check()
        self.m['version']['reservations']=['Synthetic reservation retained'];self.check()
    def test_nonindependent_review_or_rights_or_promotion_rejected(self):
        for section,key,value in [('geography','controls_held_out_of_fit',False),('geography','control_identity_reviewed',False),('geography','independent_review',False),('version','currentness_verified',False),('semantics','grants_parcel_rights',True)]:
            m=copy.deepcopy(self.m);m[section][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):self.check(m)
        for key,value in [('scope','whole_commune'),('parcel_release',True),('publication_authorized',True)]:
            m=copy.deepcopy(self.m);m[key]=value
            with self.assertRaises(ValueError):self.check(m)
    def test_bad_geometry_or_superseded_or_consultation_rejected(self):
        for key,value in [('valid_geometry',False),('review_status','superseded'),('plan_status','consultation')]:
            s=copy.deepcopy(self.sector);s[key]=value
            with self.assertRaises(ValueError):self.check(s=s)

if __name__=='__main__':unittest.main()
