"""Verify frozen provenance and exact offline extraction replay."""
import hashlib,json,subprocess,sys
from pathlib import Path
P=Path(__file__).resolve().parent
manifest=json.loads((P/'manifest.json').read_text())
for name,digest in manifest.items():
    assert hashlib.sha256((P/name).read_bytes()).hexdigest()==digest,name
raw=(P/'extraction.json').read_bytes(); d=json.loads(raw)
assert len(d['groups'])==18 and sorted(x for r in d['groups'] for x in r['historical_sector_ids'])==list(range(1,24))
assert d['qa']['invalid_targets']==[10,11,17]
assert d['qa']['href_mismatches']==[{'target':18,'href':'javascript:// Q','resolved_letter':'R'}]
assert len(d['qa']['overlaps_between_valid_polygons'])==14
assert d['totals']['population']['group_sums']['historical_current']==14712
assert d['totals']['employment']['group_sums']['historical_additional_potential']==4016.5
assert not d['georeferenced'] and not d['receiver_eligible'] and d['baseline_year'] is None
subprocess.run([sys.executable,str(P/'extract.py')],check=True,stdout=subprocess.DEVNULL)
assert (P/'extraction.json').read_bytes()==raw
print('Frozen provenance, 18 groups / 23 sectors, source discrepancies and exact JSON replay verified.')
