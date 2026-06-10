"""Post-fix Cologny re-score (Part 1 of generalization brief). DIAGNOSTIC ONLY.

Rules (the scoring standard from here on):
- TRUTH = MAJORITY-coverage: dominant category per parcel only when its
  overlap_pct >= 50; else 'none'. (Any-overlap slivers are not per-plot truth.)
- grands_domaines REMOVED from vision scope: truth-grands parcels excluded from
  the scored set, counted separately (known not-extractable-by-vision).
- coeur_village added as a PREDICTION class (real legend class, absent from the
  6-key truth). Scored two ways: STRICT (coeur counts as error unless truth is
  extension) and WORKING (coeur-predicted parcels excluded pending claude.ai
  adoption of the key).
- secteur hatch-bleed: sweep a min coverage fraction; below threshold -> none.
- Predictions: refined pass (11 tiles, with cov) overrides the original pass.
"""
import json, psycopg
LAMAP="postgresql://postgres.fckdwddgtdbvhzloejni:SUpolkmn098%24@aws-1-eu-central-2.pooler.supabase.com:5432/postgres"
KEYS=['potentiel_densification','secteur_valeur_patrimoniale','extension_village',
      'percee_visuelle','cesure_non_batie']
CLS=KEYS+['coeur_village','none']

idx=json.load(open('/tmp/fable_tiles/index.json'))
pred={}; cov={}
for f in ['g0','g1','g2','g3','g4','g5','g6','g7']:
    for t,labs in json.load(open(f'/tmp/fable_out/{f}.json')).items():
        for lab,cat in labs.items():
            pred[idx[t][lab]] = 'none' if cat=='grands_domaines' else cat
            cov[idx[t][lab]] = 85 if cat!='none' else 0   # first-pass default
for f in ['a','b','c']:
    for t,labs in json.load(open(f'/tmp/fable_out2/{f}.json')).items():
        for lab,d in labs.items():
            pred[idx[t][lab]] = d['cat']; cov[idx[t][lab]] = d['cov']

with psycopg.connect(LAMAP) as cn:
    cur=cn.cursor()
    cur.execute("""SELECT DISTINCT ON (egrid) egrid, category_key, overlap_pct
                   FROM ref.plot_pdcom_urbanisation WHERE commune_name='Cologny'
                   ORDER BY egrid, overlap_m2 DESC NULLS LAST""")
    rows=cur.fetchall()
truth={e:(k if (p or 0)>=50 else 'none') for e,k,p in rows}
grands={e for e,k,p in rows if (p or 0)>=50 and k=='grands_domaines'}
print(f"scored set: {len(pred)-len(grands)} parcels ({len(grands)} truth-grands excluded)")

def score(sec_thr, mode):
    cm={t:{p:0 for p in CLS} for t in CLS}
    ok=tot=0
    for eg,p in pred.items():
        if eg in grands: continue
        t=truth.get(eg,'none')
        pp=p
        if pp=='secteur_valeur_patrimoniale' and cov.get(eg,0)<sec_thr: pp='none'
        if mode=='working' and pp=='coeur_village': continue   # pending key adoption
        if mode=='strict' and pp=='coeur_village': pass        # counts vs truth as-is
        cm[t][pp]+=1; tot+=1; ok+=(t==pp)
    return ok,tot,cm

print("\n=== SECTEUR coverage-threshold sweep (strict mode) ===")
for thr in [0,30,50,70,85]:
    ok,tot,cm=score(thr,'strict')
    n=sum(cm[ 'secteur_valeur_patrimoniale'][c] for c in CLS)
    pn=sum(cm[t]['secteur_valeur_patrimoniale'] for t in CLS)
    tp=cm['secteur_valeur_patrimoniale']['secteur_valeur_patrimoniale']
    print(f"  thr={thr:>2}: overall={100*ok/tot:5.1f}%  secteur recall={100*tp/max(n,1):5.1f}% precision={100*tp/max(pn,1):5.1f}% (pred={pn})")

BEST=70
for mode in ['strict','working']:
    ok,tot,cm=score(BEST,mode)
    print(f"\n=== POST-FIX {mode.upper()} (secteur thr={BEST}): accuracy {100*ok/tot:.1f}% ({ok}/{tot}) ===")
    for c in CLS:
        n=sum(cm[c].values()); r=100*cm[c][c]/n if n else 0
        pn=sum(cm[t][c] for t in CLS); pr=100*cm[c][c]/pn if pn else 0
        print(f"  {c:<28} recall={r:5.1f}% (n={n:>4})  precision={pr:5.1f}% (pred={pn})")
    print("  confusion (rows=truth, cols=pred):")
    print("  truth\\pred   "+" ".join(f"{c[:9]:>9}" for c in CLS))
    for t in CLS:
        if sum(cm[t].values())==0 and t=='coeur_village': continue
        print(f"  {t[:11]:<12}"+" ".join(f"{cm[t][p]:>9}" for p in CLS))

# TRIAGE: emit only high-confidence positives + confident none; else unset.
print("\n=== TRIAGE mode (emit only: potentiel cov>=60, secteur cov>=70, coeur cov>=70, extension cov>=70, none; unset otherwise) ===")
emit=ok2=0; tot2=0; unset=0
cmt={t:{p:0 for p in CLS} for t in CLS}
for eg,p in pred.items():
    if eg in grands: continue
    tot2+=1
    t=truth.get(eg,'none'); c=cov.get(eg,0)
    pp=None
    if p=='none': pp='none'
    elif p=='potentiel_densification' and c>=60: pp=p
    elif p=='secteur_valeur_patrimoniale' and c>=70: pp=p
    elif p in ('coeur_village','extension_village') and c>=70: pp=p
    if pp is None: unset+=1; continue
    if pp=='coeur_village': continue  # pending adoption; not scored
    emit+=1; cmt[t][pp]+=1; ok2+=(t==pp)
print(f"  coverage: {100*(tot2-unset)/tot2:.1f}% of parcels emitted ({unset} unset); scored-emitted accuracy {100*ok2/emit:.1f}%")
for c in KEYS+['none']:
    pn=sum(cmt[t][c] for t in CLS); tp=cmt[c][c]
    if pn: print(f"  {c:<28} precision={100*tp/pn:5.1f}% (emitted={pn})")
json.dump({"pred":pred,"cov":cov}, open('/tmp/fable_postfix_pred.json','w'))
