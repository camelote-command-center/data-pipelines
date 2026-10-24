"""Parse SHAB commercial-register notice text (French, GE/VD) into company facts and person roles.

Output per notice:
  company: uid, name, seat, address, care_of, event (new | change | deletion), new_name, dissolved, capital
  roles:   list of {name, origin, domicile, role, signature, action: 'in' | 'out'}
  company_links: list of {uid, name, kind}  (company named as partner / shareholder / auditor / acquirer)
Rule-based; unparsed sentences are kept in `rest` so coverage can be measured and the rules improved."""
import re

UID = r'CHE-\d{3}\.\d{3}\.\d{3}'
ROLE_WORDS = [
    ('président du conseil de fondation', 'president'), ('présidente du conseil de fondation', 'president'),
    ("membre président du conseil d'administration", 'president'), ("présidente du conseil d'administration", 'president'),
    ("président du conseil d'administration", 'president'), ('vice-présidente', 'vice_president'), ('vice-président', 'vice_president'),
    ('présidente', 'president'), ('président', 'president'), ("administrateur unique", 'sole_director'), ("administratrice unique", 'sole_director'),
    ("membre du conseil d'administration", 'director'), ('administratrice', 'director'), ('administrateur', 'director'),
    ('membre du conseil de fondation', 'foundation_board'), ('membre du conseil', 'board_member'), ('membre du comité', 'committee_member'),
    ('associée gérante', 'partner_manager'), ('associé gérant', 'partner_manager'), ('associée-gérante', 'partner_manager'), ('associé-gérant', 'partner_manager'),
    ('associée', 'partner'), ('associé', 'partner'), ('gérante', 'manager'), ('gérant', 'manager'),
    ('directrice', 'executive'), ('directeur', 'executive'), ('fondée de pouvoir', 'proxy'), ('fondé de pouvoir', 'proxy'),
    ('liquidatrice', 'liquidator'), ('liquidateur', 'liquidator'), ('secrétaire', 'secretary'), ('trésorière', 'treasurer'), ('trésorier', 'treasurer'),
    ('titulaire', 'owner'), ('mandataire commercial', 'proxy'),
]
SIG = [('signature individuelle', 'individual'), ('signature collective à deux', 'collective_2'), ('signature collective', 'collective'),
       ('procuration collective à deux', 'procuration_2'), ('procuration individuelle', 'procuration'), ('sans signature', 'none')]
# « Lastname Firstname, de Origin, à Domicile » / « de et à Onex » / « d'Italie, à Commugny » / « de France, à Genève »
PERSON = re.compile(r"(?P<name>[A-ZÀ-ÖØ-Þ][\w'’\-\.]+(?:[ \-][A-ZÀ-ÖØ-Þ][\w'’\-\.]+){1,5}),\s+(?:(?:de|d'|du|des)\s*(?:et\s+)?(?P<origin>[^,;]+?)\s*,\s*à\s+(?P<dom>[^,;]+?)|(?:de|d')\s*et\s+à\s+(?P<both>[^,;]+?))\s*(?=,|;|\.|$)")
OUT = re.compile(r"(?P<names>[^.;:]+?)\s+(?:n'est|ne sont)\s+plus\s+(?P<role>[^;.]+)")

def role_of(s):
    s = re.sub(r'\bmembres\b', 'membre', (s or '').lower())
    for w, k in ROLE_WORDS:
        if w in s: return k
    return None

def sig_of(s):
    s = s.lower()
    for w, k in SIG:
        if w in s: return k
    return None

def split_names(s):
    s = re.sub(r'^(?:et|,)\s*', '', s.strip())
    parts = re.split(r',\s*|\s+et\s+', s)
    return [p.strip() for p in parts if re.match(r"^[A-ZÀ-ÖØ-Þ][\w'’\-\.]+(?:[ \-][\w'’\-\.]+){1,5}$", p.strip())]

def parse(text, sub_rubric=None):
    t = re.sub(r'\s+', ' ', text or '').strip()
    out = {'company': {}, 'roles': [], 'company_links': [], 'rest': []}
    m = re.search(UID, t)
    head = t[:m.start()] if m else t[:200]
    c = out['company']
    c['uid'] = m.group(0) if m else None
    hm = re.match(r"(?P<name>.+?), (?:en liquidation, )?à (?P<seat>[^,]+)(?:, (?P<addr>.+?))?, $", head)
    if hm:
        c['name'], c['seat'] = hm.group('name').strip(), hm.group('seat').strip()
        addr = hm.group('addr') or ''
        co = re.search(r'c/o ([^,]+)', addr)
        c['care_of'] = co.group(1).strip() if co else None
        c['address'] = addr.strip() or None
    c['event'] = {'HR01': 'new', 'HR02': 'change', 'HR03': 'deletion'}.get(sub_rubric, 'change')
    nn = re.search(r"(?:Nouvelle raison sociale|Nouveau nom|raison sociale devient)\s*:\s*([^.]+)", t)
    c['new_name'] = nn.group(1).strip() if nn else None
    na = re.search(r'Nouvelle adresse\s*:\s*([^.]+)', t)
    if na:
        c['address'] = na.group(1).strip(); co = re.search(r'c/o ([^,]+)', c['address']); c['care_of'] = co.group(1).strip() if co else c.get('care_of')
    cap = re.search(r"Capital(?:-actions|-social)?\s*:\s*CHF\s*([\d'’ ]+)", t)
    c['capital_chf'] = int(re.sub(r"\D", '', cap.group(1))) if cap else None
    c['dissolved'] = bool(re.search(r'est dissoute|en liquidation', t))
    body = t[m.end():] if m else t
    body = re.sub(r'^\s*\(FOSC du [^)]*\)\.?', '', body)
    LABELS = [('administration', 'director'), ('associé', 'partner'), ('gérant', 'manager'), ('direction', 'executive'), ('liquidat', 'liquidator'),
              ('titulaire', 'owner'), ('conseil de fondation', 'foundation_board'), ('comité', 'committee_member')]
    for sent in re.split(r'(?<=\.)\s+(?=[A-ZÀ-Ö])', body):
        hit = False
        low = sent.lower()
        label = re.match(r"^\s*([^:]{2,60}):", sent)
        label_role = next((k for w, k in LABELS if label and w in label.group(1).lower()), None)
        for om in OUT.finditer(sent):
            role = role_of(om.group('role'))
            for n in split_names(om.group('names')):
                out['roles'].append({'name': n, 'origin': None, 'domicile': None, 'role': role, 'signature': None, 'action': 'out'}); hit = True
        for sm in re.finditer(r"(?:La|Les) signatures? de (?P<names>.+?) (?:est|sont) radiée", sent):
            for n in split_names(sm.group('names')):
                out['roles'].append({'name': n, 'origin': None, 'domicile': None, 'role': None, 'signature': None, 'action': 'out'}); hit = True
        for gm in re.finditer(r"(?:Gérante?s?|Associée?s?(?:[- ]gérante?s?)?)\s*:\s*(?:les? |l')associée?s? (?P<name>[A-ZÀ-ÖØ-Þ][\w'’\-\.]+(?: [A-ZÀ-ÖØ-Þ][\w'’\-\.]+){1,5})", sent):
            out['roles'].append({'name': gm.group('name'), 'origin': None, 'domicile': None, 'role': 'partner_manager', 'signature': sig_of(sent[gm.end():gm.end()+60]), 'action': 'in'}); hit = True
        for nm in re.finditer(r"(?:^|\. |, )(?:L'administrat\w+ |Le directeur |La directrice )?(?P<name>[A-ZÀ-ÖØ-Þ][\w'’\-\.]+(?: [A-ZÀ-ÖØ-Þ][\w'’\-\.]+){1,5}),(?: [^,]*?,)* (?:est nommée?|signe désormais|est désormais) (?P<rest>[^.;]+)", sent):
            if not PERSON.search(sent[nm.start():nm.end()]):
                out['roles'].append({'name': nm.group('name'), 'origin': None, 'domicile': None, 'role': role_of(nm.group('rest')),
                                     'signature': sig_of(nm.group('rest')) or ('individual' if 'individuellement' in nm.group('rest') else None), 'action': 'in'}); hit = True
        for lm in re.finditer(r"Liquidat\w+\s*:\s*(?:les? |l')associée?s? (?P<names>[^,.;]+?(?: et [^,.;]+?)?)(?:,|\.|;|$)", sent):
            for n in split_names(lm.group('names')):
                out['roles'].append({'name': n, 'origin': None, 'domicile': None, 'role': 'liquidator', 'signature': sig_of(sent[lm.end():lm.end()+60]), 'action': 'in'}); hit = True
        cm_ = re.search(r"(?P<sig>(?:Signature|Procuration) (?:individuelle|collective à deux)) (?:a été|est) conférée à (?P<names>[^.]+)", sent)
        if cm_ and not PERSON.search(cm_.group('names')):
            for n in split_names(cm_.group('names')):
                out['roles'].append({'name': n, 'origin': None, 'domicile': None, 'role': 'proxy' if cm_.group('sig').startswith('Procuration') else None,
                                     'signature': sig_of(cm_.group('sig')), 'action': 'in'}); hit = True
        found = []
        for pm in PERSON.finditer(sent):
            tail = sent[pm.end():pm.end() + 160]
            seg = (tail.split(';')[0]).split('. ')[0]
            seg_head = re.split(r',\s+[A-ZÀ-ÖØ-Þ][\w\'’\-]+ [A-ZÀ-ÖØ-Þ]', seg)[0]  # stop at the next person
            found.append({'name': pm.group('name').strip(), 'origin': (pm.group('origin') or pm.group('both') or '').strip() or None,
                          'domicile': (pm.group('dom') or pm.group('both') or '').strip() or None,
                          'role': role_of(seg_head), 'signature': sig_of(seg_head), 'action': 'in', '_end': pm.end()})
        if found:
            after_last = sent[found[-1]['_end']:]
            s_role = role_of(after_last) or label_role or ('partner' if 'parts de chf' in low else None)
            s_sig = sig_of(after_last) or ('individual' if 'signent individuellement' in low else None) or ('none' if 'sans signature' in low else None)
            for f in found:
                f.pop('_end')
                f['role'] = f['role'] or s_role
                f['signature'] = f['signature'] or s_sig
                if not f['role'] and (f['signature'] or '').startswith('procuration'): f['role'] = 'proxy'
                if not f['role'] and 'conférée à' in low: f['role'] = 'proxy' if 'procuration' in low else 'signatory'
                out['roles'].append(f)
            hit = True
        for cm in re.finditer(r"([A-ZÀ-Ö][^,;:()]{1,80}?(?:SA|Sàrl|SARL|AG|GmbH|Holding|Fondation)[^,;:()]{0,40}?)\s*\((" + UID + r")\)", sent):
            kind = 'auditor' if 'révision' in low else ('partner' if re.search(r'associ|parts', low) else 'mentioned')
            out['company_links'].append({'name': cm.group(1).strip(), 'uid': cm.group(2), 'kind': kind}); hit = True
        if not hit and ('signature' in low or re.search(r", de [^,]+, à ", sent)):
            out['rest'].append(sent[:300])
    return out

if __name__ == '__main__':
    import sys, json
    print(json.dumps(parse(sys.stdin.read(), sys.argv[1] if len(sys.argv) > 1 else None), ensure_ascii=False, indent=1))
