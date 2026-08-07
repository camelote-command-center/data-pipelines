# DRAFT — query to SITG re: IDPADR 260629140440

**Status: NOT SENT. For operator review.**

Context for the reviewer, not to be included in the message: this address is
excluded from `gold_ch.v_ge_cad_adresses_full` and its `egid` on
`lamap_db ref.ge_cad_adresses` is deliberately NULL. It stays that way until
SITG answers. See `gold_ch.ge_cad_adresses_exceptions`.

One thing to note before sending: our own pre-incident copy of this row was
destroyed on 2026-08-07, so we cannot tell SITG which value we previously held.
The question below is therefore purely about the source data, which is the
correct scope anyway.

---

**Subject:** ADRESSES — IDPADR 260629140440 associé à deux EGID différents

Bonjour,

Nous exploitons la couche `ADRESSES` du SITG via l'API ArcGIS REST et nous
constatons une incohérence sur laquelle nous souhaiterions votre avis.

L'identifiant **IDPADR 260629140440** (Chemin Plein-Sud 40, Lancy) apparaît sur
**deux entités distinctes**, portant deux EGID différents :

| OBJECTID | EGID | ADRESSE |
|---|---|---|
| 4023 | 295531927 | Chemin Plein-Sud 40 |
| (2e entité) | 295531928 | Chemin Plein-Sud 40 |

Les deux entités partagent le même IDPADR, la même adresse, le même code de
voie et la même commune. Elles diffèrent par l'EGID, ainsi que par les
coordonnées E/N et la géométrie.

Nos questions :

1. **IDPADR est-il censé être unique** dans la couche ADRESSES ? Nous
   l'utilisons comme clé d'appariement, et nous relevons actuellement
   **5 IDPADR dupliqués** sur 54'667 entités.
2. Pour l'IDPADR 260629140440, **quel EGID fait foi** ? S'agit-il de deux
   bâtiments distincts partageant une même adresse postale, ou d'un doublon à
   corriger ?
3. Si une même adresse peut légitimement se rattacher à plusieurs bâtiments,
   quel identifiant recommandez-vous comme clé unique et stable pour la couche
   ADRESSES ?

Nous n'avons volontairement retenu aucune des deux valeurs de notre côté :
rattacher une adresse au mauvais bâtiment nous paraît plus dommageable que de
laisser le lien non renseigné en attendant votre réponse.

Les quatre autres IDPADR dupliqués sont, eux, identiques sur tous les attributs
métier et ne posent pas de problème d'interprétation ; nous les signalons pour
information :
260629124029, 260629124133, 260629145103, 260630135255.

Avec nos remerciements,

---

## Verification query, if SITG asks how we found it

```sql
SELECT idpadr, count(*) AS entities, count(DISTINCT egid) AS distinct_egid,
       string_agg(DISTINCT egid::text, ', ') AS egids
FROM bronze_ch.ge_cad_adresses
GROUP BY idpadr
HAVING count(*) > 1
ORDER BY count(DISTINCT egid) DESC, idpadr;
```

Source: SITG ArcGIS REST FeatureServer, layer `ADRESSES`, ingested
2026-08-06, 54'667 entities.
