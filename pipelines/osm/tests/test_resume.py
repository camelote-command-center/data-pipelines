"""Offline tests for the OSM importer's resume and time-budget behaviour.

No network and no database: Overpass and PostgREST are replaced by fakes, so these tests can run
anywhere. They cover the two failures that cost a real 3-hour run — losing everything when the job
cap hits, and losing a large canton that cannot finish inside the cap on its own.

Run: python3 pipelines/osm/tests/test_resume.py
"""
import importlib.util
import os
import pathlib
import sys

MOD = pathlib.Path(__file__).resolve().parents[1] / "import.py"
os.environ.setdefault("RE_LLM_SUPABASE_URL", "https://example.test")
os.environ.setdefault("RE_LLM_SUPABASE_SERVICE_ROLE_KEY", "test-key")

spec = importlib.util.spec_from_file_location("osm_import", MOD)
osm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(osm)


class Fake:
    """One canton of N communes, an in-memory table and an in-memory state row."""

    def __init__(self, communes=25, per_commune=3, commune_seconds=0.0):
        self.communes = [{"name": f"C{i:03d}", "area_id": 3600000 + i} for i in range(communes)]
        self.per_commune = per_commune
        self.commune_seconds = commune_seconds
        self.rows = {}          # osm_id -> record   (the bronze table)
        self.state = {}         # canton -> state row
        self.clock = [0.0]
        self.flushes = []

    # ── Overpass ──
    def get_canton_communes(self, iso_code):
        return list(self.communes)

    def fetch_commune_features(self, area_id, tag_union):
        base = area_id * 100
        return [{"type": "node", "id": base + k, "lat": 46.5, "lon": 6.5,
                 "tags": {"amenity": "school", "name": f"n{base + k}"}} for k in range(self.per_commune)]

    # ── PostgREST ──
    def upsert_records(self, url, key, schema, records):
        for r in records:
            self.rows[r["osm_id"]] = r
        self.flushes.append(len(records))
        return len(records)

    def load_canton_state(self, url, key, schema):
        return {k: dict(v) for k, v in self.state.items()}

    def save_canton_state(self, url, key, schema, canton, iso, communes, records, done_communes, complete):
        self.state[canton] = {"canton": canton, "iso_code": iso, "communes": communes,
                              "records": records, "done_communes": sorted(done_communes),
                              "complete": complete,
                              "completed_at": osm.datetime.now(osm.timezone.utc) if complete else
                                              (self.state.get(canton) or {}).get("completed_at")}

    # ── clock: each commune costs commune_seconds, so a budget can be reached deterministically ──
    def time(self):
        return self.clock[0]

    def sleep(self, _s):
        self.clock[0] += self.commune_seconds


def install(fake, monkey_cantons=(("NE", "CH-NE"),)):
    osm.CANTONS = list(monkey_cantons)
    osm.get_canton_communes = fake.get_canton_communes
    osm.fetch_commune_features = fake.fetch_commune_features
    osm.upsert_records = fake.upsert_records
    osm.load_canton_state = fake.load_canton_state
    osm.save_canton_state = fake.save_canton_state
    osm.get_row_count = lambda *a, **k: len(fake.rows)
    osm.probe_canton_column = lambda *a, **k: True
    osm.update_dataset_meta = lambda *a, **k: None
    osm.build_tag_union = lambda: "node[amenity]"
    osm.time.time = fake.time
    osm.time.sleep = fake.sleep


def run(argv):
    sys.argv = ["import.py"] + argv
    out = []
    real_print = print
    osm.print = lambda *a, **k: out.append(" ".join(str(x) for x in a))
    try:
        osm.main()
    finally:
        osm.print = real_print
    return "\n".join(out)


def test_full_canton_completes_and_is_recorded():
    f = Fake(communes=25, per_commune=4)
    install(f)
    log = run(["--time-budget-minutes", "999"])
    assert len(f.rows) == 100, len(f.rows)
    st = f.state["NE"]
    assert st["complete"] is True, st
    assert len(st["done_communes"]) == 25
    assert "OSM_CYCLE_COMPLETE=true" in log
    # flushed every 10 communes plus the tail, never in one lump at the end
    assert f.flushes == [40, 40, 20], f.flushes
    print("  ok  a finished canton is written in flushes and recorded complete")


def test_budget_inside_a_canton_keeps_what_it_fetched():
    # 60 s per commune, 10 min budget -> the budget is spent during the canton, not between cantons
    f = Fake(communes=40, per_commune=5, commune_seconds=60)
    install(f)
    log = run(["--time-budget-minutes", "10"])
    st = f.state["NE"]
    assert st["complete"] is False, st
    assert 0 < len(st["done_communes"]) < 40, st["done_communes"]
    assert len(f.rows) == len(st["done_communes"]) * 5 == st["records"]
    assert "OSM_CYCLE_COMPLETE=false" in log
    assert "Time budget reached inside NE" in log
    print(f"  ok  a canton stopped mid-way kept {len(f.rows)} records "
          f"and {len(st['done_communes'])} communes")
    return f


def test_next_run_resumes_where_it_stopped():
    f = test_budget_inside_a_canton_keeps_what_it_fetched()
    stopped_at = len(f.state["NE"]["done_communes"])
    first_rows = len(f.rows)
    f.clock[0] = 0.0          # a new run, a fresh budget
    f.flushes.clear()
    log = run(["--time-budget-minutes", "999"])
    st = f.state["NE"]
    assert "resume at" in log, log[:400]
    assert st["complete"] is True, st
    assert len(st["done_communes"]) == 40
    assert len(f.rows) == 200, len(f.rows)          # 40 communes x 5, nothing lost or duplicated
    assert sum(f.flushes) == 200 - first_rows, f.flushes   # only the missing communes were fetched
    assert "OSM_CYCLE_COMPLETE=true" in log
    print(f"  ok  the next run resumed past {stopped_at} communes and refetched nothing")


def test_fresh_canton_is_skipped_and_freshness_not_restamped():
    f = Fake(communes=5, per_commune=2)
    install(f)
    run(["--time-budget-minutes", "999"])
    f.flushes.clear()
    log = run(["--time-budget-minutes", "999"])
    assert "Nothing due" in log, log[-400:]
    assert "OSM_CYCLE_COMPLETE=skipped" in log
    assert f.flushes == [], f.flushes
    print("  ok  a fresh canton is skipped and does not restamp freshness")


def test_all_flag_refetches_everything():
    f = Fake(communes=5, per_commune=2)
    install(f)
    run(["--time-budget-minutes", "999"])
    f.flushes.clear()
    log = run(["--time-budget-minutes", "999", "--all"])
    assert sum(f.flushes) == 10, f.flushes
    assert "OSM_CYCLE_COMPLETE=true" in log
    print("  ok  --all ignores recorded progress")


if __name__ == "__main__":
    tests = [test_full_canton_completes_and_is_recorded,
             test_budget_inside_a_canton_keeps_what_it_fetched,
             test_next_run_resumes_where_it_stopped,
             test_fresh_canton_is_skipped_and_freshness_not_restamped,
             test_all_flag_refetches_everything]
    for t in tests:
        t()
    print(f"\n{len(tests)} tests passed")
