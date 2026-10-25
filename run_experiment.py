"""Runs every experiment from the report and writes results.json.
Usage: python3 run_experiment.py            (full suite)"""
import json, os, signal, statistics, subprocess, sys, time, urllib.request
import store, mock_site

PORT, BASE = 8099, "http://127.0.0.1:8099"
PY, HERE = sys.executable, os.path.dirname(os.path.abspath(__file__))
SEED = f"{BASE}/catalogue/page-1.html"
EXPECTED_PAGES = mock_site.PAGES + mock_site.N_BOOKS      # 50 catalogue + 1000 books

def site(path):
    return urllib.request.urlopen(BASE + path, timeout=5).read().decode()

def run_crawl(n_workers, tag, kill_at=None, flaky=0.0, lease=10, latency=0.03):
    db = os.path.join(HERE, f"exp_{tag}.db")
    for f in (db, db + "-wal", db + "-shm"):
        if os.path.exists(f): os.remove(f)
    site("/reset"); site(f"/config?latency={latency}&flaky={flaky}")
    coord = subprocess.Popen([PY, "coordinator.py", "--db", db, "--seed", SEED, "--exit-when-done"], cwd=HERE,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    time.sleep(0.5)
    workers = [subprocess.Popen([PY, "worker.py", "--db", db, "--id", f"w{i+1}", "--lease", str(lease)], cwd=HERE)
               for i in range(n_workers)]
    killed = None
    if kill_at:
        time.sleep(kill_at)
        workers[0].send_signal(signal.SIGKILL); killed = "w1"       # hard crash: no cleanup, lease not released
    coord.wait(timeout=400)
    for w in workers: w.wait(timeout=60)
    coord_log = coord.stdout.read()
    conn = store.connect(db)
    c = store.counts(conn)
    t0, t1 = conn.execute("SELECT MIN(first_leased), MAX(done_at) FROM tasks").fetchone()
    pages = conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    wk = conn.execute("SELECT worker_id,pages_done,links_seen,links_new,retries,blocked FROM workers ORDER BY worker_id").fetchall()
    requeues = conn.execute("SELECT COALESCE(SUM(requeues),0) FROM tasks").fetchone()[0]
    task_retries = conn.execute("SELECT COALESCE(SUM(retries),0) FROM tasks").fetchone()[0]
    # correctness: every book record must equal the ground truth the mock site generates from
    bad = 0; books = 0
    for url, data in conn.execute("SELECT url,data FROM pages WHERE kind='book'"):
        d = json.loads(data); e = mock_site.book(d["id"]); books += 1
        if d != e: bad += 1
    st = json.loads(site("/stats"))
    secs = t1 - t0
    return {"workers": n_workers, "crawl_seconds": round(secs, 2), "pages_saved": pages,
            "pages_per_sec": round(pages / secs, 2), "task_status": c, "books_verified": books - bad,
            "books_wrong": bad, "lease_requeues": requeues, "http_retries": task_retries,
            "per_worker_pages": {w[0]: w[1] for w in wk},
            "links_seen": sum(w[2] for w in wk), "links_new": sum(w[3] for w in wk),
            "blocked_by_robots": sum(w[5] for w in wk), "killed_worker": killed,
            "server": st, "coordinator_log": coord_log.strip().splitlines()}

def main():
    out = {"expected_pages": EXPECTED_PAGES, "cpu_cores": os.cpu_count(), "scaling": {}, "started": time.ctime()}
    srv = subprocess.Popen([PY, "mock_site.py", str(PORT)], cwd=HERE, stderr=subprocess.DEVNULL)
    time.sleep(1)
    try:
        for n in (1, 2, 4, 8):
            out["scaling"][n] = [run_crawl(n, f"scale{n}_{r}") for r in range(2)]
            print(f"scaling n={n}:", [r["crawl_seconds"] for r in out["scaling"][n]], flush=True)
            json.dump(out, open("results.json", "w"), indent=1)
        print("fault test ...", flush=True)
        out["fault"] = run_crawl(4, "fault", kill_at=3, lease=2)
        print("flaky test ...", flush=True)
        out["flaky"] = run_crawl(4, "flaky", flaky=0.10)
        # dedup test: re-submit every known URL again -> nothing new may enter the frontier
        conn = store.connect(os.path.join(HERE, "exp_flaky.db"))
        urls = [r[0] for r in conn.execute("SELECT url FROM tasks")]
        again = sum(store.add_url(conn, "job1", u, 0) for u in urls)
        again2 = sum(store.add_url(conn, "job1", u + "#frag", 0) for u in urls[:100])
        out["dedup"] = {"urls_in_frontier": len(urls), "resubmitted": len(urls), "accepted_as_new": again,
                        "fragment_variants_tried": 100, "fragment_variants_accepted": again2}
        json.dump(out, open("results.json", "w"), indent=1)
        print("ALL DONE", flush=True)
    finally:
        srv.kill()

if __name__ == "__main__":
    main()
