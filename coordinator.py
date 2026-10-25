"""Coordinator: seeds the job, runs the lease reaper, detects completion, serves a status API."""
import argparse, json, threading, time
from http.server import BaseHTTPRequestHandler, HTTPServer
import store

def status(conn):
    c = store.counts(conn)
    alive = conn.execute("SELECT COUNT(*) FROM workers WHERE last_heartbeat>?", (time.time() - 5,)).fetchone()[0]
    pages = conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    requeues = conn.execute("SELECT COALESCE(SUM(requeues),0) FROM tasks").fetchone()[0]
    return {"tasks": c, "pages_saved": pages, "workers_alive": alive, "lease_requeues": requeues}

def serve(db, port):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a): pass
        def do_GET(self):
            body = json.dumps(status(store.connect(db))).encode()
            self.send_response(200); self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    threading.Thread(target=HTTPServer(("127.0.0.1", port), H).serve_forever, daemon=True).start()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True); ap.add_argument("--job", default="job1")
    ap.add_argument("--seed", action="append", default=[]); ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--exit-when-done", action="store_true")
    a = ap.parse_args()
    conn = store.init_db(a.db)
    if a.seed:
        conn.execute("INSERT OR IGNORE INTO jobs VALUES(?,?,?,?,NULL)", (a.job, ",".join(a.seed), "running", time.time()))
        for s in a.seed: store.add_url(conn, a.job, s, 0)
        print(f"[coordinator] job {a.job} seeded with {len(a.seed)} URL(s)", flush=True)
    if a.port: serve(a.db, a.port)
    while True:
        n = store.reap_expired(conn)                  # failure recovery
        if n: print(f"[coordinator] re-queued {n} task(s) with expired lease", flush=True)
        if store.is_complete(conn):
            conn.execute("UPDATE jobs SET status='complete', finished_at=? WHERE job_id=? AND finished_at IS NULL", (time.time(), a.job))
            print("[coordinator] job complete:", json.dumps(status(conn)), flush=True)
            if a.exit_when_done: break
        time.sleep(0.25)

if __name__ == "__main__":
    main()
