"""Shared state layer: URL frontier (task queue), dedup, leases, rate limiter, results.
SQLite in WAL mode is the shared store that every process (coordinator + workers) talks to.
Every state change that must be atomic runs inside BEGIN IMMEDIATE ... COMMIT."""
import sqlite3, time, json
from contextlib import contextmanager
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs(job_id TEXT PRIMARY KEY, seeds TEXT, status TEXT, created_at REAL, finished_at REAL);
CREATE TABLE IF NOT EXISTS tasks(
  url TEXT PRIMARY KEY,            -- PRIMARY KEY = the distributed "seen" set (dedup)
  job_id TEXT, depth INTEGER, status TEXT,   -- pending | leased | done | failed | skipped
  lease_owner TEXT, lease_expiry REAL, retries INTEGER DEFAULT 0,
  available_at REAL DEFAULT 0, first_leased REAL, done_at REAL,
  requeues INTEGER DEFAULT 0, error TEXT);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status, available_at, depth);
CREATE TABLE IF NOT EXISTS pages(
  url TEXT PRIMARY KEY, job_id TEXT, kind TEXT, data TEXT,
  http_status INTEGER, fetched_at REAL, worker_id TEXT);
CREATE TABLE IF NOT EXISTS workers(
  worker_id TEXT PRIMARY KEY, started_at REAL, last_heartbeat REAL,
  pages_done INTEGER DEFAULT 0, links_seen INTEGER DEFAULT 0, links_new INTEGER DEFAULT 0,
  retries INTEGER DEFAULT 0, blocked INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS domain_limit(domain TEXT PRIMARY KEY, next_allowed REAL);
"""

def connect(path):
    c = sqlite3.connect(path, timeout=60, isolation_level=None)   # autocommit; we manage txns
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute("PRAGMA busy_timeout=60000")
    return c

def init_db(path):
    c = connect(path)
    c.executescript(SCHEMA)
    return c

@contextmanager
def immediate(conn):
    conn.execute("BEGIN IMMEDIATE")          # takes the write lock up-front -> no lost updates
    try:
        yield
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise

def normalize(url):
    """Canonical form so the same page is never queued twice under different spellings."""
    p = urlsplit(url)
    q = urlencode(sorted(parse_qsl(p.query)))
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path or "/", q, ""))  # fragment dropped

def add_url(conn, job_id, url, depth):
    """Atomic dedup: INSERT OR IGNORE succeeds only for the first caller. Returns True if new."""
    cur = conn.execute("INSERT OR IGNORE INTO tasks(url,job_id,depth,status) VALUES(?,?,?, 'pending')",
                       (normalize(url), job_id, depth))
    return cur.rowcount == 1

def lease_task(conn, worker_id, lease_s):
    """Pull model: a worker atomically claims the next pending URL and gets a time-limited lease."""
    now = time.time()
    with immediate(conn):
        row = conn.execute("""SELECT url, job_id, depth, retries FROM tasks
                              WHERE status='pending' AND available_at<=?
                              ORDER BY depth, rowid LIMIT 1""", (now,)).fetchone()
        if row:
            conn.execute("""UPDATE tasks SET status='leased', lease_owner=?, lease_expiry=?,
                            first_leased=COALESCE(first_leased, ?) WHERE url=?""",
                         (worker_id, now + lease_s, now, row[0]))
    return row

def ack(conn, url, status="done"):
    conn.execute("UPDATE tasks SET status=?, done_at=?, lease_owner=NULL WHERE url=?", (status, time.time(), url))

def fail(conn, url, err, max_retries=3):
    """Retry with exponential backoff; give up after max_retries."""
    with immediate(conn):
        r = conn.execute("SELECT retries FROM tasks WHERE url=?", (url,)).fetchone()[0] + 1
        if r >= max_retries:
            conn.execute("UPDATE tasks SET status='failed', retries=?, error=?, done_at=?, lease_owner=NULL WHERE url=?",
                         (r, err, time.time(), url))
        else:
            conn.execute("""UPDATE tasks SET status='pending', retries=?, error=?, lease_owner=NULL,
                            available_at=? WHERE url=?""", (r, err, time.time() + 0.1 * 2 ** r, url))

def reserve_slot(conn, domain, min_interval):
    """Shared per-domain rate limiter (token-bucket style). All workers book the next free time
    slot for a domain, so the limit holds across every node, not per node. Returns seconds to wait."""
    now = time.time()
    with immediate(conn):
        row = conn.execute("SELECT next_allowed FROM domain_limit WHERE domain=?", (domain,)).fetchone()
        slot = max(now, row[0]) if row else now
        conn.execute("INSERT OR REPLACE INTO domain_limit(domain,next_allowed) VALUES(?,?)", (domain, slot + min_interval))
    return slot - now

def save_page(conn, job_id, url, kind, data, http_status, worker_id):
    # idempotent write keyed by URL -> a repeated (at-least-once) delivery just overwrites
    conn.execute("INSERT OR REPLACE INTO pages VALUES(?,?,?,?,?,?,?)",
                 (normalize(url), job_id, kind, json.dumps(data), http_status, time.time(), worker_id))

def reap_expired(conn):
    """Coordinator duty: leases that expired (worker died / hung) go back to the queue."""
    with immediate(conn):
        cur = conn.execute("""UPDATE tasks SET status='pending', lease_owner=NULL, requeues=requeues+1
                              WHERE status='leased' AND lease_expiry<?""", (time.time(),))
    return cur.rowcount

def counts(conn):
    d = dict(conn.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status").fetchall())
    return {k: d.get(k, 0) for k in ("pending", "leased", "done", "failed", "skipped")}

def is_complete(conn):
    c = counts(conn)
    return c["pending"] == 0 and c["leased"] == 0 and sum(c.values()) > 0
