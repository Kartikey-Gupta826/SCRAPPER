"""Stateless scraping worker: lease -> robots check -> shared rate limit -> fetch -> parse -> save -> enqueue links -> ack."""
import argparse, os, re, time, urllib.robotparser
from urllib.parse import urljoin, urlsplit
import requests
from bs4 import BeautifulSoup
import store

UA = "DSCC-PBL-Crawler/1.0 (student project)"
RATING = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}

def parse(url, html):
    """Return (kind, data, links). Links are (url, is_pagination) pairs."""
    soup = BeautifulSoup(html, "lxml")
    links = []
    if soup.select_one("article.product_pod"):
        nxt = soup.select_one("li.next a")
        if nxt: links.append(urljoin(url, nxt["href"]))
        links += [urljoin(url, a["href"]) for a in soup.select("article.product_pod h3 a")]
        links += [urljoin(url, a["href"]) for a in soup.select("footer a")]
        return "catalog", {"title": soup.h1.text}, links
    title = soup.h1.text
    rating = RATING[[c for c in soup.select_one("p.star-rating")["class"] if c in RATING][0]]
    data = {"id": int(re.search(r"/book/(\d+)", url).group(1)), "title": title,
            "price": float(soup.select_one("p.price_color").text.strip().lstrip("£")),
            "rating": rating,
            "in_stock": int(re.search(r"\((\d+) available\)", soup.select_one("p.availability").text).group(1)),
            "upc": soup.select_one("table td").text}
    links = [urljoin(url, a["href"]) for a in soup.select("a.related")]
    return "book", data, links

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True); ap.add_argument("--id", default=f"w{os.getpid()}")
    ap.add_argument("--lease", type=float, default=10); ap.add_argument("--timeout", type=float, default=5)
    ap.add_argument("--min-interval", type=float, default=0.002, help="min seconds between requests to one domain, ALL workers combined")
    ap.add_argument("--max-retries", type=int, default=3)
    a = ap.parse_args()
    conn = store.connect(a.db)
    now = time.time()
    conn.execute("INSERT OR REPLACE INTO workers(worker_id,started_at,last_heartbeat) VALUES(?,?,?)", (a.id, now, now))
    sess = requests.Session(); sess.headers["User-Agent"] = UA
    robots = {}

    def allowed(url):
        host = urlsplit(url).netloc
        if host not in robots:
            rp = urllib.robotparser.RobotFileParser()
            try:
                rp.parse(sess.get(f"http://{host}/robots.txt", timeout=a.timeout).text.splitlines())
            except requests.RequestException:
                rp.parse([])
            robots[host] = rp
        return robots[host].can_fetch(UA, url)

    def bump(col, n=1):
        conn.execute(f"UPDATE workers SET {col}={col}+?, last_heartbeat=? WHERE worker_id=?", (n, time.time(), a.id))

    while True:
        conn.execute("UPDATE workers SET last_heartbeat=? WHERE worker_id=?", (time.time(), a.id))  # heartbeat
        task = store.lease_task(conn, a.id, a.lease)
        if not task:
            if store.is_complete(conn): break
            time.sleep(0.05); continue
        url, job, depth, _ = task
        if not allowed(url):
            store.ack(conn, url, "skipped"); bump("blocked"); continue
        time.sleep(store.reserve_slot(conn, urlsplit(url).netloc, a.min_interval))
        try:
            r = sess.get(url, timeout=a.timeout)
        except requests.RequestException as e:
            store.fail(conn, url, f"net:{type(e).__name__}", a.max_retries); bump("retries"); continue
        if r.status_code == 429 or r.status_code >= 500:
            store.fail(conn, url, f"http {r.status_code}", a.max_retries); bump("retries"); continue
        if r.status_code >= 400:
            store.ack(conn, url, "failed"); continue
        try:
            kind, data, links = parse(url, r.text)
        except Exception as e:                       # malformed page: log, mark failed, keep going
            store.fail(conn, url, f"parse:{type(e).__name__}", 1); continue
        store.save_page(conn, job, url, kind, data, r.status_code, a.id)
        new = sum(store.add_url(conn, job, l, depth + 1) for l in links)
        bump("links_seen", len(links)); bump("links_new", new)
        store.ack(conn, url); bump("pages_done")

if __name__ == "__main__":
    main()
