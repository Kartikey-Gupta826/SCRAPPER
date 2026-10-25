"""Local practice site that mimics books.toscrape.com (50 catalogue pages x 20 books = 1000 books).
Used because the experiments must be repeatable and must not hammer a real website.
Knobs (changeable at runtime via /config): latency per request, fraction of random 503 errors."""
import json, random, threading, time, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qs

PAGES, PER_PAGE = 50, 20
N_BOOKS = PAGES * PER_PAGE
ADJ = ["Silent", "Hidden", "Last", "Golden", "Broken", "Quiet", "Distant", "Burning", "Hollow", "Endless"]
NOUN = ["River", "Garden", "Empire", "Letter", "Harbor", "Winter", "Machine", "Orchard", "Signal", "Archive"]

def book(i):
    r = random.Random(i)
    return {"id": i, "title": f"Book {i}: {r.choice(ADJ)} {r.choice(NOUN)}", "price": round(r.uniform(10, 60), 2),
            "rating": r.randint(1, 5), "in_stock": r.randint(0, 30), "upc": f"{r.getrandbits(48):012x}"}

WORDS = ["One", "Two", "Three", "Four", "Five"]
def catalog_html(k):
    items = "".join(f'<article class="product_pod"><h3><a href="/book/{i}.html">Book {i}</a></h3></article>'
                    for i in range((k - 1) * PER_PAGE + 1, k * PER_PAGE + 1))
    nxt = f'<li class="next"><a href="/catalogue/page-{k+1}.html">next</a></li>' if k < PAGES else ""
    return f'<html><body><h1>Catalogue page {k}</h1>{items}<ul class="pager">{nxt}</ul>' \
           f'<footer><a href="/admin/panel.html">admin</a></footer></body></html>'

def book_html(i):
    b = book(i)
    rel = [(i * 7) % N_BOOKS + 1, (i * 13) % N_BOOKS + 1]
    rl = "".join(f'<a class="related" href="/book/{j}.html">related {j}</a>' for j in rel)
    return (f'<html><body><h1>{b["title"]}</h1><p class="price_color">£{b["price"]:.2f}</p>'
            f'<p class="instock availability">In stock ({b["in_stock"]} available)</p>'
            f'<p class="star-rating {WORDS[b["rating"]-1]}"></p>'
            f'<table><tr><th>UPC</th><td>{b["upc"]}</td></tr></table>{rl}'
            f'<a href="/catalogue/page-1.html">back</a></body></html>')

LOCK = threading.Lock()
STATE = {"latency": 0.03, "flaky": 0.0, "hits": {}, "ok": {}, "status": {}, "admin_hits": 0, "total": 0}
RNG = random.Random(42)

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def send(self, code, body, ctype="text/html; charset=utf-8"):
        b = body.encode()
        self.send_response(code); self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        u = urlsplit(self.path); path = u.path
        if path == "/stats":
            with LOCK:
                ok = STATE["ok"]
                out = {"total_requests": STATE["total"], "unique_urls_ok": len(ok),
                       "duplicate_ok_fetches": sum(v - 1 for v in ok.values() if v > 1),
                       "status_counts": STATE["status"], "admin_hits": STATE["admin_hits"]}
            return self.send(200, json.dumps(out), "application/json")
        if path == "/reset":
            with LOCK:
                STATE.update(hits={}, ok={}, status={}, admin_hits=0, total=0)
            return self.send(200, "ok")
        if path == "/config":
            q = parse_qs(u.query)
            with LOCK:
                if "latency" in q: STATE["latency"] = float(q["latency"][0])
                if "flaky" in q: STATE["flaky"] = float(q["flaky"][0])
            return self.send(200, "ok")
        if path == "/robots.txt":
            return self.send(200, "User-agent: *\nDisallow: /admin/\n", "text/plain")
        time.sleep(STATE["latency"])
        code, body = 404, "not found"
        with LOCK:
            STATE["total"] += 1
            if path.startswith("/admin"): STATE["admin_hits"] += 1
            flaky = path.startswith("/book/") and RNG.random() < STATE["flaky"]
        if flaky:
            code, body = 503, "temporarily unavailable"
        elif path.startswith("/catalogue/page-"):
            k = int(path.split("page-")[1].split(".")[0])
            if 1 <= k <= PAGES: code, body = 200, catalog_html(k)
        elif path.startswith("/book/"):
            i = int(path.split("/")[2].split(".")[0])
            if 1 <= i <= N_BOOKS: code, body = 200, book_html(i)
        with LOCK:
            STATE["status"][str(code)] = STATE["status"].get(str(code), 0) + 1
            if code == 200: STATE["ok"][path] = STATE["ok"].get(path, 0) + 1
        self.send(code, body)

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8099
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
