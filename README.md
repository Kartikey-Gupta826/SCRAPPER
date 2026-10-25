# Distributed Web Scraper (DSCC project)
Coordinator + N stateless workers sharing a SQLite (WAL) frontier. Pull-based tasks, leases, atomic dedup,
shared per-domain rate limiter, robots.txt, retries with backoff, idempotent writes.

Files: store.py (shared state), worker.py, coordinator.py, mock_site.py (test target), run_experiment.py (all experiments).

Run by hand (4 terminals):
  python3 mock_site.py 8099
  python3 coordinator.py --db crawl.db --seed http://127.0.0.1:8099/catalogue/page-1.html --port 8000 --exit-when-done
  python3 worker.py --db crawl.db --id w1      # start as many as you like, each in its own terminal
  curl localhost:8000                          # live status
Full benchmark: python3 run_experiment.py   (about 8 minutes, writes results.json)
