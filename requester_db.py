"""
REQUESTER - Database Layer
===========================
SQLite persistence for posts, demands, run history, and user votes.
Zero install required — Python's sqlite3 is built in.
"""

import sqlite3, json, re, os
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "requester.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at          TEXT    NOT NULL,
    subreddits      TEXT,
    apps            TEXT,
    posts_scanned   INTEGER DEFAULT 0,
    requests_found  INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS posts (
    source_id   TEXT PRIMARY KEY,
    source      TEXT NOT NULL,
    subreddit   TEXT,
    app_name    TEXT,
    title       TEXT,
    body        TEXT,
    score       INTEGER DEFAULT 0,
    rating      INTEGER,
    permalink   TEXT,
    created_utc REAL,
    first_seen  TEXT NOT NULL,
    last_seen   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS demands (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    slug        TEXT UNIQUE NOT NULL,
    subject     TEXT NOT NULL,
    action      TEXT NOT NULL,
    category    TEXT,
    summary     TEXT,
    first_seen  TEXT NOT NULL,
    last_seen   TEXT NOT NULL,
    times_seen  INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS demand_posts (
    demand_id   INTEGER NOT NULL REFERENCES demands(id),
    source_id   TEXT    NOT NULL REFERENCES posts(source_id),
    PRIMARY KEY (demand_id, source_id)
);
CREATE TABLE IF NOT EXISTS snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      INTEGER NOT NULL REFERENCES runs(id),
    demand_id   INTEGER NOT NULL REFERENCES demands(id),
    gravity     REAL    DEFAULT 0,
    post_count  INTEGER DEFAULT 0,
    rank        INTEGER,
    recorded_at TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS votes (
    demand_slug     TEXT NOT NULL,
    google_user_id  TEXT NOT NULL,
    voted_at        TEXT NOT NULL,
    PRIMARY KEY (demand_slug, google_user_id)
);
CREATE INDEX IF NOT EXISTS idx_snapshots_demand ON snapshots(demand_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_run    ON snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_dp_demand        ON demand_posts(demand_id);
CREATE INDEX IF NOT EXISTS idx_votes_slug       ON votes(demand_slug);
"""

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        # Migration: drop legacy demand_status table if it exists
        conn.execute("DROP TABLE IF EXISTS demand_status")
    print(f"  ✓ Database ready: {DB_PATH}")

def _slugify(text):
    s = text.lower().strip()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", "_", s)
    return s[:120]

def _now():
    return datetime.now(timezone.utc).isoformat()

def save_run(output: dict) -> int:
    init_db()
    now         = _now()
    subreddits  = output.get("subreddits", [])
    apps        = output.get("appstore_apps", []) + output.get("googleplay_apps", [])
    demands     = output.get("leaderboard", [])
    post_lookup = output.get("post_lookup", {})

    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO runs (run_at, subreddits, apps, posts_scanned, requests_found) VALUES (?,?,?,?,?)",
            (now, json.dumps(subreddits), json.dumps(apps),
             output.get("posts_scanned", 0), output.get("requests_found", 0))
        )
        run_id = cur.lastrowid

        print(f"\n  Saving {len(post_lookup)} posts to DB...")
        for pid, p in post_lookup.items():
            body = (p.get("selftext") or p.get("body") or "")[:1000]
            conn.execute("""
                INSERT INTO posts (source_id, source, subreddit, app_name, title, body,
                     score, rating, permalink, created_utc, first_seen, last_seen)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(source_id) DO UPDATE SET
                    last_seen = excluded.last_seen,
                    score     = excluded.score,
                    body      = CASE WHEN excluded.body != '' THEN excluded.body ELSE body END
            """, (pid, p.get("_source","reddit"), p.get("_subreddit"), p.get("_app_name"),
                  p.get("title","")[:500], body, p.get("score",0), p.get("_rating"),
                  p.get("permalink",""), p.get("created_utc"), now, now))

        print(f"  Saving {len(demands)} demands to DB...")
        for rank, demand in enumerate(demands, 1):
            slug = _slugify(demand.get("action","")) or _slugify(demand.get("subject","") + "_" + demand.get("category",""))
            row = conn.execute("SELECT id FROM demands WHERE slug=?", (slug,)).fetchone()
            if row:
                demand_id = row["id"]
                conn.execute("UPDATE demands SET last_seen=?, times_seen=times_seen+1, summary=?, category=? WHERE id=?",
                             (now, demand.get("summary"), demand.get("category"), demand_id))
            else:
                c = conn.execute("INSERT INTO demands (slug,subject,action,category,summary,first_seen,last_seen,times_seen) VALUES (?,?,?,?,?,?,?,1)",
                                 (slug, demand.get("subject",""), demand.get("action",""), demand.get("category",""), demand.get("summary",""), now, now))
                demand_id = c.lastrowid
            for pid in demand.get("post_ids", []):
                conn.execute("INSERT OR IGNORE INTO demand_posts (demand_id,source_id) VALUES (?,?)", (demand_id, pid))
            conn.execute("INSERT INTO snapshots (run_id,demand_id,gravity,post_count,rank,recorded_at) VALUES (?,?,?,?,?,?)",
                         (run_id, demand_id, demand.get("gravity_score",0), len(demand.get("post_ids",[])), rank, now))

    print(f"  ✓ Run #{run_id} saved to requester.db")
    return run_id

def get_latest_leaderboard(limit=100):
    with get_conn() as conn:
        run = conn.execute("SELECT id FROM runs ORDER BY id DESC LIMIT 1").fetchone()
        if not run: return []
        rows = conn.execute("""
            SELECT s.rank, s.gravity, s.post_count,
                   d.id as demand_id, d.slug, d.subject, d.action, d.category,
                   d.summary, d.first_seen, d.last_seen, d.times_seen
            FROM snapshots s
            JOIN demands d ON d.id=s.demand_id
            WHERE s.run_id=? ORDER BY s.rank LIMIT ?
        """, (run["id"], limit)).fetchall()
        results = []
        for r in rows:
            item = dict(r)
            item["post_ids"] = [p["source_id"] for p in conn.execute(
                "SELECT source_id FROM demand_posts WHERE demand_id=?", (r["demand_id"],)).fetchall()]
            results.append(item)
        return results

# ── Votes ──────────────────────────────────────────────────────────────────────

def cast_vote(demand_slug: str, google_user_id: str) -> bool:
    """Record a vote. Returns True if this was a new vote, False if already existed."""
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT 1 FROM votes WHERE demand_slug=? AND google_user_id=?",
            (demand_slug, google_user_id)
        ).fetchone()
        if existing:
            return False
        conn.execute(
            "INSERT INTO votes (demand_slug, google_user_id, voted_at) VALUES (?,?,?)",
            (demand_slug, google_user_id, _now())
        )
        return True

def retract_vote(demand_slug: str, google_user_id: str) -> bool:
    """Remove a vote. Returns True if a vote was removed, False if none existed."""
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT 1 FROM votes WHERE demand_slug=? AND google_user_id=?",
            (demand_slug, google_user_id)
        ).fetchone()
        if not existing:
            return False
        conn.execute(
            "DELETE FROM votes WHERE demand_slug=? AND google_user_id=?",
            (demand_slug, google_user_id)
        )
        return True

def get_vote_counts(slugs: list) -> dict:
    """Return {slug: count} for the given demand slugs. Single query."""
    if not slugs:
        return {}
    with get_conn() as conn:
        ph = ",".join("?" * len(slugs))
        rows = conn.execute(
            f"SELECT demand_slug, COUNT(*) as cnt FROM votes WHERE demand_slug IN ({ph}) GROUP BY demand_slug",
            slugs
        ).fetchall()
        counts = {r["demand_slug"]: r["cnt"] for r in rows}
        # Fill zeros for slugs with no votes
        return {slug: counts.get(slug, 0) for slug in slugs}

def get_user_votes(google_user_id: str) -> set:
    """Return set of demand_slugs the user has voted for."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT demand_slug FROM votes WHERE google_user_id=?",
            (google_user_id,)
        ).fetchall()
        return {r["demand_slug"] for r in rows}

# ── Misc ───────────────────────────────────────────────────────────────────────

def get_run_history(limit=10):
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT id,run_at,subreddits,apps,posts_scanned,requests_found FROM runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]

def get_post_details(source_ids: list):
    if not source_ids: return {}
    with get_conn() as conn:
        ph = ",".join("?"*len(source_ids))
        return {r["source_id"]: dict(r) for r in conn.execute(f"SELECT * FROM posts WHERE source_id IN ({ph})", source_ids).fetchall()}

def get_stats():
    with get_conn() as conn:
        lr = conn.execute("SELECT run_at FROM runs ORDER BY id DESC LIMIT 1").fetchone()
        return {
            "total_runs":    conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0],
            "total_posts":   conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0],
            "total_demands": conn.execute("SELECT COUNT(*) FROM demands").fetchone()[0],
            "last_run_at":   lr["run_at"] if lr else None,
        }

if __name__ == "__main__":
    init_db()
    print(get_stats())
