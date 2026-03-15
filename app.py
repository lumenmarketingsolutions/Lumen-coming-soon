from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import os, sqlite3, datetime

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "lumen-wl-key-2026")

ADMIN_PIN = "112501"

DB_PATH = os.path.join(os.path.dirname(__file__), "waitlist.db")

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS waitlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()

init_db()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/join", methods=["POST"])
def join_waitlist():
    data = request.get_json()
    email = (data.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Valid email required"})
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute(
            "INSERT INTO waitlist (email, created_at) VALUES (?, ?)",
            (email, datetime.datetime.utcnow().isoformat()),
        )
        con.commit()
        con.close()
        return jsonify({"ok": True})
    except sqlite3.IntegrityError:
        return jsonify({"ok": True})  # already on list, don't reveal

@app.route("/waitlist", methods=["GET", "POST"])
def waitlist_admin():
    if request.method == "POST":
        pin = (request.form.get("pin") or "").strip()
        if pin == ADMIN_PIN:
            session["wl_auth"] = True
            return redirect(url_for("waitlist_admin"))
        return render_template("waitlist.html", error=True, authed=False, entries=[])
    if not session.get("wl_auth"):
        return render_template("waitlist.html", authed=False, entries=[], error=False)
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("SELECT email, created_at FROM waitlist ORDER BY id DESC").fetchall()
    con.close()
    entries = [{"email": r[0], "date": r[1][:10]} for r in rows]
    return render_template("waitlist.html", authed=True, entries=entries, error=False)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
