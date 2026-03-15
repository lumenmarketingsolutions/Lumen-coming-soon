from flask import Flask, render_template, request, jsonify
import os, sqlite3, datetime

app = Flask(__name__)

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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
