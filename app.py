import os
import sqlite3
from datetime import datetime, timedelta
from functools import wraps

import bcrypt
import jwt
from flask import Flask, g, jsonify, redirect, render_template, request, send_from_directory

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "attendance.db")

JWT_SECRET = "change-this-secret"
JWT_ALGORITHM = "HS256"
QR_TEXT = "KUMAMOTO_HIGO"

app = Flask(__name__, template_folder="templates")


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def ensure_column(db, table_name, column_name, column_type_sql):
    cols = db.execute(f"PRAGMA table_info({table_name})").fetchall()
    col_names = [c["name"] for c in cols]
    if column_name not in col_names:
        db.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type_sql}")


def init_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    cur.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'employee',
            manager_id INTEGER,
            executive_id INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS attendance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            work_date TEXT NOT NULL,
            type TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            approved INTEGER DEFAULT 0,
            approved_by INTEGER,
            approved_at DATETIME
        );

        CREATE TABLE IF NOT EXISTS daily_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            work_date TEXT NOT NULL,
            work_type TEXT,
            clock_in TEXT,
            break_start TEXT,
            break_end TEXT,
            clock_out TEXT,
            overtime_requested_at TEXT,
            overtime_planned_end TEXT,
            overtime_reason TEXT,
            has_help INTEGER DEFAULT 0,
            help_department TEXT,
            help_time TEXT,
            remarks TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, work_date)
        );
        """
    )

    ensure_column(db, "users", "manager_id", "INTEGER")
    ensure_column(db, "users", "executive_id", "INTEGER")
    ensure_column(db, "attendance", "work_date", "TEXT")

    ensure_column(db, "daily_records", "attendance_manager_approved_by", "INTEGER")
    ensure_column(db, "daily_records", "attendance_manager_approved_at", "TEXT")

    ensure_column(db, "daily_records", "overtime_manager_approved_by", "INTEGER")
    ensure_column(db, "daily_records", "overtime_manager_approved_at", "TEXT")

    ensure_column(db, "daily_records", "overtime_executive_approved_by", "INTEGER")
    ensure_column(db, "daily_records", "overtime_executive_approved_at", "TEXT")

    db.commit()
    db.close()


def make_token(user):
    payload = {
        "user_id": user["id"],
        "role": user["role"],
        "name": user["name"],
        "exp": datetime.utcnow() + timedelta(days=1),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_token(token):
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])


def auth_required(roles=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            auth = request.headers.get("Authorization", "")
            if not auth.startswith("Bearer "):
                return jsonify({"error": "Unauthorized"}), 401

            token = auth.split(" ", 1)[1]
            try:
                payload = decode_token(token)
            except Exception:
                return jsonify({"error": "Invalid token"}), 401

            db = get_db()
            user = db.execute(
                "SELECT * FROM users WHERE id = ?",
                (payload["user_id"],)
            ).fetchone()

            if not user:
                return jsonify({"error": "User not found"}), 401

            if roles and user["role"] not in roles:
                return jsonify({"error": "Forbidden"}), 403

            g.current_user = user
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_text():
    return datetime.now().strftime("%Y-%m-%d")


def combine_work_date_and_now_time(work_date):
    current_time = datetime.now().strftime("%H:%M:%S")
    return f"{work_date} {current_time}"


def parse_dt(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def minutes_between(start_value, end_value):
    start_dt = parse_dt(start_value)
    end_dt = parse_dt(end_value)
    if not start_dt or not end_dt:
        return 0
    diff = int((end_dt - start_dt).total_seconds() // 60)
    return max(diff, 0)


def format_minutes(total_minutes):
    h = total_minutes // 60
    m = total_minutes % 60
    return f"{h}:{m:02d}"


def build_daily_summary(row):
    break_minutes = minutes_between(row["break_start"], row["break_end"])
    total_minutes = minutes_between(row["clock_in"], row["clock_out"])
    work_minutes = max(total_minutes - break_minutes, 0)

    return {
        **dict(row),
        "work_duration": format_minutes(work_minutes),
        "break_duration": format_minutes(break_minutes),
    }


def get_or_create_daily_record(user_id, work_date):
    db = get_db()
    row = db.execute(
        "SELECT * FROM daily_records WHERE user_id = ? AND work_date = ?",
        (user_id, work_date)
    ).fetchone()
    if row:
        return row

    db.execute(
        "INSERT INTO daily_records (user_id, work_date) VALUES (?, ?)",
        (user_id, work_date)
    )
    db.commit()

    return db.execute(
        "SELECT * FROM daily_records WHERE user_id = ? AND work_date = ?",
        (user_id, work_date)
    ).fetchone()


@app.route("/")
def index():
    return redirect("/login.html")


@app.route("/<path:path>")
def serve_pages(path):
    if path in {"login.html", "stamp.html", "admin.html"}:
        return render_template(path)
    return send_from_directory(BASE_DIR, path)

@app.route("/manifest.json")
def manifest():
    return send_from_directory(BASE_DIR, "manifest.json")


@app.route("/sw.js")
def service_worker():
    return send_from_directory(BASE_DIR, "sw.js")

@app.get("/api/register/options")
def register_options():
    db = get_db()

    managers = db.execute(
        "SELECT id, name, email FROM users WHERE role IN ('manager', 'admin') ORDER BY name"
    ).fetchall()

    executives = db.execute(
        "SELECT id, name, email FROM users WHERE role IN ('executive', 'admin') ORDER BY name"
    ).fetchall()

    user_count = db.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]

    return jsonify({
        "managers": [dict(r) for r in managers],
        "executives": [dict(r) for r in executives],
        "allow_first_admin": user_count == 0
    })


@app.post("/api/register")
def register():
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    role = (data.get("role") or "employee").strip()
    manager_id = data.get("manager_id") or None
    executive_id = data.get("executive_id") or None

    if not name or not email or not password:
        return jsonify({"error": "必須項目を入力してください"}), 400

    if role not in {"employee", "manager", "executive", "admin"}:
        return jsonify({"error": "ロールが不正です"}), 400

    db = get_db()
    existing = db.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        return jsonify({"error": "Email already exists"}), 409

    user_count = db.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    if user_count > 0 and role == "admin":
        return jsonify({"error": "最初の1人以外は admin 登録できません"}), 403

    pw_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    db.execute(
        """
        INSERT INTO users (email, password_hash, name, role, manager_id, executive_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (email, pw_hash, name, role, manager_id, executive_id)
    )
    db.commit()

    return jsonify({"message": "registered"}), 201


@app.post("/api/login")
def login():
    data = request.get_json(force=True)
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    db = get_db()
    user = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()

    if not user or not bcrypt.checkpw(password.encode("utf-8"), user["password_hash"].encode("utf-8")):
        return jsonify({"error": "メールアドレスまたはパスワードが違います"}), 401

    token = make_token(user)
    return jsonify({
        "token": token,
        "role": user["role"],
        "name": user["name"],
        "userId": user["id"],
    })


@app.get("/api/me")
@auth_required()
def me():
    user = g.current_user
    return jsonify({
        "id": user["id"],
        "name": user["name"],
        "email": user["email"],
        "role": user["role"]
    })


@app.post("/api/stamp")
@auth_required()
def stamp():
    data = request.get_json(force=True)

    work_date = (data.get("work_date") or today_text()).strip()
    stamp_type = data.get("type")
    qr_value = data.get("qr_value")
    work_type = (data.get("work_type") or "").strip()
    has_help = 1 if data.get("has_help") else 0
    help_department = (data.get("help_department") or "").strip()
    help_time = (data.get("help_time") or "").strip()
    remarks = (data.get("remarks") or "").strip()

    if qr_value != QR_TEXT:
        return jsonify({"error": "有効なQRコードではありません"}), 400

    field_map = {
        "clock_in": "clock_in",
        "break_start": "break_start",
        "break_end": "break_end",
        "clock_out": "clock_out"
    }

    if stamp_type not in field_map:
        return jsonify({"error": "打刻種別が不正です"}), 400

    record = get_or_create_daily_record(g.current_user["id"], work_date)
    existing_work_type = record["work_type"]

    if stamp_type == "clock_in" and not work_type and not existing_work_type:
        return jsonify({"error": "出勤時は勤務形態を選択してください"}), 400

    db = get_db()
    ts = combine_work_date_and_now_time(work_date)

    db.execute(
        """
        INSERT INTO attendance (user_id, work_date, type, timestamp)
        VALUES (?, ?, ?, ?)
        """,
        (g.current_user["id"], work_date, stamp_type, ts)
    )

    final_work_type = work_type or existing_work_type

    db.execute(
        f"""
        UPDATE daily_records
        SET {field_map[stamp_type]} = ?,
            work_type = ?,
            has_help = ?,
            help_department = ?,
            help_time = ?,
            remarks = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ? AND work_date = ?
        """,
        (
            ts,
            final_work_type,
            has_help,
            help_department,
            help_time,
            remarks,
            g.current_user["id"],
            work_date
        )
    )
    db.commit()

    updated = db.execute(
        "SELECT * FROM daily_records WHERE user_id = ? AND work_date = ?",
        (g.current_user["id"], work_date)
    ).fetchone()

    return jsonify({
        "message": "recorded",
        "type": stamp_type,
        "timestamp": ts,
        "daily": build_daily_summary(updated)
    })


@app.post("/api/overtime-request")
@auth_required()
def overtime_request():
    data = request.get_json(force=True)
    work_date = (data.get("work_date") or today_text()).strip()
    planned_end_time = (data.get("planned_end_time") or "").strip()
    reason = (data.get("reason") or "").strip()

    if not planned_end_time or not reason:
        return jsonify({"error": "終了予定時刻と理由を入力してください"}), 400

    get_or_create_daily_record(g.current_user["id"], work_date)
    db = get_db()

    db.execute(
        """
        UPDATE daily_records
        SET overtime_requested_at = ?,
            overtime_planned_end = ?,
            overtime_reason = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ? AND work_date = ?
        """,
        (
            now_text(),
            planned_end_time,
            reason,
            g.current_user["id"],
            work_date
        )
    )
    db.commit()

    updated = db.execute(
        "SELECT * FROM daily_records WHERE user_id = ? AND work_date = ?",
        (g.current_user["id"], work_date)
    ).fetchone()

    return jsonify({
        "message": "requested",
        "daily": build_daily_summary(updated)
    })

@app.post("/api/day-info/save")
@auth_required()
def save_day_info():
    data = request.get_json(force=True)

    work_date = (data.get("work_date") or today_text()).strip()
    work_type = (data.get("work_type") or "").strip()
    has_help = 1 if data.get("has_help") else 0
    help_department = (data.get("help_department") or "").strip()
    help_time = (data.get("help_time") or "").strip()
    remarks = (data.get("remarks") or "").strip()

    if not work_type:
        return jsonify({"error": "勤務形態を選択してください"}), 400

    get_or_create_daily_record(g.current_user["id"], work_date)
    db = get_db()

    db.execute(
        """
        UPDATE daily_records
        SET work_type = ?,
            has_help = ?,
            help_department = ?,
            help_time = ?,
            remarks = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ? AND work_date = ?
        """,
        (
            work_type,
            has_help,
            help_department,
            help_time,
            remarks,
            g.current_user["id"],
            work_date
        )
    )
    db.commit()

    updated = db.execute(
        "SELECT * FROM daily_records WHERE user_id = ? AND work_date = ?",
        (g.current_user["id"], work_date)
    ).fetchone()

    return jsonify({
        "message": "saved",
        "daily": build_daily_summary(updated)
    })


@app.get("/api/my-attendance")
@auth_required()
def my_attendance():
    db = get_db()
    rows = db.execute(
        """
        SELECT *
        FROM daily_records
        WHERE user_id = ?
        ORDER BY work_date DESC
        """,
        (g.current_user["id"],)
    ).fetchall()

    return jsonify([build_daily_summary(r) for r in rows])


@app.get("/api/attendance")
@auth_required(roles={"admin"})
def attendance_list():
    date = request.args.get("date") or today_text()
    name = (request.args.get("name") or "").strip()

    db = get_db()
    sql = """
        SELECT
            a.id,
            u.name AS user_name,
            a.type,
            a.timestamp,
            a.approved
        FROM attendance a
        JOIN users u ON u.id = a.user_id
        WHERE a.work_date = ?
    """
    params = [date]

    if name:
        sql += " AND u.name LIKE ?"
        params.append(f"%{name}%")

    sql += " ORDER BY a.timestamp DESC"

    rows = db.execute(sql, params).fetchall()
    return jsonify([dict(r) for r in rows])


@app.post("/api/attendance/approve")
@auth_required(roles={"admin"})
def attendance_approve():
    data = request.get_json(force=True)
    attendance_id = data.get("id")
    ids = data.get("ids")

    db = get_db()

    if attendance_id:
        db.execute(
            """
            UPDATE attendance
            SET approved = 1,
                approved_by = ?,
                approved_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (g.current_user["id"], attendance_id),
        )
    elif ids and isinstance(ids, list):
        placeholders = ",".join(["?"] * len(ids))
        db.execute(
            f"""
            UPDATE attendance
            SET approved = 1,
                approved_by = ?,
                approved_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
            """,
            [g.current_user["id"], *ids],
        )
    else:
        return jsonify({"error": "id または ids が必要です"}), 400

    db.commit()
    return jsonify({"message": "approved"})

# =========================
# 承認系API
# =========================

@app.post('/api/approve/attendance')
@auth_required(roles={'manager', 'admin'})
def approve_attendance():
    data = request.get_json()
    record_id = data.get('id')

    db = get_db()

    db.execute('''
        UPDATE daily_records
        SET attendance_manager_approved_by = ?,
            attendance_manager_approved_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (g.current_user['id'], record_id))

    db.commit()
    return jsonify({'message': 'attendance approved'})


@app.post('/api/approve/overtime/manager')
@auth_required(roles={'manager', 'admin'})
def approve_overtime_manager():
    data = request.get_json()
    record_id = data.get('id')

    db = get_db()

    db.execute('''
        UPDATE daily_records
        SET overtime_manager_approved_by = ?,
            overtime_manager_approved_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (g.current_user['id'], record_id))

    db.commit()
    return jsonify({'message': 'overtime manager approved'})

@app.post('/api/approve/overtime/executive')
@auth_required(roles={'executive', 'admin'})
def approve_overtime_executive():
    data = request.get_json()
    record_id = data.get('id')

    db = get_db()

    db.execute('''
        UPDATE daily_records
        SET overtime_executive_approved_by = ?,
            overtime_executive_approved_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (g.current_user['id'], record_id))

    db.commit()
    return jsonify({'message': 'overtime executive approved'})

@app.get('/api/admin/daily')
@auth_required(roles={'manager','executive','admin'})
def admin_daily():
    db = get_db()

    date = request.args.get('date')
    name = request.args.get('name')

    sql = '''
        SELECT d.*, u.name
        FROM daily_records d
        JOIN users u ON u.id = d.user_id
        WHERE 1=1
    '''
    params = []

    if date:
        sql += ' AND d.work_date = ?'
        params.append(date)

    if name:
        sql += ' AND u.name LIKE ?'
        params.append(f'%{name}%')

    sql += ' ORDER BY d.work_date DESC'

    rows = db.execute(sql, params).fetchall()

    return jsonify([dict(r) for r in rows])

init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
