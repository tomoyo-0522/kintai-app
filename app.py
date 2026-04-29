import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from functools import wraps

import bcrypt
import jwt
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, g, jsonify, redirect, render_template, request, send_from_directory

import csv
import io

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATABASE_URL = os.environ.get("DATABASE_URL")
JWT_SECRET = os.environ.get("JWT_SECRET", "change-this-secret")
JWT_ALGORITHM = "HS256"
JST = ZoneInfo("Asia/Tokyo")

QR_MAP = {
    "SUNFARM_KINTAI_HONSHA": "本社",
    "SUNFARM_KINTAI_FARM": "農場",
    "SUNFARM_KINTAI_OODU": "大津"
}

app = Flask(__name__, template_folder="templates")


def get_db():
    if "db" not in g:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is not set")
        g.db = psycopg2.connect(
            DATABASE_URL,
            cursor_factory=RealDictCursor
        )
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db:
        db.close()


def ensure_column(db, table_name, column_name, column_type_sql):
    with db.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
            """,
            (table_name, column_name)
        )
        exists = cur.fetchone()

        if not exists:
            cur.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type_sql}"
            )


def init_db():
    if not DATABASE_URL:
        return

    db = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

    with db.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                name TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'employee',
                manager_id INTEGER,
                executive_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS attendance (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                work_date TEXT NOT NULL,
                type TEXT NOT NULL,
                timestamp TEXT DEFAULT TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS'),
                location TEXT,
                qr_value TEXT,
                approved INTEGER DEFAULT 0,
                approved_by INTEGER,
                approved_at TEXT
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_records (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                work_date TEXT NOT NULL,
                work_type TEXT,
                location TEXT,
                qr_value TEXT,
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
                attendance_manager_approved_by INTEGER,
                attendance_manager_approved_at TEXT,
                overtime_manager_approved_by INTEGER,
                overtime_manager_approved_at TEXT,
                overtime_executive_approved_by INTEGER,
                overtime_executive_approved_at TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, work_date)
            );
            """
        )

    ensure_column(db, "users", "manager_id", "INTEGER")
    ensure_column(db, "users", "executive_id", "INTEGER")

    ensure_column(db, "attendance", "work_date", "TEXT")
    ensure_column(db, "attendance", "location", "TEXT")
    ensure_column(db, "attendance", "qr_value", "TEXT")
    ensure_column(db, "attendance", "approved", "INTEGER DEFAULT 0")
    ensure_column(db, "attendance", "approved_by", "INTEGER")
    ensure_column(db, "attendance", "approved_at", "TEXT")

    ensure_column(db, "daily_records", "work_type", "TEXT")
    ensure_column(db, "daily_records", "location", "TEXT")
    ensure_column(db, "daily_records", "qr_value", "TEXT")
    ensure_column(db, "daily_records", "clock_in", "TEXT")
    ensure_column(db, "daily_records", "break_start", "TEXT")
    ensure_column(db, "daily_records", "break_end", "TEXT")
    ensure_column(db, "daily_records", "clock_out", "TEXT")
    ensure_column(db, "daily_records", "overtime_requested_at", "TEXT")
    ensure_column(db, "daily_records", "overtime_planned_end", "TEXT")
    ensure_column(db, "daily_records", "overtime_reason", "TEXT")
    ensure_column(db, "daily_records", "has_help", "INTEGER DEFAULT 0")
    ensure_column(db, "daily_records", "help_department", "TEXT")
    ensure_column(db, "daily_records", "help_time", "TEXT")
    ensure_column(db, "daily_records", "remarks", "TEXT")
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
            with db.cursor() as cur:
                cur.execute(
                    "SELECT * FROM users WHERE id = %s",
                    (payload["user_id"],)
                )
                user = cur.fetchone()

            if not user:
                return jsonify({"error": "User not found"}), 401

            if roles and user["role"] not in roles:
                return jsonify({"error": "Forbidden"}), 403

            g.current_user = user
            return fn(*args, **kwargs)

        return wrapper
    return decorator


def now_text():
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

def today_text():
    return datetime.now(JST).strftime("%Y-%m-%d")

def combine_work_date_and_now_time(work_date):
    current_time = datetime.now(JST).strftime("%H:%M:%S")
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
    if not row:
        return None

    break_minutes = minutes_between(row.get("break_start"), row.get("break_end"))
    total_minutes = minutes_between(row.get("clock_in"), row.get("clock_out"))
    work_minutes = max(total_minutes - break_minutes, 0)

    result = dict(row)
    result["work_duration"] = format_minutes(work_minutes)
    result["break_duration"] = format_minutes(break_minutes)
    return result


def get_or_create_daily_record(user_id, work_date):
    db = get_db()

    with db.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM daily_records
            WHERE user_id = %s AND work_date = %s
            """,
            (user_id, work_date)
        )
        row = cur.fetchone()

        if row:
            return row

        cur.execute(
            """
            INSERT INTO daily_records (user_id, work_date)
            VALUES (%s, %s)
            ON CONFLICT (user_id, work_date) DO NOTHING
            """,
            (user_id, work_date)
        )

    db.commit()

    with db.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM daily_records
            WHERE user_id = %s AND work_date = %s
            """,
            (user_id, work_date)
        )
        return cur.fetchone()


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

    with db.cursor() as cur:
        cur.execute(
            """
            SELECT id, name, email
            FROM users
            WHERE role IN ('manager', 'admin')
            ORDER BY name
            """
        )
        managers = cur.fetchall()

        cur.execute(
            """
            SELECT id, name, email
            FROM users
            WHERE role IN ('executive', 'admin')
            ORDER BY name
            """
        )
        executives = cur.fetchall()

        cur.execute("SELECT COUNT(*) AS c FROM users")
        user_count = cur.fetchone()["c"]

    return jsonify({
        "managers": managers,
        "executives": executives,
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

    with db.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        if cur.fetchone():
            return jsonify({"error": "Email already exists"}), 409

        cur.execute("SELECT COUNT(*) AS c FROM users")
        user_count = cur.fetchone()["c"]

        if user_count > 0 and role == "admin":
            return jsonify({"error": "最初の1人以外は admin 登録できません"}), 403

        pw_hash = bcrypt.hashpw(
            password.encode("utf-8"),
            bcrypt.gensalt()
        ).decode("utf-8")

        cur.execute(
            """
            INSERT INTO users (email, password_hash, name, role, manager_id, executive_id)
            VALUES (%s, %s, %s, %s, %s, %s)
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

    with db.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE email = %s", (email,))
        user = cur.fetchone()

    if not user or not bcrypt.checkpw(
        password.encode("utf-8"),
        user["password_hash"].encode("utf-8")
    ):
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

    location = QR_MAP.get(qr_value)

    if not location:
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
    existing_work_type = record.get("work_type")

    if stamp_type == "clock_in" and not work_type and not existing_work_type:
        return jsonify({"error": "出勤時は勤務形態を選択してください"}), 400

    ts = combine_work_date_and_now_time(work_date)
    final_work_type = work_type or existing_work_type

    db = get_db()

    with db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO attendance (user_id, work_date, type, timestamp, location, qr_value)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                g.current_user["id"],
                work_date,
                stamp_type,
                ts,
                location,
                qr_value
            )
        )

        cur.execute(
            f"""
            UPDATE daily_records
            SET {field_map[stamp_type]} = %s,
                work_type = %s,
                location = %s,
                qr_value = %s,
                has_help = %s,
                help_department = %s,
                help_time = %s,
                remarks = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s AND work_date = %s
            """,
            (
                ts,
                final_work_type,
                location,
                qr_value,
                has_help,
                help_department,
                help_time,
                remarks,
                g.current_user["id"],
                work_date
            )
        )

    db.commit()

    with db.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM daily_records
            WHERE user_id = %s AND work_date = %s
            """,
            (g.current_user["id"], work_date)
        )
        updated = cur.fetchone()

    return jsonify({
        "message": "recorded",
        "type": stamp_type,
        "timestamp": ts,
        "location": location,
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

    with db.cursor() as cur:
        cur.execute(
            """
            UPDATE daily_records
            SET overtime_requested_at = %s,
                overtime_planned_end = %s,
                overtime_reason = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s AND work_date = %s
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

    with db.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM daily_records
            WHERE user_id = %s AND work_date = %s
            """,
            (g.current_user["id"], work_date)
        )
        updated = cur.fetchone()

    return jsonify({
        "message": "requested",
        "daily": build_daily_summary(updated)
    })


@app.post("/api/day-record/update")
@auth_required()
def update_day_record():
    data = request.get_json(force=True)

    work_date = (data.get("work_date") or today_text()).strip()
    work_type = (data.get("work_type") or "").strip()
    clock_in = (data.get("clock_in") or "").strip()
    break_start = (data.get("break_start") or "").strip()
    break_end = (data.get("break_end") or "").strip()
    clock_out = (data.get("clock_out") or "").strip()
    overtime_planned_end = (data.get("overtime_planned_end") or "").strip()
    overtime_reason = (data.get("overtime_reason") or "").strip()
    has_help = 1 if data.get("has_help") else 0
    help_department = (data.get("help_department") or "").strip()
    help_time = (data.get("help_time") or "").strip()
    remarks = (data.get("remarks") or "").strip()

    if not work_date:
        return jsonify({"error": "日付を入力してください"}), 400

    get_or_create_daily_record(g.current_user["id"], work_date)

    db = get_db()

    with db.cursor() as cur:
        cur.execute(
            """
            UPDATE daily_records
            SET work_type = %s,
                clock_in = %s,
                break_start = %s,
                break_end = %s,
                clock_out = %s,
                overtime_planned_end = %s,
                overtime_reason = %s,
                has_help = %s,
                help_department = %s,
                help_time = %s,
                remarks = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s AND work_date = %s
            """,
            (
                work_type,
                clock_in or None,
                break_start or None,
                break_end or None,
                clock_out or None,
                overtime_planned_end,
                overtime_reason,
                has_help,
                help_department,
                help_time,
                remarks,
                g.current_user["id"],
                work_date,
            )
        )

    db.commit()

    with db.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM daily_records
            WHERE user_id = %s AND work_date = %s
            """,
            (g.current_user["id"], work_date)
        )
        updated = cur.fetchone()

    return jsonify({
        "message": "updated",
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

    with db.cursor() as cur:
        cur.execute(
            """
            UPDATE daily_records
            SET work_type = %s,
                has_help = %s,
                help_department = %s,
                help_time = %s,
                remarks = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s AND work_date = %s
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

    with db.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM daily_records
            WHERE user_id = %s AND work_date = %s
            """,
            (g.current_user["id"], work_date)
        )
        updated = cur.fetchone()

    return jsonify({
        "message": "saved",
        "daily": build_daily_summary(updated)
    })


@app.get("/api/my-attendance")
@auth_required()
def my_attendance():
    db = get_db()

    with db.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM daily_records
            WHERE user_id = %s
            ORDER BY work_date DESC
            """,
            (g.current_user["id"],)
        )
        rows = cur.fetchall()

    return jsonify([build_daily_summary(r) for r in rows])


@app.get("/api/attendance")
@auth_required(roles={"admin"})
def attendance_list():
    date = request.args.get("date") or today_text()
    name = (request.args.get("name") or "").strip()

    sql = """
        SELECT
            a.id,
            u.name AS user_name,
            a.type,
            a.timestamp,
            a.location,
            a.qr_value,
            a.approved
        FROM attendance a
        JOIN users u ON u.id = a.user_id
        WHERE a.work_date = %s
    """
    params = [date]

    if name:
        sql += " AND u.name LIKE %s"
        params.append(f"%{name}%")

    sql += " ORDER BY a.timestamp DESC"

    db = get_db()

    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return jsonify(rows)


@app.post("/api/attendance/approve")
@auth_required(roles={"admin"})
def attendance_approve():
    data = request.get_json(force=True)

    attendance_id = data.get("id")
    ids = data.get("ids")

    db = get_db()

    with db.cursor() as cur:
        if attendance_id:
            cur.execute(
                """
                UPDATE attendance
                SET approved = 1,
                    approved_by = %s,
                    approved_at = %s
                WHERE id = %s
                """,
                (g.current_user["id"], now_text(), attendance_id)
            )
        elif ids and isinstance(ids, list):
            placeholders = ",".join(["%s"] * len(ids))
            cur.execute(
                f"""
                UPDATE attendance
                SET approved = 1,
                    approved_by = %s,
                    approved_at = %s
                WHERE id IN ({placeholders})
                """,
                [g.current_user["id"], now_text(), *ids]
            )
        else:
            return jsonify({"error": "id または ids が必要です"}), 400

    db.commit()
    return jsonify({"message": "approved"})


@app.post("/api/approve/attendance")
@auth_required(roles={"manager", "admin"})
def approve_attendance():
    data = request.get_json(force=True)
    record_id = data.get("id")

    db = get_db()

    with db.cursor() as cur:
        cur.execute(
            """
            UPDATE daily_records
            SET attendance_manager_approved_by = %s,
                attendance_manager_approved_at = %s
            WHERE id = %s
            """,
            (g.current_user["id"], now_text(), record_id)
        )

    db.commit()
    return jsonify({"message": "attendance approved"})


@app.post("/api/approve/overtime/manager")
@auth_required(roles={"manager", "admin"})
def approve_overtime_manager():
    data = request.get_json(force=True)
    record_id = data.get("id")

    db = get_db()

    with db.cursor() as cur:
        cur.execute(
            """
            UPDATE daily_records
            SET overtime_manager_approved_by = %s,
                overtime_manager_approved_at = %s
            WHERE id = %s
            """,
            (g.current_user["id"], now_text(), record_id)
        )

    db.commit()
    return jsonify({"message": "overtime manager approved"})


@app.post("/api/approve/overtime/executive")
@auth_required(roles={"executive", "admin"})
def approve_overtime_executive():
    data = request.get_json(force=True)
    record_id = data.get("id")

    db = get_db()

    with db.cursor() as cur:
        cur.execute(
            """
            UPDATE daily_records
            SET overtime_executive_approved_by = %s,
                overtime_executive_approved_at = %s
            WHERE id = %s
            """,
            (g.current_user["id"], now_text(), record_id)
        )

    db.commit()
    return jsonify({"message": "overtime executive approved"})


@app.get("/api/admin/daily")
@auth_required(roles={"manager", "executive", "admin"})
def admin_daily():
    date = request.args.get("date")
    name = request.args.get("name")

    sql = """
        SELECT d.*, u.name
        FROM daily_records d
        JOIN users u ON u.id = d.user_id
        WHERE 1 = 1
    """
    params = []

    if date:
        sql += " AND d.work_date = %s"
        params.append(date)

    if name:
        sql += " AND u.name LIKE %s"
        params.append(f"%{name}%")

    sql += " ORDER BY d.work_date DESC"

    db = get_db()

    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return jsonify([build_daily_summary(r) for r in rows])


    csv_data = "\ufeff" + output.getvalue()

    return app.response_class(
        csv_data,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=kintai_{start_str}_to_{end_str}.csv"
        }
    )
@app.get("/api/admin/export-csv")
@auth_required(roles={"manager", "executive", "admin"})
def admin_export_csv():
    month = request.args.get("month") or datetime.now().strftime("%Y-%m")
    name = (request.args.get("name") or "").strip()

    year, m = map(int, month.split("-"))

    end_date = datetime(year, m, 15)

    if m == 1:
        start_date = datetime(year - 1, 12, 16)
    else:
        start_date = datetime(year, m - 1, 16)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    sql = """
        SELECT d.*, u.name
        FROM daily_records d
        JOIN users u ON u.id = d.user_id
        WHERE d.work_date BETWEEN %s AND %s
    """
    params = [start_str, end_str]

    if name:
        sql += " AND u.name LIKE %s"
        params.append(f"%{name}%")

    sql += " ORDER BY d.work_date ASC, u.name ASC"

    db = get_db()
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "日付",
        "名前",
        "勤務地",
        "勤務形態",
        "出勤時刻",
        "休憩開始時刻",
        "休憩終了時刻",
        "退勤時刻",
        "勤務時間",
        "休憩時間",
        "残業申請日時",
        "残業終了予定時刻",
        "残業申請理由",
        "ヘルプ有無",
        "ヘルプ部署",
        "ヘルプ時間",
        "備考",
        "勤怠上長承認",
        "残業上長承認",
        "残業役員承認"
    ])

    for r in rows:
        s = build_daily_summary(r)

        writer.writerow([
            s.get("work_date", ""),
            s.get("name", ""),
            s.get("location", ""),
            s.get("work_type", ""),
            s.get("clock_in", ""),
            s.get("break_start", ""),
            s.get("break_end", ""),
            s.get("clock_out", ""),
            s.get("work_duration", ""),
            s.get("break_duration", ""),
            s.get("overtime_requested_at", ""),
            s.get("overtime_planned_end", ""),
            s.get("overtime_reason", ""),
            "あり" if s.get("has_help") else "なし",
            s.get("help_department", ""),
            s.get("help_time", ""),
            s.get("remarks", ""),
            "承認済" if s.get("attendance_manager_approved_at") else "未承認",
            "承認済" if s.get("overtime_manager_approved_at") else "未承認",
            "承認済" if s.get("overtime_executive_approved_at") else "未承認"
        ])

    csv_data = "\ufeff" + output.getvalue()

    return app.response_class(
        csv_data,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=kintai_{start_str}_to_{end_str}.csv"
        }
    )

init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)