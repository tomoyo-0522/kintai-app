import os
# --- ここから追加 ---
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import threading
# --- ここまで追加 ---
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

def sync_to_google_sheet_async(data):
    # dataの想定: [氏名, 年月日, 区分, 時刻, 理由/場所など...]
    try:
        import json
        import os
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials

        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_json = os.environ.get("GOOGLE_CREDENTIALS")
        if not creds_json: return

        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sheet = client.open("勤怠バックアップ").sheet1

        all_records = sheet.get_all_values()
        target_row_index = -1
        
        # A列(氏名)とB列(年月日)で一致する行を探す
        search_name = data[0]
        search_date = data[1]
        
        for i, row in enumerate(all_records):
            if len(row) >= 2:
                if row[0] == search_name and row[1] == search_date:
                    target_row_index = i + 1
                    break

        # 区分(data[2])に応じて書き込む列を決める
        mode = data[2]
        timestamp = data[3] # 打刻時刻

        if target_row_index == -1:
            # 新規行作成（まだその日の行がない場合）
            # A:氏名, B:年月日 を埋めて作成
            new_row = [""] * 13 # A-M列分
            new_row[0] = search_name
            new_row[1] = search_date
            sheet.append_row(new_row)
            # 追加した直後の行番号を取得
            target_row_index = len(all_records) + 1

        # 各ボタンに応じた列への書き込み
        if mode == "出勤":
            sheet.update_cell(target_row_index, 3, timestamp) # C列
        elif mode == "休憩開始":
            sheet.update_cell(target_row_index, 4, timestamp) # D列
        elif mode == "休憩終了":
            sheet.update_cell(target_row_index, 5, timestamp) # E列
        elif mode == "退勤":
            sheet.update_cell(target_row_index, 6, timestamp) # F列
        elif mode == "残業申請":
            # G:申請日時, H:終了予定, I:理由
            sheet.update_cell(target_row_index, 7, timestamp) 
            sheet.update_cell(target_row_index, 8, data[4]) # 終了予定
            sheet.update_cell(target_row_index, 9, data[5]) # 理由
        elif mode == "ヘルプ備考":
            # J:有無, K:部署, L:時間, M:備考
            sheet.update_cell(target_row_index, 10, "あり")
            sheet.update_cell(target_row_index, 11, data[4]) # 部署
            sheet.update_cell(target_row_index, 12, data[5]) # 時間
            sheet.update_cell(target_row_index, 13, data[6]) # 備考

        print(f"DEBUG: {mode} updated for row {target_row_index}")

    except Exception as e:
        print(f"DEBUG: Update Error: {e}")

def sync_to_sheet(data):
    """ユーザーを待たせないように別スレッドで実行する"""
    threading.Thread(target=sync_to_google_sheet_async, args=(data,)).start()

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

    # --- 修正箇所：QRコードの判定 ---
    location = QR_MAP.get(qr_value)

    # 修正：打刻（出勤・退勤など）を伴う場合はQR必須だが、
    # 備考やヘルプ情報の更新（stamp_typeが既存リストにない場合など）はQRなしでも通す
    if not location and stamp_type in field_map:
        return jsonify({"error": "有効なQRコードではありません"}), 400

    field_map = {
        "clock_in": "clock_in",
        "break_start": "break_start",
        "break_end": "break_end",
        "clock_out": "clock_out"
    }

    label_map = {
        "clock_in": "出勤",
        "break_start": "休憩開始",
        "break_end": "休憩終了",
        "clock_out": "退勤"
    }

    if stamp_type not in field_map:
        return jsonify({"error": "打刻種別が不正です"}), 400

    record = get_or_create_daily_record(g.current_user["id"], work_date)
    existing_work_type = record.get("work_type")
    target_field = field_map[stamp_type]

    if record.get(target_field):
        return jsonify({
            "error": f"{label_map[stamp_type]}は既に登録されています。上書きはできません。"
        }), 409

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
            SET {target_field} = %s,
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

    # スプレッドシートに送るデータの中身を作る
    sync_data = [
        g.current_user["name"],    # [0] A列: 氏名
        work_date,                 # [1] B列: 年月日
        "ヘルプ備考",               # [2] 区分 (スプレッドシート側でJ〜M列更新のトリガーになります)
        now_text(),                # [3] 操作時刻
        help_department,           # [4] K列: ヘルプ部署 (sync_to_google_sheet_asyncのdata[4]に対応)
        help_time,                 # [5] L列: ヘルプ時間 (sync_to_google_sheet_asyncのdata[5]に対応)
        remarks                    # [6] M列: 備考 (sync_to_google_sheet_asyncのdata[6]に対応)
    ]
    sync_to_sheet(sync_data)
    # --- ここまで追加 ---

    with db.cursor() as cur:
        # ...（以下、既存の updated = cur.fetchone() などの処理）

    # --- ここから修正（データの並び順をA〜M列に合わせる） ---
    sync_data = [
        g.current_user["name"],    # [0] A列: 氏名
        work_date,                 # [1] B列: 年月日
        label_map[stamp_type],     # [2] 区分 (出勤・休憩などの判定用)
        ts,                        # [3] C〜F列: 打刻時刻
        "",                        # [4] H列用 (空)
        "",                        # [5] I列用 (空)
        remarks                    # [6] M列: 備考
    ]
    sync_to_sheet(sync_data)
    # --- ここまで修正 ---
    # --- ここまで追加 ---

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

    db.commit()

    # --- ここから修正 ---
    sync_data = [
        g.current_user["name"],    # [0] A列: 氏名
        work_date,                 # [1] B列: 年月日
        "残業申請",                 # [2] 区分判定用
        now_text(),                # [3] G列: 申請日時
        planned_end_time,          # [4] H列: 終了予定時刻
        reason,                    # [5] I列: 理由
        ""                         # [6] M列: 備考
    ]
    sync_to_sheet(sync_data)
    # --- ここまで修正 ---

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
        # 対象レコード取得
        cur.execute("SELECT * FROM daily_records WHERE id = %s", (record_id,))
        record = cur.fetchone()

        # 対象ユーザー取得
        cur.execute("SELECT * FROM users WHERE id = %s", (record["user_id"],))
        user = cur.fetchone()

        # 権限チェック
        if g.current_user["role"] == "manager":
            if user["manager_id"] != g.current_user["id"]:
                return jsonify({"error": "権限がありません"}), 403

        cur.execute("""
            UPDATE daily_records
            SET attendance_manager_approved_by = %s,
                attendance_manager_approved_at = %s
            WHERE id = %s
        """, (g.current_user["id"], now_text(), record_id))

    db.commit()
    return jsonify({"message": "attendance approved"})

@app.post("/api/approve/overtime/manager")
@auth_required(roles={"manager", "admin"})
def approve_overtime_manager():
    data = request.get_json(force=True)
    record_id = data.get("id")

    db = get_db()

    with db.cursor() as cur:
        cur.execute("SELECT * FROM daily_records WHERE id = %s", (record_id,))
        record = cur.fetchone()

        cur.execute("SELECT * FROM users WHERE id = %s", (record["user_id"],))
        user = cur.fetchone()

        if g.current_user["role"] == "manager":
            if user["manager_id"] != g.current_user["id"]:
                return jsonify({"error": "権限がありません"}), 403

        cur.execute("""
            UPDATE daily_records
            SET overtime_manager_approved_by = %s,
                overtime_manager_approved_at = %s
            WHERE id = %s
        """, (g.current_user["id"], now_text(), record_id))

    db.commit()
    return jsonify({"message": "overtime manager approved"})

@app.post("/api/approve/overtime/executive")
@auth_required(roles={"executive", "admin"})
def approve_overtime_executive():
    data = request.get_json(force=True)
    record_id = data.get("id")

    db = get_db()

    with db.cursor() as cur:
        cur.execute("SELECT * FROM daily_records WHERE id = %s", (record_id,))
        record = cur.fetchone()

        cur.execute("SELECT * FROM users WHERE id = %s", (record["user_id"],))
        user = cur.fetchone()

        if g.current_user["role"] == "executive":
            if user["executive_id"] != g.current_user["id"]:
                return jsonify({"error": "権限がありません"}), 403

        cur.execute("""
            UPDATE daily_records
            SET overtime_executive_approved_by = %s,
                overtime_executive_approved_at = %s
            WHERE id = %s
        """, (g.current_user["id"], now_text(), record_id))

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

@app.get("/api/admin/users")
@auth_required(roles={"admin"})
def admin_users():
    db = get_db()

    with db.cursor() as cur:
        cur.execute("""
            SELECT 
                u.id, u.name, u.email, u.role,
                m.name AS manager_name,
                e.name AS executive_name
            FROM users u
            LEFT JOIN users m ON u.manager_id = m.id
            LEFT JOIN users e ON u.executive_id = e.id
            ORDER BY u.id
        """)
        users = cur.fetchall()

    return jsonify(users)


@app.post("/api/admin/users")
@auth_required(roles={"admin"})
def admin_create_user():
    data = request.get_json(force=True)

    name = data.get("name")
    email = data.get("email")
    password = data.get("password")
    role = data.get("role")
    manager_id = data.get("manager_id")
    executive_id = data.get("executive_id")

    if not name or not email or not password:
        return jsonify({"error": "必須項目です"}), 400

    db = get_db()

    with db.cursor() as cur:
        pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

        cur.execute("""
            INSERT INTO users (name, email, password_hash, role, manager_id, executive_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (name, email, pw_hash, role, manager_id, executive_id))

    db.commit()
    return jsonify({"message": "created"})

@app.delete("/api/admin/users/<int:user_id>")
@auth_required(roles={"admin"})
def admin_delete_user(user_id):
    db = get_db()

    with db.cursor() as cur:
        # 自分削除禁止
        if user_id == g.current_user["id"]:
            return jsonify({"error": "自分自身は削除できません"}), 400

        # 紐づきチェック（上長・役員）
        cur.execute("""
            SELECT COUNT(*) AS c FROM users
            WHERE manager_id = %s OR executive_id = %s
        """, (user_id, user_id))
        linked = cur.fetchone()["c"]

        if linked > 0:
            return jsonify({"error": "他ユーザーに割り当てられているため削除できません"}), 400

        # 勤怠データチェック
        cur.execute("""
            SELECT COUNT(*) AS c FROM daily_records
            WHERE user_id = %s
        """, (user_id,))
        has_data = cur.fetchone()["c"]

        if has_data > 0:
            return jsonify({"error": "勤怠データがあるため削除できません"}), 400

        cur.execute("DELETE FROM users WHERE id = %s", (user_id,))

    db.commit()
    return jsonify({"message": "deleted"})

init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)