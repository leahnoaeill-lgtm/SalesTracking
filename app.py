"""BE Strategy Sales Dashboard."""
import sqlite3, os, io
from datetime import datetime
from flask import Flask, render_template, request, send_file, redirect, url_for, make_response
import openpyxl

DB = os.path.join(os.path.dirname(__file__), 'be_strategy.db')
ACTIVITY_TYPES = ['call', 'email', 'site visit', 'training', 'demo']
ADMIN_USERS = {u.strip() for u in os.environ.get('ADMIN_USERS', '').split(',') if u.strip()}

app = Flask(__name__)

@app.template_filter('fmtdate')
def fmtdate(v):
    if not v: return ''
    try: return datetime.strptime(str(v)[:10], '%Y-%m-%d').strftime('%b %d, %Y')
    except Exception: return str(v)

def db():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c

def migrate():
    conn = db()
    def add(table, col, decl):
        try: conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
        except sqlite3.OperationalError: pass
    add('activities', 'created_by', 'TEXT')
    add('activities', 'updated_by', 'TEXT')
    add('activities', 'updated_at', 'TEXT')
    add('providers', 'next_step_by', 'TEXT')
    add('providers', 'next_step_at', 'TEXT')
    add('providers', 'kam', 'TEXT')
    add('providers', 'ir', 'TEXT')
    # States west of the Mississippi River
    west = {'WA','OR','CA','NV','ID','MT','WY','UT','AZ','CO','NM','ND','SD',
            'NE','KS','OK','TX','MN','IA','MO','AR','LA','AK','HI'}
    rows = conn.execute("SELECT npi, state FROM providers WHERE kam IS NULL OR kam=''").fetchall()
    for r in rows:
        kam = 'Mark Frantzen' if (r['state'] or '').upper() in west else 'Mike Grillo'
        conn.execute("UPDATE providers SET kam=? WHERE npi=?", (kam, r['npi']))
    conn.commit(); conn.close()
migrate()

SORTABLE = {
    'last_name': 'p.last_name', 'first_name': 'p.first_name',
    'credentials': 'p.credentials', 'specialty': 'p.specialty',
    'total_claims': 'p.total_claims', 'beneficiaries': 'p.beneficiaries',
    'city': 'p.city', 'state': 'p.state', 'clinic_name': 'p.clinic_name', 'kam': 'p.kam',
    'activity_type': 'latest_type', 'activity_date': 'latest_date',
    'next_step': 'p.next_step',
}

def current_user():
    return request.cookies.get('sales_user', '')

def is_admin():
    return current_user() in ADMIN_USERS

def is_clinic_admin():
    return current_user() == 'Leah Noaeill'

@app.context_processor
def inject_admin():
    return {'is_admin': is_admin(), 'is_clinic_admin': is_clinic_admin()}

def fetch_rows(sort='last_name', direction='asc', search='', state='', kam=''):
    sort_col = SORTABLE.get(sort, 'p.last_name')
    direction = 'desc' if direction.lower() == 'desc' else 'asc'
    sql = f"""
    SELECT p.*,
      (SELECT activity_type FROM activities a WHERE a.npi=p.npi
        ORDER BY activity_date DESC, id DESC LIMIT 1) AS latest_type,
      (SELECT activity_date FROM activities a WHERE a.npi=p.npi
        ORDER BY activity_date DESC, id DESC LIMIT 1) AS latest_date
    FROM providers p
    """
    where, params = [], []
    if search:
        where.append("""(p.last_name LIKE ? OR p.first_name LIKE ?
                   OR p.city LIKE ? OR p.state LIKE ? OR p.clinic_name LIKE ?
                   OR p.specialty LIKE ? OR p.npi LIKE ? OR p.kam LIKE ?)""")
        params += [f"%{search}%"] * 8
    if state:
        where.append("p.state = ?"); params.append(state)
    if kam:
        where.append("p.kam = ?"); params.append(kam)
    if where: sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {sort_col} {direction}"
    conn = db()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows

@app.route('/')
def index():
    sort = request.args.get('sort', 'last_name')
    direction = request.args.get('dir', 'asc')
    search = request.args.get('q', '')
    state = request.args.get('state', '')
    kam = request.args.get('kam', '')
    rows = fetch_rows(sort, direction, search, state, kam)
    conn = db()
    states = [r[0] for r in conn.execute("SELECT DISTINCT state FROM providers WHERE state IS NOT NULL AND state<>'' ORDER BY state")]
    kams = [r[0] for r in conn.execute("SELECT DISTINCT kam FROM providers WHERE kam IS NOT NULL AND kam<>'' ORDER BY kam")]
    conn.close()
    return render_template('index.html', rows=rows, sort=sort,
                           direction=direction, search=search,
                           state=state, kam=kam, states=states, kams=kams,
                           user=current_user())

@app.route('/set_user', methods=['POST'])
def set_user():
    name = request.form.get('name', '').strip()
    resp = make_response(redirect(request.referrer or url_for('index')))
    if name:
        resp.set_cookie('sales_user', name, max_age=60*60*24*365)
    return resp

@app.route('/provider/<npi>')
def provider(npi):
    conn = db()
    p = conn.execute("SELECT * FROM providers WHERE npi=?", (npi,)).fetchone()
    if not p:
        conn.close(); return "Not found", 404
    clinics = conn.execute("SELECT * FROM clinics WHERE npi=?", (npi,)).fetchall()
    acts = conn.execute("""SELECT * FROM activities WHERE npi=?
                           ORDER BY activity_date DESC, id DESC""", (npi,)).fetchall()
    conn.close()
    return render_template('provider.html', p=p, clinics=clinics, activities=acts,
                           activity_types=ACTIVITY_TYPES, user=current_user())

@app.route('/provider/<npi>/activity', methods=['POST'])
def add_activity(npi):
    user = current_user()
    if not user: return "Set your name first", 400
    t = request.form.get('activity_type', '').strip()
    d = request.form.get('activity_date', '').strip()
    n = request.form.get('notes', '').strip()
    if t not in ACTIVITY_TYPES or not d: return "Invalid", 400
    conn = db()
    conn.execute("""INSERT INTO activities (npi, activity_type, activity_date, notes, created_by)
                    VALUES (?,?,?,?,?)""", (npi, t, d, n, user))
    # Clear next step after logging the activity
    conn.execute("""UPDATE providers SET next_step='', next_step_by=?, next_step_at=?
                    WHERE npi=?""",
                 (user, datetime.utcnow().isoformat(timespec='seconds'), npi))
    conn.commit(); conn.close()
    return redirect(url_for('provider', npi=npi))

@app.route('/activity/<int:aid>/edit', methods=['POST'])
def edit_activity(aid):
    user = current_user()
    if not user: return "Set your name first", 400
    t = request.form.get('activity_type', '').strip()
    d = request.form.get('activity_date', '').strip()
    n = request.form.get('notes', '').strip()
    if t not in ACTIVITY_TYPES or not d: return "Invalid", 400
    conn = db()
    row = conn.execute("SELECT npi FROM activities WHERE id=?", (aid,)).fetchone()
    if not row: conn.close(); return "Not found", 404
    now = datetime.utcnow().isoformat(timespec='seconds')
    conn.execute("""UPDATE activities SET activity_type=?, activity_date=?, notes=?,
                    updated_by=?, updated_at=? WHERE id=?""",
                 (t, d, n, user, now, aid))
    ns = request.form.get('next_step', None)
    if ns is not None:
        conn.execute("""UPDATE providers SET next_step=?, next_step_by=?, next_step_at=?
                        WHERE npi=?""", (ns, user, now, row['npi']))
    conn.commit(); npi = row['npi']; conn.close()
    return redirect(url_for('provider', npi=npi))

@app.route('/provider/<npi>/kam', methods=['POST'])
def update_kam(npi):
    kam = request.form.get('kam', '').strip()
    conn = db()
    conn.execute("UPDATE providers SET kam=? WHERE npi=?", (kam, npi))
    conn.commit(); conn.close()
    return redirect(url_for('provider', npi=npi))

@app.route('/provider/<npi>/ir', methods=['POST'])
def update_ir(npi):
    ir = request.form.get('ir', '').strip()
    conn = db()
    conn.execute("UPDATE providers SET ir=? WHERE npi=?", (ir, npi))
    conn.commit(); conn.close()
    return redirect(url_for('provider', npi=npi))

@app.route('/provider/<npi>/next_step', methods=['POST'])
def update_next_step(npi):
    user = current_user()
    if not user: return "Set your name first", 400
    ns = request.form.get('next_step', '')
    conn = db()
    conn.execute("""UPDATE providers SET next_step=?, next_step_by=?, next_step_at=?
                    WHERE npi=?""",
                 (ns, user, datetime.utcnow().isoformat(timespec='seconds'), npi))
    conn.commit(); conn.close()
    return redirect(url_for('provider', npi=npi))

WEST_STATES = {'WA','OR','CA','NV','ID','MT','WY','UT','AZ','CO','NM','ND','SD',
               'NE','KS','OK','TX','MN','IA','MO','AR','LA','AK','HI'}

@app.route('/provider/new', methods=['POST'])
def add_provider():
    if not is_clinic_admin(): return "Forbidden", 403
    npi = request.form.get('npi', '').strip()
    if not npi: return "NPI required", 400
    conn = db()
    exists = conn.execute("SELECT 1 FROM providers WHERE npi=?", (npi,)).fetchone()
    if exists:
        conn.close(); return f"NPI {npi} already exists", 400
    state = request.form.get('state', '').strip().upper()
    kam = request.form.get('kam', '').strip()
    if not kam:
        kam = 'Mark Frantzen' if state in WEST_STATES else 'Mike Grillo'
    conn.execute("""INSERT INTO providers
        (npi, first_name, last_name, credentials, specialty, patient_focus,
         conditions, city, state, clinic_name, kam, ir, next_step,
         total_claims, beneficiaries)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'',0,0)""",
        (npi,
         request.form.get('first_name', '').strip(),
         request.form.get('last_name', '').strip(),
         request.form.get('credentials', '').strip(),
         request.form.get('specialty', '').strip(),
         request.form.get('patient_focus', '').strip(),
         request.form.get('conditions', '').strip(),
         request.form.get('city', '').strip(),
         state,
         request.form.get('clinic_name', '').strip(),
         kam,
         request.form.get('ir', '').strip()))
    conn.commit(); conn.close()
    return redirect(url_for('provider', npi=npi))

@app.route('/provider/<npi>/edit', methods=['POST'])
def edit_provider(npi):
    if not is_clinic_admin(): return "Forbidden", 403
    conn = db()
    row = conn.execute("SELECT 1 FROM providers WHERE npi=?", (npi,)).fetchone()
    if not row: conn.close(); return "Not found", 404
    conn.execute("""UPDATE providers SET first_name=?, last_name=?, credentials=?,
                    specialty=?, patient_focus=?, conditions=?, city=?, state=?,
                    clinic_name=? WHERE npi=?""",
                 (request.form.get('first_name', '').strip(),
                  request.form.get('last_name', '').strip(),
                  request.form.get('credentials', '').strip(),
                  request.form.get('specialty', '').strip(),
                  request.form.get('patient_focus', '').strip(),
                  request.form.get('conditions', '').strip(),
                  request.form.get('city', '').strip(),
                  request.form.get('state', '').strip().upper(),
                  request.form.get('clinic_name', '').strip(),
                  npi))
    conn.commit(); conn.close()
    return redirect(url_for('provider', npi=npi))

@app.route('/provider/<npi>/clinic', methods=['POST'])
def add_clinic(npi):
    if not is_clinic_admin(): return "Forbidden", 403
    name = request.form.get('name', '').strip()
    if not name: return "Clinic name required", 400
    address = request.form.get('address', '').strip()
    city = request.form.get('city', '').strip()
    state = request.form.get('state', '').strip()
    zip_ = request.form.get('zip', '').strip()
    conn = db()
    conn.execute("""INSERT INTO clinics (npi, name, address, city, state, zip)
                    VALUES (?,?,?,?,?,?)""",
                 (npi, name, address, city, state, zip_))
    conn.commit(); conn.close()
    return redirect(url_for('provider', npi=npi))

@app.route('/clinic/<int:cid>/edit', methods=['POST'])
def edit_clinic(cid):
    if not is_clinic_admin(): return "Forbidden", 403
    name = request.form.get('name', '').strip()
    address = request.form.get('address', '').strip()
    city = request.form.get('city', '').strip()
    state = request.form.get('state', '').strip()
    zip_ = request.form.get('zip', '').strip()
    conn = db()
    row = conn.execute("SELECT npi FROM clinics WHERE id=?", (cid,)).fetchone()
    if not row: conn.close(); return "Not found", 404
    conn.execute("""UPDATE clinics SET name=?, address=?, city=?, state=?, zip=?
                    WHERE id=?""", (name, address, city, state, zip_, cid))
    conn.commit(); npi = row['npi']; conn.close()
    return redirect(url_for('provider', npi=npi))

@app.route('/clinic/<int:cid>/delete', methods=['POST'])
def delete_clinic(cid):
    if not is_clinic_admin(): return "Forbidden", 403
    conn = db()
    row = conn.execute("SELECT npi FROM clinics WHERE id=?", (cid,)).fetchone()
    if not row: conn.close(); return "Not found", 404
    conn.execute("DELETE FROM clinics WHERE id=?", (cid,))
    conn.commit(); npi = row['npi']; conn.close()
    return redirect(url_for('provider', npi=npi))

@app.route('/export')
def export():
    if not is_admin():
        return "Forbidden — admin access required", 403
    sort = request.args.get('sort', 'last_name')
    direction = request.args.get('dir', 'asc')
    search = request.args.get('q', '')
    state = request.args.get('state', '')
    kam = request.args.get('kam', '')
    rows = fetch_rows(sort, direction, search, state, kam)
    wb = openpyxl.Workbook()
    ws = wb.active; ws.title = 'Providers'
    ws.append(['NPI','Last Name','First Name','Credentials','Specialty',
               'Patient Focus','Conditions','Total Claims','Beneficiaries',
               'City','State','KAM','IR','Clinic Name','Latest Activity','Latest Activity Date',
               'Next Step','Next Step By','Next Step At'])
    for r in rows:
        ws.append([r['npi'], r['last_name'], r['first_name'], r['credentials'],
                   r['specialty'], r['patient_focus'], r['conditions'],
                   r['total_claims'], r['beneficiaries'], r['city'], r['state'],
                   r['kam'], r['ir'], r['clinic_name'], r['latest_type'], r['latest_date'],
                   r['next_step'], r['next_step_by'], r['next_step_at']])
    ws2 = wb.create_sheet('Activities')
    ws2.append(['NPI','Last Name','First Name','Activity Type','Activity Date',
                'Notes','Created By','Created At','Updated By','Updated At'])
    conn = db()
    arows = conn.execute("""SELECT a.*, p.last_name, p.first_name FROM activities a
                            JOIN providers p ON p.npi=a.npi
                            ORDER BY p.last_name, a.activity_date DESC""").fetchall()
    conn.close()
    for a in arows:
        ws2.append([a['npi'], a['last_name'], a['first_name'], a['activity_type'],
                    a['activity_date'], a['notes'], a['created_by'], a['created_at'],
                    a['updated_by'], a['updated_at']])
    buf = io.BytesIO(); wb.save(buf); buf.seek(0)
    return send_file(buf, as_attachment=True, download_name='be_strategy_export.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

if __name__ == '__main__':
    app.run(debug=True, port=5002)
