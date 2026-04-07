"""Load cms_providers_E0483_2023.xlsx into SQLite."""
import sqlite3, openpyxl, os

DB = os.path.join(os.path.dirname(__file__), 'be_strategy.db')
XLSX = os.path.join(os.path.dirname(__file__), 'cms_providers_E0483_2023.xlsx')

SCHEMA = """
CREATE TABLE IF NOT EXISTS providers (
  npi TEXT PRIMARY KEY,
  first_name TEXT, last_name TEXT, credentials TEXT,
  specialty TEXT, patient_focus TEXT, conditions TEXT,
  total_claims INTEGER, beneficiaries INTEGER,
  city TEXT, state TEXT, year INTEGER, hcpcs TEXT,
  clinic_name TEXT, next_step TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS clinics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  npi TEXT, name TEXT, address TEXT, city TEXT, state TEXT, zip TEXT,
  FOREIGN KEY(npi) REFERENCES providers(npi)
);
CREATE TABLE IF NOT EXISTS activities (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  npi TEXT NOT NULL,
  activity_type TEXT NOT NULL,
  activity_date TEXT NOT NULL,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(npi) REFERENCES providers(npi)
);
CREATE INDEX IF NOT EXISTS idx_act_npi ON activities(npi);
"""

def main():
    conn = sqlite3.connect(DB)
    conn.executescript(SCHEMA)
    wb = openpyxl.load_workbook(XLSX, read_only=True)
    ws = wb.active
    rows = ws.iter_rows(min_row=2, values_only=True)
    current_npi = None
    p_count = c_count = 0
    for r in rows:
        (npi, fn, ln, cred, spec, focus, conds, claims, benes,
         city, state, year, hcpcs, cname, caddr, ccity, cstate, czip) = r
        if npi:
            current_npi = str(npi)
            # first clinic name is the primary
            conn.execute("""INSERT OR REPLACE INTO providers
                (npi, first_name, last_name, credentials, specialty, patient_focus,
                 conditions, total_claims, beneficiaries, city, state, year, hcpcs, clinic_name, next_step)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                  COALESCE((SELECT next_step FROM providers WHERE npi=?), ''))""",
                (current_npi, fn, ln, cred, spec, focus, conds, claims, benes,
                 city, state, year, hcpcs, cname, current_npi))
            # clear prior clinics for this npi on reload
            conn.execute("DELETE FROM clinics WHERE npi=?", (current_npi,))
            p_count += 1
        if current_npi and cname:
            conn.execute("""INSERT INTO clinics (npi, name, address, city, state, zip)
                VALUES (?,?,?,?,?,?)""", (current_npi, cname, caddr, ccity, cstate, czip))
            c_count += 1
    conn.commit()
    conn.close()
    print(f"Loaded {p_count} providers, {c_count} clinic rows -> {DB}")

if __name__ == '__main__':
    main()
