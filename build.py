"""
ProofServe Dashboard Build Script
Reads data from Google Sheets, generates dashboard HTML, outputs to ./output/index.html

This script runs in GitHub Actions on a schedule.
It can also be run locally: python build.py --local path/to/spreadsheet.xlsx
"""

import os
import sys
import json
import re
import statistics
import tempfile
from collections import defaultdict
from pathlib import Path

# ═══════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════

MONTHS = ['Jan', 'Feb', 'Mar', 'Apr']
MONTH_MAP = {'2026-01': 'Jan', '2026-02': 'Feb', '2026-03': 'Mar', '2026-04': 'Apr'}

WEIGHTS = {
    'Service': {'Chats': 30, '1st Attempt Breach (1+)': 25, 'Job SLA Breach': 20,
                'Bad Address': 10, 'Ad Hoc': 8, 'Attempt Limit': 5, 'Nudge Server (1+)': 2},
    'ESC Lead': {'Chats': 30, '1st Attempt Breach (1+)': 25, 'Job SLA Breach': 20,
                 'Bad Address': 10, 'Ad Hoc': 8, 'Attempt Limit': 5, 'Nudge Server (1+)': 2},
    'Off App': {'Chats': 30, '1st Attempt Breach (1+)': 25, 'Job SLA Breach': 20,
                'Bad Address': 10, 'Ad Hoc': 8, 'Attempt Limit': 5, 'Nudge Server (1+)': 2},
    'QA': {'Affidavit Preparation': 75, 'Affidavit Changes': 20, 'Ad Hoc': 5},
    'Dispatch': {'Ad Hoc': 20, 'Assign Server': 80},
}

ROLE_NORM = {
    'Escalation Leads': 'ESC Lead', 'Service': 'Service', 'QA': 'QA',
    'Dispatch': 'Dispatch', 'Off App': 'Off App', 'Team Lead': 'Team Lead'
}


# ═══════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════

def sf(v):
    """Safe float conversion"""
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    try: return float(str(v).replace(',', '').replace('%', '').strip())
    except: return None

def si(v):
    """Safe int conversion"""
    if v is None: return 0
    if isinstance(v, (int, float)): return int(v)
    try: return int(str(v).replace(',', '').strip())
    except: return 0

def pm(v):
    """Parse date value to month label"""
    if v is None: return None
    s = str(v)
    m = re.search(r'(2026-0[1-4])', s)
    return MONTH_MAP.get(m.group(1)) if m else None

def cn(v):
    """Clean name: strip whitespace, remove @, filter non-people"""
    if not v: return None
    s = str(v).strip()
    if s.startswith('@'): s = s[1:].strip()
    if s.lower() in ('none', 'server', 'client', 'nan', ''): return None
    return s

def quality_score(wq, total_tasks):
    """Compute quality score with volume-adjusted anchor (k=75)"""
    if total_tasks == 0: return None
    anchor = 0.03 + (75.0 / total_tasks)
    return round(max(0, min(100, 100 - (wq / anchor) * 100)), 1)


# ═══════════════════════════════════════════
# DATA EXTRACTION
# ═══════════════════════════════════════════

def get_workbook(local_path=None):
    """Get openpyxl workbook from Google Sheets or local file"""
    import openpyxl

    if local_path:
        print(f"Reading local file: {local_path}")
        return openpyxl.load_workbook(local_path, data_only=True)

    # Download from Google Sheets
    import gspread
    from google.oauth2.service_account import Credentials

    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    sheet_id = os.environ.get('SHEET_ID')

    if not creds_path or not sheet_id:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS and SHEET_ID env vars required")
        sys.exit(1)

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly'
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    gc = gspread.authorize(creds)

    print(f"Downloading sheet {sheet_id} as xlsx...")
    spreadsheet = gc.open_by_key(sheet_id)

    # Export as xlsx
    xlsx_bytes = spreadsheet.export(format=gspread.utils.ExportFormat.EXCEL)
    tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
    tmp.write(xlsx_bytes)
    tmp.close()
    print(f"Downloaded to {tmp.name}")

    return openpyxl.load_workbook(tmp.name, data_only=True)


def extract_roster(wb):
    """Extract roster from Roster1 sheet"""
    roster = {}
    ws = wb['Roster1']
    for r in range(3, 200):
        for base in [1, 4, 7, 10, 13]:
            t = ws.cell(r, base).value
            n = ws.cell(r, base + 1).value
            rl = ws.cell(r, base + 2).value
            if n and rl and t:
                nm = str(n).strip()
                role = ROLE_NORM.get(str(rl).strip(), str(rl).strip())
                roster[nm] = {'team': str(t).strip(), 'role': role}
    print(f"Roster: {len(roster)} reps")
    return roster


def detect_layout(ws):
    """Detect if Row 1 has task names or dates"""
    r1c2 = str(ws.cell(1, 2).value or '')
    return 'dates_first' if '2026' in r1c2 else 'tasks_first'


def parse_sheet(ws):
    """Parse a Tasks or Adh sheet with auto-detected layout"""
    layout = detect_layout(ws)
    task_row, month_row = (2, 1) if layout == 'dates_first' else (1, 2)

    cols = {}
    for c in range(2, ws.max_column + 1):
        tn = ws.cell(task_row, c).value
        md = ws.cell(month_row, c).value
        if tn and md:
            mi = pm(md)
            if mi: cols[(str(tn).strip(), mi)] = c

    task_names = sorted(set(k[0] for k in cols))
    records = {}
    for r in range(4, 300):
        name = ws.cell(r, 1).value
        if not name: break
        n = str(name).strip()
        records[n] = {}
        for mi in MONTHS:
            records[n][mi] = {}
            for tn in task_names:
                col = cols.get((tn, mi))
                records[n][mi][tn] = sf(ws.cell(r, col).value) if col else None
    return records, task_names


def extract_fails(wb, roster):
    """Extract fails from all 4 Monday boards"""
    roster_names = set(roster.keys())
    all_fails = []

    # Affidavit Exceptions
    ws = wb['Affidavit Exceptions']
    for r in range(2, ws.max_row + 1):
        month = pm(ws.cell(r, 9).value)
        if not month: continue
        attrib = str(ws.cell(r, 14).value or '').strip()
        nm = cn(ws.cell(r, 13).value)
        nj = cn(ws.cell(r, 10).value)
        cat = str(ws.cell(r, 2).value or '')
        job = str(ws.cell(r, 6).value or '')
        note = str(ws.cell(r, 11).value or '')[:200]
        firm = cn(ws.cell(r, 17).value)
        reason = str(ws.cell(r, 12).value or '')[:100]
        if attrib == 'Attributee' and nm:
            all_fails.append({'p': nm, 'm': month, 'b': 'Aff Exceptions', 'c': cat,
                              'j': job, 'note': note, 'f': firm, 'reason': reason})
        elif attrib == 'Both':
            if nm:
                all_fails.append({'p': nm, 'm': month, 'b': 'Aff Exceptions', 'c': cat,
                                  'j': job, 'note': note, 'f': firm, 'reason': reason})
            if nj and nj != nm:
                all_fails.append({'p': nj, 'm': month, 'b': 'Aff Exceptions', 'c': cat,
                                  'j': job, 'note': note, 'f': firm, 'reason': reason})

    # Ops Sales Escal
    ws = wb['Ops Sales Escal']
    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, 9).value or '').strip().lower() != 'fail': continue
        name = cn(ws.cell(r, 7).value)
        if not name: continue
        month = pm(ws.cell(r, 10).value)
        if not month: continue
        all_fails.append({'p': name, 'm': month, 'b': 'Ops Sales Escal',
                          'c': str(ws.cell(r, 12).value or '')[:60],
                          'j': str(ws.cell(r, 2).value or ''),
                          'note': str(ws.cell(r, 13).value or '')[:200],
                          'f': cn(ws.cell(r, 15).value), 'reason': ''})

    # Client Feedback
    ws = wb['Client Feedback']
    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, 12).value or '').strip().lower() != 'fail': continue
        name = cn(ws.cell(r, 7).value)
        if not name: continue
        month = pm(ws.cell(r, 9).value)
        if not month: continue
        raw_b = str(ws.cell(r, 2).value or '')
        parts = re.split(r'\s*[-\u2013\u2014]\s*', raw_b)
        firm = parts[-1].strip() if len(parts) >= 2 else None
        all_fails.append({'p': name, 'm': month, 'b': 'Client Feedback',
                          'c': 'Client Feedback',
                          'j': str(ws.cell(r, 13).value or ''),
                          'note': str(ws.cell(r, 10).value or '')[:200],
                          'f': firm, 'reason': ''})

    # Ops Leadership
    ws = wb['Ops Leadership']
    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, 13).value or '').strip().lower() != 'fail': continue
        name = cn(ws.cell(r, 8).value)
        if not name: continue
        month = pm(ws.cell(r, 11).value)
        if not month: month = pm(ws.cell(r, 4).value)
        if not month: continue
        all_fails.append({'p': name, 'm': month, 'b': 'Ops Leadership',
                          'c': str(ws.cell(r, 15).value or '')[:60],
                          'j': str(ws.cell(r, 6).value or ''),
                          'note': str(ws.cell(r, 16).value or '')[:200],
                          'f': cn(ws.cell(r, 7).value),
                          'reason': str(ws.cell(r, 15).value or '')[:100]})

    # Filter to roster names only
    all_fails = [f for f in all_fails if f['p'] in roster_names]
    print(f"Total fails (roster-filtered): {len(all_fails)}")
    return all_fails


def extract_rework(wb):
    """Extract rework rate from Rework Rate sheet"""
    ws = wb['Rework Rate']
    rework = {}
    # Pattern: 6 cols per month, Not-Server Rate is the 6th col
    # Jan=Col7, Feb=Col13, Mar=Col19, Apr=Col25
    for r in range(3, ws.max_row + 1):
        name = ws.cell(r, 1).value
        if not name: break
        n = str(name).strip()
        rework[n] = {
            'Jan': sf(ws.cell(r, 7).value) or 0,
            'Feb': sf(ws.cell(r, 13).value) or 0,
            'Mar': sf(ws.cell(r, 19).value) or 0,
            'Apr': sf(ws.cell(r, 25).value) or 0,
        }
    return rework


def extract_tot(wb):
    """Extract Time on Tasks from 50K raw rows"""
    ws = wb['Time on Tasks']
    agg = defaultdict(lambda: defaultdict(lambda: {
        'tms': [], 'fms': [], 'paces': defaultdict(int),
        'overlaps': defaultdict(int), 'n': 0
    }))

    for r in range(2, ws.max_row + 1):
        person = cn(ws.cell(r, 1).value)
        if not person: continue
        mi = pm(ws.cell(r, 4).value)
        if not mi: continue
        tm = sf(ws.cell(r, 22).value)
        fm = sf(ws.cell(r, 20).value)
        pace = str(ws.cell(r, 28).value or '').strip().lower()
        overlap = str(ws.cell(r, 29).value or '').strip().lower()
        d = agg[person][mi]
        d['n'] += 1
        if tm is not None: d['tms'].append(tm)
        if fm is not None: d['fms'].append(fm)
        if pace: d['paces'][pace] += 1
        if overlap: d['overlaps'][overlap] += 1

    tot_data = {}
    for p, months in agg.items():
        tot_data[p] = {}
        for mi, d in months.items():
            if d['n'] == 0: continue
            tp = sum(d['paces'].values()) or 1
            to = sum(d['overlaps'].values()) or 1
            tot_data[p][mi] = {
                'med': round(statistics.median(d['tms']), 1) if d['tms'] else 0,
                'foc': round(sum(d['fms']) / max(sum(d['tms']), 0.01) * 100, 1) if d['tms'] else 0,
                'lng': round(d['paces'].get('long', 0) / tp * 100, 1),
                'rap': round(d['paces'].get('rapid', 0) / tp * 100, 1),
                'sol': round(d['overlaps'].get('solo', 0) / to * 100, 1),
                'tot': round(sum(d['tms']), 0),
                'n': d['n']
            }
    print(f"ToT: {len(tot_data)} people with data")
    return tot_data


# ═══════════════════════════════════════════
# BUILD MONTHLY DATA
# ═══════════════════════════════════════════

def build_monthly_data(wb, roster, all_fails, rework, tot_data):
    """Build the complete monthly data structure"""

    # Parse all role sheets
    roles_tasks = {}
    roles_adh = {}
    for role, ts, ads in [
        ('Service', 'Service (Tasks)', 'Service (Adh)'),
        ('ESC Lead', 'ESC Lead (Tasks)', 'ESC Lead (Adh)'),
        ('QA', 'QA (Tasks)', 'QA (Adh)'),
        ('Dispatch', 'Dispatch (Tasks)', 'Dispatch (Adh)'),
        ('Off App', 'Off App (Tasks)', 'Off App (Adh)')
    ]:
        t_data, _ = parse_sheet(wb[ts])
        a_data, _ = parse_sheet(wb[ads])
        roles_tasks[role] = t_data
        roles_adh[role] = a_data
        print(f"  {role}: {len(t_data)} reps in Tasks, {len(a_data)} in Adh, layout={detect_layout(wb[ads])}")

    # Build fail lookup
    fail_map = defaultdict(lambda: defaultdict(list))
    for f in all_fails:
        fail_map[f['p']][f['m']].append(f)

    output = {'months': {}}

    for mi in MONTHS:
        md = {'roles': {}}

        for role in ['Service', 'ESC Lead', 'QA', 'Dispatch', 'Off App']:
            wts = WEIGHTS[role]
            reps = []
            t_data = roles_tasks[role]
            a_data = roles_adh[role]

            for name in set(list(t_data.keys()) + list(a_data.keys())):
                ri = roster.get(name, {})
                if ri.get('role') != role: continue

                tasks_m = t_data.get(name, {}).get(mi, {})
                adh_m = a_data.get(name, {}).get(mi, {})

                total_tasks = sum(v or 0 for v in tasks_m.values() if isinstance(v, (int, float)))
                if total_tasks == 0: continue

                # W.Adh
                wtotal = 0
                wsum = 0
                ps_dict = {}
                tk_dict = {}
                for tname, wt in wts.items():
                    t_count = tasks_m.get(tname) or 0
                    adh_val = adh_m.get(tname)
                    if isinstance(t_count, (int, float)) and t_count > 0:
                        tk_dict[tname] = int(t_count)
                    if adh_val is not None and isinstance(adh_val, (int, float)) and t_count and t_count > 0:
                        ps = min(100, max(0, adh_val * 100))
                        ps_dict[tname] = round(ps, 1)
                        wtotal += ps * wt / 100
                        wsum += wt

                if wsum == 0: continue
                wadh = round(wtotal * 100 / wsum, 1)
                if wadh < 5: continue

                # Quality
                person_fails = fail_map.get(name, {}).get(mi, [])
                mf = len(person_fails)
                if mf > 0:
                    wq = mf / total_tasks
                    qs = quality_score(wq, total_tasks)
                else:
                    wq = 0
                    qs = 100.0

                ops = round(wadh * 0.4 + qs * 0.6, 1)
                rw = round(rework.get(name, {}).get(mi, 0) * 100, 2)
                tot_p = tot_data.get(name, {}).get(mi)

                rep = {
                    'n': name, 't': ri['team'].split('(')[0].strip(),
                    'tf': ri['team'], 'r': role,
                    'w': wadh, 'q': qs, 'o': ops, 'tt': int(total_tasks),
                    'mf': mf, 'wq': round(wq * 100, 4),
                    'rr': rw, 'ps': ps_dict, 'tk': tk_dict,
                    'fd': [{'b': f['b'], 'c': f['c'], 'j': f['j'], 'note': f['note'],
                            'f': f.get('f', ''), 'reason': f.get('reason', ''), 'm': f['m']}
                           for f in person_fails[:10]]
                }
                if tot_p: rep['tot'] = tot_p
                reps.append(rep)

            reps.sort(key=lambda x: x['o'], reverse=True)
            md['roles'][role] = reps

        # Org stats
        all_r = [r for reps in md['roles'].values() for r in reps]
        ranked = [r for r in all_r if r.get('o')]
        if ranked:
            md['org'] = {
                'avg_ops': round(sum(r['o'] for r in ranked) / len(ranked), 1),
                'avg_wadh': round(sum(r['w'] for r in ranked) / len(ranked), 1),
                'avg_qs': round(sum(r['q'] for r in ranked) / len(ranked), 1),
                'total_tasks': sum(r['tt'] for r in all_r),
                'total_fails': sum(r['mf'] for r in all_r),
                'ranked_count': len(ranked),
                'avg_rework': round(sum(r['rr'] for r in all_r) / max(len(all_r), 1), 2)
            }
        else:
            md['org'] = {'avg_ops': 0, 'avg_wadh': 0, 'avg_qs': 0,
                         'total_tasks': 0, 'total_fails': 0, 'ranked_count': 0, 'avg_rework': 0}

        # Teams
        teams = {}
        for r in ranked:
            tn = r['tf']
            if tn not in teams:
                teams[tn] = {'ops': [], 'qs': [], 'wadh': [], 'tasks': 0, 'errors': 0, 'reps': []}
            teams[tn]['ops'].append(r['o'])
            teams[tn]['qs'].append(r['q'])
            teams[tn]['wadh'].append(r['w'])
            teams[tn]['tasks'] += r['tt']
            teams[tn]['errors'] += r['mf']
            teams[tn]['reps'].append({'name': r['n'], 'ops': r['o'], 'role': r['r']})

        md['teams'] = {
            tn: {
                'avg_ops': round(sum(v['ops']) / len(v['ops']), 1),
                'avg_qs': round(sum(v['qs']) / len(v['qs']), 1),
                'avg_wadh': round(sum(v['wadh']) / len(v['wadh']), 1),
                'n': len(v['ops']), 'tasks': v['tasks'], 'errors': v['errors'],
                'reps': sorted(v['reps'], key=lambda x: x['ops'], reverse=True)
            }
            for tn, v in sorted(teams.items(),
                                key=lambda x: sum(x[1]['ops']) / len(x[1]['ops']),
                                reverse=True)
        }

        # Winners
        md['winners'] = {}
        for role, reps in md['roles'].items():
            valid = [r for r in reps if r.get('o')]
            if valid:
                md['winners'][role] = {'name': valid[0]['n'], 'ops': valid[0]['o'], 'team': valid[0]['t']}

        output['months'][mi] = md
        print(f"{mi}: OPS={md['org']['avg_ops']} Fails={md['org']['total_fails']} Ranked={md['org']['ranked_count']}")

    # Firm data for Client View
    firm_data = defaultdict(lambda: {'count': 0, 'months': defaultdict(int), 'people': set(), 'details': []})
    for f in all_fails:
        firm = f.get('f') or 'Unknown / Not recorded'
        firm_data[firm]['count'] += 1
        firm_data[firm]['months'][f['m']] += 1
        firm_data[firm]['people'].add(f['p'])
        if len(firm_data[firm]['details']) < 25:
            firm_data[firm]['details'].append({
                'b': f['b'], 'c': f['c'], 'j': f['j'], 'note': f['note'],
                'f': firm, 'person': f['p'], 'team': roster.get(f['p'], {}).get('team', ''),
                'reason': f.get('reason', ''), 'm': f['m']
            })

    output['firms'] = {
        k: {'c': v['count'], 'm': dict(v['months']), 'p': list(v['people']), 'd': v['details']}
        for k, v in sorted(firm_data.items(), key=lambda x: x[1]['count'], reverse=True)[:60]
    }

    return output


# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════

def main():
    # Check for local mode
    local_path = None
    if len(sys.argv) > 1 and sys.argv[1] == '--local' and len(sys.argv) > 2:
        local_path = sys.argv[2]

    # Get workbook
    wb = get_workbook(local_path)

    # Extract all data
    print("\n=== Extracting data ===")
    roster = extract_roster(wb)
    all_fails = extract_fails(wb, roster)
    rework = extract_rework(wb)
    tot_data = extract_tot(wb)

    print("\n=== Building monthly data ===")
    data = build_monthly_data(wb, roster, all_fails, rework, tot_data)

    # Generate JSON
    data_json = json.dumps(data, separators=(',', ':'), default=str)
    print(f"\nData JSON: {len(data_json):,} chars")

    # Read template and inject data
    template_path = Path(__file__).parent / 'template.html'
    if not template_path.exists():
        print(f"ERROR: template.html not found at {template_path}")
        sys.exit(1)

    template = template_path.read_text(encoding='utf-8')
    html = template.replace('__DASHBOARD_DATA_PLACEHOLDER__', data_json)

    # Write output
    output_dir = Path(__file__).parent / 'output'
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / 'index.html'
    output_file.write_text(html, encoding='utf-8')

    print(f"\nDashboard written to {output_file} ({output_file.stat().st_size:,} bytes)")
    print("Done!")


if __name__ == '__main__':
    main()
