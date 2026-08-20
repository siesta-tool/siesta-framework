#!/usr/bin/env python3
"""
Build a single self-contained, interactive HTML viewer from a declarative-rules CSV.

Usage:
    python3 make_rules_html.py <csv_file> [template ...] [--countries PATH] [--no-country-filter]

Arguments:
    csv_file     Path to the rules CSV. Expected columns (by header name):
                 template, source, target, support  (required)
                 trace_ids                           (optional -> adds "# Traces" column)
    template...  Optional list of templates to INCLUDE (e.g. choice coexistence).
                 If omitted, all templates found in the file are included.

Options:
    --countries PATH      Country-name list, one per line (default: countries.txt
                          next to this script).
    --no-country-filter   Keep cross-country rules that would otherwise be dropped.

Country filtering:
    Events scoped to a country carry the country name somewhere in the label,
    usually followed by an attribute-value marker after a '-' or '_' separator,
    e.g. "Country Legal Registration - South Korea_no" or
    "Country Distribution Start Date - Austria-Friday".
    Two different countries are mutually exclusive, so a rule relating one to
    another carries no information and is dropped. Rules where the two events
    share a country, or where at most one event is country-scoped, are kept.

Output:
    <csv_stem>_rules[.<tmpl>...].html  in the same directory as the CSV.

The generated HTML embeds all data (labels dictionary-encoded for size) and offers:
  - Template filter (dropdown)
  - Source event / Target event filters (dropdowns of discrete event types)
  - Support lower/upper bound
  - Free-text search over source/target
  - Click-to-sort on every column, pagination, reset.
Trace-ID lists are summarised as a count (embedding the raw lists would make the
file as large as the source CSV and unopenable in a browser).
"""
import csv
import json
import os
import re
import sys

csv.field_size_limit(sys.maxsize)

DEFAULT_COUNTRIES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "countries.txt")


def die(msg):
    sys.stderr.write("error: " + msg + "\n")
    sys.exit(1)


def load_countries(path):
    if not os.path.isfile(path):
        die("country list not found: %s\n"
            "       pass --countries PATH, or --no-country-filter to skip filtering" % path)
    with open(path, encoding="utf-8") as f:
        names = set(line.strip() for line in f if line.strip())
    if not names:
        die("country list is empty: " + path)
    return names


def country_matcher(countries):
    """Compile a regex that finds a country name anywhere in a label.

    The country name is no longer guaranteed to sit at the tail after " - ": it
    may be followed by an attribute-value marker (e.g. "-Friday", "-no", "_no"),
    so we scan the whole label. Names are matched longest-first so the most
    specific wins ("Guinea-Bissau" over "Guinea", "South Sudan" over "Sudan"),
    and only on letter boundaries so a name embedded in a longer word is ignored
    ("Oman" inside "Romania", "India" inside "British Indian Ocean Territory").
    """
    letter = r"[^\W\d_]"  # any Unicode letter (word char that isn't a digit / _)
    alt = "|".join(re.escape(n) for n in sorted(countries, key=len, reverse=True))
    return re.compile(r"(?<!%s)(?:%s)(?!%s)" % (letter, alt, letter))


def country_of(label, country_rx):
    """Country this event is scoped to, or None if it is country-agnostic.

    country_rx comes from country_matcher(). "Country Legal Registration -
    South Korea_no" and "Country Distribution Start Date - Austria-Friday" both
    resolve to their country; the trailing marker is not part of the name.
    """
    m = country_rx.search(label)
    return m.group(0) if m else None


def rule_of(label):
    """The rule part of a label: everything before the '§' attribute-value marker.

    Labels encode "rule§value" (e.g. "A§somevalue"); the text after '§' is an
    attribute value, not part of the rule's identity. A rule relating an event
    to another event of the same rule (e.g. source "A§x", target "A§y") carries
    no information and is dropped. Labels without a '§' are their own rule.
    """
    return label.split("§", 1)[0].strip()


def parse_args(args):
    countries_path, country_filter, negation_filter, positional = DEFAULT_COUNTRIES, True, True, []
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--no-country-filter":
            country_filter = False
        elif a == "--no-negation-filter":
            negation_filter = False
        elif a == "--countries":
            i += 1
            if i == len(args):
                die("--countries needs a path")
            countries_path = args[i]
        elif a.startswith("--countries="):
            countries_path = a.split("=", 1)[1]
        elif a.startswith("-"):
            die("unknown option: " + a)
        else:
            positional.append(a)
        i += 1
    return countries_path, country_filter, negation_filter, positional


def main(argv):
    args = argv[1:]
    if not args or args[0] in ("-h", "--help"):
        sys.stderr.write(__doc__)
        sys.exit(0 if args else 1)

    countries_path, country_filter, negation_filter, positional = parse_args(args)
    if not positional:
        die("no csv file given")

    src = positional[0]
    wanted = [t.strip() for t in positional[1:] if t.strip()]
    wanted_set = set(wanted) if wanted else None

    if not os.path.isfile(src):
        die("file not found: " + src)

    countries = load_countries(countries_path) if country_filter else None
    country_rx = country_matcher(countries) if countries else None

    # --- locate columns by header name ---
    with open(src, newline="") as f:
        header = next(csv.reader(f))
    col = {name: i for i, name in enumerate(header)}
    for required in ("template", "source", "target", "support"):
        if required not in col:
            die("missing required column '%s' (have: %s)" % (required, ", ".join(header)))
    ti, si, gi, pi = col["template"], col["source"], col["target"], col["support"]
    has_traces = "trace_ids" in col
    ri = col.get("trace_ids")
    has_conf = "confidence" in col
    ci = col.get("confidence", -1)
    has_int = "interest" in col
    ii = col.get("interest", -1)

    # --- stream rows, dictionary-encode labels, discover templates ---
    labels = []
    lab_idx = {}

    def L(s):
        i = lab_idx.get(s)
        if i is None:
            i = len(labels)
            lab_idx[s] = i
            labels.append(s)
        return i

    tmpl_names = []      # ordered list of template names -> index
    tmpl_idx = {}

    def T(name):
        i = tmpl_idx.get(name)
        if i is None:
            i = len(tmpl_names)
            tmpl_idx[name] = i
            tmpl_names.append(name)
        return i

    rows = []
    seen_templates = set()
    cross_country = 0
    same_rule = 0
    with open(src, newline="") as f:
        r = csv.reader(f)
        next(r)
        for row in r:
            tmpl = row[ti].strip()
            seen_templates.add(tmpl)
            if wanted_set is not None and tmpl not in wanted_set:
                continue
            source, target = row[si].strip(), row[gi].strip()
            if rule_of(source) == rule_of(target):
                same_rule += 1
                continue
            if country_rx is not None:
                cs, ct = country_of(source, country_rx), country_of(target, country_rx)
                if cs is not None and ct is not None and cs != ct:
                    cross_country += 1
                    continue
            ntr = 0
            if has_traces:
                tv = row[ri]
                ntr = 0 if tv == "" else tv.count("|") + 1
            conf = float(row[ci]) if has_conf and row[ci] != "" else 0.0
            interest = float(row[ii]) if has_int and row[ii] != "" else 0.0
            # labels keep the raw "rule§value" form; the viewer splits on '§'
            # for the rule/value filters and displays the marker as " = "
            rows.append([T(tmpl), L(source), L(target), float(row[pi]), conf, interest, ntr])

    if not rows:
        if cross_country:
            die("every matching row related two different countries (%d dropped). "
                "Re-run with --no-country-filter to keep them." % cross_country)
        if wanted_set is not None:
            die("no rows matched template filter %s. Templates present: %s"
                % (sorted(wanted_set), sorted(seen_templates)))
        die("no data rows found in " + src)

    if wanted_set is not None:
        unknown = wanted_set - seen_templates
        if unknown:
            sys.stderr.write("warning: requested template(s) not found in file: %s\n"
                             % sorted(unknown))

    data_json = json.dumps({"labels": labels, "rows": rows, "tmpl": tmpl_names},
                           separators=(",", ":"))

    # --- output filename ---
    stem = os.path.splitext(os.path.basename(src))[0]
    suffix = ("_" + "_".join(wanted)) if wanted else ""
    out = os.path.join(os.path.dirname(os.path.abspath(src)), stem + "_rules" + suffix + ".html")

    html = (HTML_TEMPLATE
            .replace("__SRC__", os.path.basename(src))
            .replace("__HASTR__", "true" if has_traces else "false")
            .replace("__HASCONF__", "true" if has_conf else "false")
            .replace("__HASINT__", "true" if has_int else "false")
            .replace("__DATA__", data_json))
    with open(out, "w") as f:
        f.write(html)

    print("rows: %d  templates: %s  labels: %d" % (len(rows), tmpl_names, len(labels)))
    if same_rule:
        print("dropped %d same-rule rows (source/target share the rule before '§')" % same_rule)
    if country_rx is not None:
        print("dropped %d cross-country rows (%.1f%% of %d matching)"
              % (cross_country, 100.0 * cross_country / (len(rows) + cross_country),
                 len(rows) + cross_country))
    print("wrote %s (%d bytes)" % (out, os.path.getsize(out)))


HTML_TEMPLATE = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Declarative Rules &mdash; __SRC__</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}

:root{
  --bg:#f7f8fb;--surface:#fff;--surface2:#f0f2f6;
  --bd:rgba(0,0,0,.07);--bd-em:rgba(0,0,0,.13);
  --text:#111827;--text-sub:#4b5563;--text-muted:#9ca3af;
  --accent:#1d9e75;--accent-bg:rgba(29,158,117,.10);
  --pill-bg:#e8eaf0;--pill-txt:#4b5563;
  --font:'Inter',system-ui,sans-serif;
  --mono:'JetBrains Mono','Consolas',monospace;
  --r:8px
}
@media(prefers-color-scheme:dark){
  :root{
    --bg:#090d16;--surface:#111827;--surface2:#1a2235;
    --bd:rgba(255,255,255,.07);--bd-em:rgba(255,255,255,.13);
    --text:#e2e8f0;--text-sub:#94a3b8;--text-muted:#4a5568;
    --accent:#1d9e75;--accent-bg:rgba(29,158,117,.15);
    --pill-bg:#1a2235;--pill-txt:#7f9ab8
  }
}

body{background:var(--bg);color:var(--text);font-family:var(--font);font-size:14px;line-height:1.5}

/* ── header ─────────────────────────────────────────────────────── */
header{padding:14px 24px;border-bottom:1px solid var(--bd-em);background:var(--surface)}
h1{margin:0;font-size:11px;font-weight:600;letter-spacing:.1em;text-transform:uppercase;color:var(--accent)}
.sub{color:var(--text-muted);font-size:13px;margin-top:5px}
.sub #total{color:var(--accent);font-weight:500;font-variant-numeric:tabular-nums}

/* ── controls ───────────────────────────────────────────────────── */
.controls{display:flex;flex-wrap:wrap;gap:16px;align-items:flex-end;padding:16px 24px;background:var(--surface2);border-bottom:1px solid var(--bd-em)}
.ctrl{display:flex;flex-direction:column;gap:5px}
.ctrl label{font-size:11px;font-weight:500;color:var(--text-muted);text-transform:uppercase;letter-spacing:.08em}
select,input{background:var(--surface);color:var(--text);border:1px solid var(--bd-em);border-radius:var(--r);font-family:var(--font);font-size:13px;padding:6px 10px;outline:none;transition:border-color .15s}
select:focus,input:focus{border-color:var(--accent)}
input::placeholder{color:var(--text-muted)}
input[type=number]{width:120px;font-family:var(--mono);font-size:12px}
input[type=text]{width:220px}
select.evt{max-width:260px}
button{background:var(--accent);color:#fff;border:0;border-radius:var(--r);padding:8px 14px;font-size:13px;font-family:var(--font);cursor:pointer;transition:opacity .15s}
button:hover{opacity:.9}
button.sec{background:var(--surface);border:1px solid var(--bd-em);color:var(--text-sub);transition:border-color .15s,color .15s}
button.sec:hover{border-color:var(--accent);color:var(--accent);opacity:1}

.count{padding:12px 24px;color:var(--text-muted);font-size:13px}

/* ── table ──────────────────────────────────────────────────────── */
.wrap{padding:0 24px 40px}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:9px 12px;border-bottom:1px solid var(--bd);vertical-align:top}
th{position:sticky;top:0;z-index:10;background:var(--surface2);cursor:pointer;user-select:none;white-space:nowrap;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--text-sub)}
th .arrow{color:var(--accent);font-size:11px}
tbody tr{transition:background .1s}
tbody tr:hover{background:var(--surface2)}
td.sup,td.num{font-family:var(--mono);font-variant-numeric:tabular-nums;white-space:nowrap;color:var(--text-sub)}

/* ── template tags ──────────────────────────────────────────────── */
.tag{display:inline-block;padding:2px 9px;border-radius:20px;font-size:12px;font-family:var(--mono);background:var(--pill-bg);color:var(--pill-txt)}
.tg0{background:rgba(29,158,117,.15);color:#1d9e75}
.tg1{background:rgba(92,124,250,.15);color:#5c7cfa}
.tg2{background:rgba(230,126,34,.15);color:#e67e22}
.tg3{background:rgba(192,57,43,.16);color:#c0392b}
.tg4{background:rgba(142,68,173,.16);color:#8e44ad}
.tg5{background:rgba(41,128,185,.16);color:#2980b9}
.tg6{background:rgba(211,84,0,.15);color:#d35400}
.tg7{background:rgba(22,160,133,.15);color:#16a085}

/* ── pager ──────────────────────────────────────────────────────── */
.pager{display:flex;gap:10px;align-items:center;padding:12px 24px;flex-wrap:wrap}
.pager button{background:var(--surface);border:1px solid var(--bd-em);border-radius:var(--r);color:var(--text-sub);font-family:var(--font);font-size:13px;padding:6px 14px;cursor:pointer;transition:border-color .15s,color .15s}
.pager button:hover:not(:disabled){border-color:var(--accent);color:var(--accent);opacity:1}
.pager button:disabled{opacity:.35;cursor:default}
.pager span{font-size:13px;color:var(--text-muted)}
.pager input{width:70px;text-align:center;background:var(--surface);border:1px solid var(--bd-em);border-radius:var(--r);color:var(--text);font-family:var(--mono);font-size:12px;padding:5px 6px}
</style>
</head>
<body>
<header>
<h1>Declarative Rules</h1>
<div class="sub">Source: __SRC__ &middot; <span id="total"></span> rules</div>
</header>
<div class="controls">
  <div class="ctrl">
    <label>Template</label>
    <select id="tmpl" class="evt"><option value="-1">All</option></select>
  </div>
  <div class="ctrl">
    <label>Source rule</label>
    <input id="srcRule" class="evt" list="srcRuleList" placeholder="All — type to search" autocomplete="off">
    <datalist id="srcRuleList"></datalist>
  </div>
  <div class="ctrl">
    <label>Source value</label>
    <input id="srcVal" class="evt" list="srcValList" placeholder="Any value — type to search" autocomplete="off">
    <datalist id="srcValList"></datalist>
  </div>
  <div class="ctrl">
    <label>Target rule</label>
    <input id="tgtRule" class="evt" list="tgtRuleList" placeholder="All — type to search" autocomplete="off">
    <datalist id="tgtRuleList"></datalist>
  </div>
  <div class="ctrl">
    <label>Target value</label>
    <input id="tgtVal" class="evt" list="tgtValList" placeholder="Any value — type to search" autocomplete="off">
    <datalist id="tgtValList"></datalist>
  </div>
  <div class="ctrl">
    <label>Support &ge; (lower bound)</label>
    <input type="number" id="low" step="any" placeholder="min">
  </div>
  <div class="ctrl">
    <label>Support &le; (upper bound)</label>
    <input type="number" id="high" step="any" placeholder="max">
  </div>
  <div class="ctrl confctrl">
    <label>Confidence &ge; (lower bound)</label>
    <input type="number" id="clow" step="any" placeholder="min">
  </div>
  <div class="ctrl confctrl">
    <label>Confidence &le; (upper bound)</label>
    <input type="number" id="chigh" step="any" placeholder="max">
  </div>
  <div class="ctrl intctrl">
    <label>Interest &ge; (lower bound)</label>
    <input type="number" id="ilow" step="any" placeholder="min">
  </div>
  <div class="ctrl intctrl">
    <label>Interest &le; (upper bound)</label>
    <input type="number" id="ihigh" step="any" placeholder="max">
  </div>
  <div class="ctrl">
    <label>Search source / target</label>
    <input type="text" id="q" placeholder="text filter">
  </div>
  <div class="ctrl">
    <label>Rows per page</label>
    <select id="pp"><option>100</option><option>250</option><option>500</option><option>1000</option></select>
  </div>
  <div class="ctrl">
    <label>&nbsp;</label>
    <button class="sec" id="reset">Reset</button>
  </div>
</div>
<div class="count" id="count"></div>
<div class="wrap">
<table>
<thead><tr id="head"></tr></thead>
<tbody id="tbody"></tbody>
</table>
</div>
<div class="pager">
  <button id="first">&laquo;</button>
  <button id="prev">&lsaquo; Prev</button>
  <span>Page <input type="number" id="page" min="1" value="1"> / <span id="pages">1</span></span>
  <button id="next">Next &rsaquo;</button>
  <button id="last">&raquo;</button>
</div>
<script id="data" type="application/json">__DATA__</script>
<script>
const D=JSON.parse(document.getElementById('data').textContent);
const LAB=D.labels, ROWS=D.rows, TNAME=D.tmpl;
const HAS_TRACES=__HASTR__;
const HAS_CONF=__HASCONF__;
const HAS_INT=__HASINT__;
// labels are stored "rule§value"; the value is the attribute value after the
// first § marker. Split once per label so the filter loop and renderer never
// re-parse. Display shows the marker as " = ".
const LAB_DISP=[], LAB_DISP_LC=[], LAB_RULE=[], LAB_RULE_LC=[], LAB_VAL=[], LAB_VAL_LC=[];
for(const s of LAB){
  const i=s.indexOf('§');
  const rl=(i===-1?s:s.slice(0,i)).trim();
  const vl=(i===-1?'':s.slice(i+1)).trim();
  const disp=s.replace('§',' = ');
  LAB_DISP.push(disp); LAB_DISP_LC.push(disp.toLowerCase());
  LAB_RULE.push(rl); LAB_RULE_LC.push(rl.toLowerCase());
  LAB_VAL.push(vl); LAB_VAL_LC.push(vl.toLowerCase());
}
document.getElementById('total').textContent=ROWS.length.toLocaleString();
if(!HAS_CONF) document.querySelectorAll('.confctrl').forEach(e=>e.style.display='none');
if(!HAS_INT) document.querySelectorAll('.intctrl').forEach(e=>e.style.display='none');
let sortCol=3, sortDir=-1;
let view=[];
let perPage=100, curPage=1;
const $=id=>document.getElementById(id);
function esc(s){return s.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}
function escAttr(s){return s.replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}

// header columns
(function buildHead(){
  let cols=[['Template',0],['Source',1],['Target',2],['Support',3]];
  if(HAS_CONF) cols.push(['Confidence',4]);
  if(HAS_INT) cols.push(['Interest',5]);
  if(HAS_TRACES) cols.push(['# Traces',6]);
  $('head').innerHTML=cols.map(c=>'<th data-c="'+c[1]+'">'+c[0]+' <span class="arrow" data-a="'+c[1]+'"></span></th>').join('');
})();

// lowercased option sets per field, used to tell an exact pick from free text
const srcRuleSet=new Set(), srcValSet=new Set(), tgtRuleSet=new Set(), tgtValSet=new Set();
// populate Template <select> and the rule/value search lists from values present
(function fillDropdowns(){
  const tSet=new Set(), srcI=new Set(), tgtI=new Set();
  for(const r of ROWS){tSet.add(r[0]);srcI.add(r[1]);tgtI.add(r[2]);}
  // templates: plain <select>
  const tArr=[...tSet].map(i=>[TNAME[i],i]).sort((a,b)=>a[0]<b[0]?-1:(a[0]>b[0]?1:0));
  $('tmpl').innerHTML='<option value="-1">All ('+tArr.length+')</option>'+
    tArr.map(([n,i])=>'<option value="'+i+'">'+esc(n)+'</option>').join('');
  // distinct non-empty strings across a side's label indices, via a lookup array
  function strs(idxSet,arr){
    const s=new Set();
    for(const i of idxSet){const v=arr[i];if(v!=='')s.add(v);}
    return [...s].sort((a,b)=>a<b?-1:(a>b?1:0));
  }
  // searchable <input> backed by <datalist>; record lowercased options for exact match
  function fillList(listEl,inputEl,arr,knownLC){
    listEl.innerHTML=arr.map(n=>'<option value="'+escAttr(n)+'"></option>').join('');
    for(const n of arr) knownLC.add(n.toLowerCase());
    inputEl.placeholder='All '+arr.length+' — type to search';
  }
  fillList($('srcRuleList'),$('srcRule'),strs(srcI,LAB_RULE),srcRuleSet);
  fillList($('srcValList'),$('srcVal'),strs(srcI,LAB_VAL),srcValSet);
  fillList($('tgtRuleList'),$('tgtRule'),strs(tgtI,LAB_RULE),tgtRuleSet);
  fillList($('tgtValList'),$('tgtVal'),strs(tgtI,LAB_VAL),tgtValSet);
})();

// resolve a search input to a filter: null=all, {exact:text} pick, {sub:text} substring
function fieldFilter(inputEl,knownLC){
  const v=inputEl.value.trim().toLowerCase();
  if(v==='') return null;
  return knownLC.has(v)?{exact:v}:{sub:v};
}
// test a precomputed lowercased field value against a filter
function match(f,lc){
  if(!f) return true;
  return f.exact!==undefined ? lc===f.exact : lc.indexOf(f.sub)!==-1;
}

function applyFilters(){
  const t=parseInt($('tmpl').value);
  const srf=fieldFilter($('srcRule'),srcRuleSet);
  const svf=fieldFilter($('srcVal'),srcValSet);
  const trf=fieldFilter($('tgtRule'),tgtRuleSet);
  const tvf=fieldFilter($('tgtVal'),tgtValSet);
  const lowv=$('low').value, highv=$('high').value;
  const low=lowv===''?-Infinity:parseFloat(lowv);
  const high=highv===''?Infinity:parseFloat(highv);
  const clowv=$('clow').value, chighv=$('chigh').value;
  const clow=clowv===''?-Infinity:parseFloat(clowv);
  const chigh=chighv===''?Infinity:parseFloat(chighv);
  const ilowv=$('ilow').value, ihighv=$('ihigh').value;
  const ilow=ilowv===''?-Infinity:parseFloat(ilowv);
  const ihigh=ihighv===''?Infinity:parseFloat(ihighv);
  const q=$('q').value.trim().toLowerCase();
  view=[];
  for(let i=0;i<ROWS.length;i++){
    const r=ROWS[i];
    if(t!==-1 && r[0]!==t) continue;
    if(!match(srf,LAB_RULE_LC[r[1]])) continue;
    if(!match(svf,LAB_VAL_LC[r[1]])) continue;
    if(!match(trf,LAB_RULE_LC[r[2]])) continue;
    if(!match(tvf,LAB_VAL_LC[r[2]])) continue;
    if(r[3]<low || r[3]>high) continue;
    if(r[4]<clow || r[4]>chigh) continue;
    if(r[5]<ilow || r[5]>ihigh) continue;
    if(q){
      if(LAB_DISP_LC[r[1]].indexOf(q)===-1 && LAB_DISP_LC[r[2]].indexOf(q)===-1) continue;
    }
    view.push(r);
  }
  sortView(); curPage=1; render();
}
function sortView(){
  const c=sortCol, d=sortDir;
  view.sort((a,b)=>{
    let av,bv;
    if(c===0){av=TNAME[a[0]];bv=TNAME[b[0]];}
    else if(c>=3){av=a[c];bv=b[c];}
    else {av=LAB_DISP[a[c]];bv=LAB_DISP[b[c]];}
    if(av<bv)return -1*d; if(av>bv)return 1*d; return 0;
  });
}
function render(){
  const pages=Math.max(1,Math.ceil(view.length/perPage));
  if(curPage>pages)curPage=pages;
  $('pages').textContent=pages; $('page').value=curPage; $('page').max=pages;
  const start=(curPage-1)*perPage;
  const slice=view.slice(start,start+perPage);
  let h='';
  for(const r of slice){
    h+='<tr><td><span class="tag tg'+(r[0]%8)+'">'+esc(TNAME[r[0]])+'</span></td><td>'+esc(LAB_DISP[r[1]])+'</td><td>'+esc(LAB_DISP[r[2]])+'</td><td class="sup">'+fmt(r[3])+'</td>'+(HAS_CONF?'<td class="sup">'+fmt(r[4])+'</td>':'')+(HAS_INT?'<td class="sup">'+fmt(r[5])+'</td>':'')+(HAS_TRACES?'<td class="num">'+r[6].toLocaleString()+'</td>':'')+'</tr>';
  }
  $('tbody').innerHTML=h;
  $('count').textContent=view.length.toLocaleString()+' rules match'+(view.length?'  (showing '+(start+1)+'–'+(start+slice.length)+')':'');
  document.querySelectorAll('.arrow').forEach(a=>{
    a.textContent=(parseInt(a.dataset.a)===sortCol)?(sortDir===1?'▲':'▼'):'';
  });
}
function fmt(v){
  if(v!==0 && Math.abs(v)<1e-3) return v.toExponential(4);
  return (Math.round(v*1e6)/1e6).toString();
}
function bindHead(){
  document.querySelectorAll('th').forEach(th=>{
    th.addEventListener('click',()=>{
      const c=parseInt(th.dataset.c);
      if(sortCol===c) sortDir=-sortDir; else {sortCol=c; sortDir=(c>=3)?-1:1;}
      sortView(); render();
    });
  });
}
bindHead();
['tmpl','srcRule','srcVal','tgtRule','tgtVal','low','high','clow','chigh','ilow','ihigh','q'].forEach(id=>$(id).addEventListener('input',applyFilters));
$('pp').addEventListener('change',()=>{perPage=parseInt($('pp').value);curPage=1;render();});
$('reset').addEventListener('click',()=>{
  $('tmpl').value='-1';$('srcRule').value='';$('srcVal').value='';$('tgtRule').value='';$('tgtVal').value='';$('low').value='';$('high').value='';$('clow').value='';$('chigh').value='';$('ilow').value='';$('ihigh').value='';$('q').value='';
  sortCol=3;sortDir=-1;applyFilters();
});
$('first').onclick=()=>{curPage=1;render();};
$('prev').onclick=()=>{if(curPage>1){curPage--;render();}};
$('next').onclick=()=>{curPage++;render();};
$('last').onclick=()=>{curPage=Math.ceil(view.length/perPage);render();};
$('page').addEventListener('change',()=>{curPage=Math.max(1,parseInt($('page').value)||1);render();});
applyFilters();
</script>
</body>
</html>'''


if __name__ == "__main__":
    main(sys.argv)
