#!/usr/bin/env python3
"""
Build a single self-contained, interactive HTML viewer from a declarative-rules CSV.

Usage:
    python3 make_rules_html.py <csv_file> [template ...] [--countries PATH] [--no-country-filter]

Arguments:
    csv_file     Path to the rules CSV. Expected columns (by header name):
                 template, source, target, support  (required)
                 trace_ids                           (optional -> adds a "Traces" column)
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
Trace-ID lists are NOT embedded - on a real export they are most of the CSV. The
report instead records, per row, the byte span its ids occupy in the CSV, and
each row shows only a trace count. Expanding a row reads just that row's few
hundred bytes out of the CSV, which the viewer asks for once per session (a
browser cannot open a path on its own, so the file is picked, not configured);
the file's size is checked against the report so stale offsets cannot be read.
"""
import csv
import io
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
        raise ValueError(
            "country list not found: %s "
            "(pass --countries PATH, or --no-country-filter to skip filtering)" % path
        )
    with open(path, encoding="utf-8") as f:
        names = set(line.strip() for line in f if line.strip())
    if not names:
        raise ValueError("country list is empty: " + path)
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


def build_rules_html(header, rows, *, src_name="rules.csv", wanted=None,
                      country_filter=True, countries_path=DEFAULT_COUNTRIES,
                      traces=None):
    """Build the self-contained rules HTML viewer from an already-parsed rules table.

    header/rows mirror csv.reader() output (rows are lists of raw field strings),
    so a caller that already holds the rules table in memory (e.g. an API handler
    piping mining output straight through) can build the report without writing
    it to a file and reading it back.

    traces, when given, turns on the on-demand Traces column: {"file": csv
    basename, "size": its size in bytes, "spans": the flat offset/length/count
    triples read_rules_csv returns}. The ids themselves stay in the CSV - the
    viewer asks for the file and reads the few hundred bytes each row needs. rows
    must then be a sized sequence, since the spans are matched to it positionally.

    Returns (html, stats) where stats reports what was kept/dropped.
    Raises ValueError on bad input (missing columns, no matching rows, ...).
    """
    wanted = [t.strip() for t in wanted if t.strip()] if wanted else []
    wanted_set = set(wanted) if wanted else None

    countries = load_countries(countries_path) if country_filter else None
    country_rx = country_matcher(countries) if countries else None

    # --- locate columns by header name ---
    col = {name: i for i, name in enumerate(header)}
    for required in ("template", "source", "target", "support"):
        if required not in col:
            raise ValueError("missing required column '%s' (have: %s)" % (required, ", ".join(header)))
    ti, si, gi, pi = col["template"], col["source"], col["target"], col["support"]
    # trace ids are never embedded - on a real export they dwarf everything else
    # in the payload. When the caller hands over byte spans (read_rules_csv builds
    # them), the viewer reads each row's ids straight out of the CSV on demand.
    span_arr = traces["spans"] if traces else None
    has_traces = span_arr is not None
    if has_traces and len(span_arr) != 3 * len(rows):
        raise ValueError("trace spans (%d) do not match the row count (%d)"
                         % (len(span_arr) // 3, len(rows)))
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

    out_rows = []
    tr_off, tr_len, tr_n = [], [], []   # kept rows' trace spans, parallel to out_rows
    seen_templates = set()
    cross_country = 0
    same_rule = 0
    for ri, row in enumerate(rows):
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
        conf = float(row[ci]) if has_conf and row[ci] != "" else 0.0
        interest = float(row[ii]) if has_int and row[ii] != "" else 0.0
        if has_traces:
            b = 3 * ri
            tr_off.append(span_arr[b]); tr_len.append(span_arr[b + 1]); tr_n.append(span_arr[b + 2])
        # labels keep the raw "rule§value" form; the viewer splits on '§'
        # for the rule/value filters and displays the marker as " = "
        out_rows.append([T(tmpl), L(source), L(target), float(row[pi]), conf, interest])

    if not out_rows:
        if cross_country:
            raise ValueError(
                "every matching row related two different countries (%d dropped). "
                "Re-run with country_filter disabled to keep them." % cross_country
            )
        if wanted_set is not None:
            raise ValueError(
                "no rows matched template filter %s. Templates present: %s"
                % (sorted(wanted_set), sorted(seen_templates))
            )
        raise ValueError("no data rows found in " + src_name)

    unknown_templates = sorted(wanted_set - seen_templates) if wanted_set is not None else []

    payload = {"labels": labels, "rows": out_rows, "tmpl": tmpl_names}
    if has_traces:
        # offsets are delta-encoded (kept rows stay in file order, so the gaps are
        # small numbers); the viewer prefix-sums them once on load. Rows and these
        # arrays are built together, so position i in one is position i in the other.
        prev = 0
        for k in range(len(tr_off)):
            tr_off[k], prev = tr_off[k] - prev, tr_off[k]
        payload["traces"] = {"file": traces.get("file", ""), "size": traces.get("size", 0),
                             "off": tr_off, "len": tr_len, "n": tr_n}
    data_json = json.dumps(payload, separators=(",", ":"))

    html = (HTML_TEMPLATE
            .replace("__SRC__", src_name)
            .replace("__HASTR__", "true" if has_traces else "false")
            .replace("__HASCONF__", "true" if has_conf else "false")
            .replace("__HASINT__", "true" if has_int else "false")
            .replace("__DATA__", data_json))

    stats = {
        "rows": len(out_rows),
        "templates": tmpl_names,
        "labels": len(labels),
        "dropped_same_rule": same_rule,
        "dropped_cross_country": cross_country if country_rx is not None else None,
        "unknown_templates": unknown_templates,
    }
    return html, stats


def _cut_traces(f, off, spans):
    """Yield each line with its trace field removed, recording where that field sat.

    Feeds csv.reader line by line so the trace ids are never parsed, appending
    (byte offset, byte length, id count) to `spans` for each line as it goes.
    """
    for raw in f:
        head, sep, tail = raw.rpartition(b",")
        beg = off
        off += len(raw)
        if not sep:            # blank line: nothing to cut, nothing to index
            continue
        tail = tail.rstrip(b"\r\n")
        # point the span at the ids themselves, not at a writer's quotes around
        # them (a quoted field holding a comma trips the width check instead)
        lead = 1 if len(tail) > 1 and tail[:1] == b'"' == tail[-1:] else 0
        if lead:
            tail = tail[1:-1]
        spans.append(beg + len(head) + 1 + lead)
        spans.append(len(tail))
        spans.append(tail.count(b"|") + 1 if tail else 0)
        yield head.decode("utf-8")


def read_rules_csv(f):
    """Parse a rules CSV opened in binary into (header, rows, spans).

    Trace-id lists are most of the bytes in a mining export and are never
    embedded in the report, so when that column sits last the field is cut off
    each raw line and the csv parser never builds the string. On a 196 MB export
    this reads in 1.1s / 102 MB peak, against 4.7s / 298 MB parsing the column and
    5.2s / 112 MB deleting it from each row afterwards.

    spans is a flat list of (offset, length, id count) per row - where that row's
    ids live in this exact file - for build_rules_html to hand to the viewer, or
    None when there is no trace column (or the cut had to be abandoned).

    The cut assumes the trace field holds no comma of its own. Rather than trust
    that, the result is checked for rows that came out the wrong width, and a
    file that fails the check is re-read with a plain parse.
    """
    header_line = f.readline()
    header = next(csv.reader([header_line.decode("utf-8")]))

    def plain():
        # a full csv parse, which unlike the cut copes with quoted commas and
        # newlines anywhere in the row
        f.seek(0)
        text = io.TextIOWrapper(f, encoding="utf-8", newline="")
        r = csv.reader(text)
        next(r)
        return header, list(r), None

    if not header or header[-1] != "trace_ids" or not f.seekable():
        return plain()

    stripped = header[:-1]
    width = len(stripped)
    spans = []
    rows = list(csv.reader(_cut_traces(f, len(header_line), spans)))
    if len(spans) == 3 * len(rows) and all(len(row) == width for row in rows):
        return stripped, rows, spans
    return plain()


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

    if not os.path.isfile(src):
        die("file not found: " + src)

    with open(src, "rb") as f:
        header, rows, spans = read_rules_csv(f)

    # the ids stay where they are; the viewer is told which file to ask for and
    # how big it should be, so a regenerated CSV can't be read with stale offsets
    traces = None
    if spans is not None:
        traces = {"file": os.path.basename(src), "size": os.path.getsize(src), "spans": spans}

    try:
        html, stats = build_rules_html(
            header, rows,
            src_name=os.path.basename(src),
            wanted=wanted,
            traces=traces,
            country_filter=country_filter,
            countries_path=countries_path,
        )
    except ValueError as e:
        die(str(e))

    # --- output filename ---
    stem = os.path.splitext(os.path.basename(src))[0]
    suffix = ("_" + "_".join(wanted)) if wanted else ""
    out = os.path.join(os.path.dirname(os.path.abspath(src)), stem + "_rules" + suffix + ".html")
    with open(out, "w") as f:
        f.write(html)

    print("rows: %d  templates: %s  labels: %d" % (stats["rows"], stats["templates"], stats["labels"]))
    if stats["dropped_same_rule"]:
        print("dropped %d same-rule rows (source/target share the rule before '§')" % stats["dropped_same_rule"])
    if stats["dropped_cross_country"]:
        cc = stats["dropped_cross_country"]
        print("dropped %d cross-country rows (%.1f%% of %d matching)"
              % (cc, 100.0 * cc / (stats["rows"] + cc), stats["rows"] + cc))
    if stats["unknown_templates"]:
        sys.stderr.write("warning: requested template(s) not found in file: %s\n"
                         % stats["unknown_templates"])
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

input[type=file]{padding:5px 8px;font-size:12px;max-width:260px;cursor:pointer}
input[type=file]::file-selector-button{background:var(--surface2);color:var(--text-sub);border:1px solid var(--bd-em);border-radius:6px;padding:3px 9px;margin-right:8px;font-family:var(--font);font-size:12px;cursor:pointer}
input[type=file]::file-selector-button:hover{border-color:var(--accent);color:var(--accent)}
.hint{font-size:11px;color:var(--text-muted);max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.hint.bad{color:#c0392b}
.hint.ok{color:var(--accent)}

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

/* ── trace expander ─────────────────────────────────────────────── */
.trace-toggle{background:var(--surface2);border:1px solid var(--bd-em);border-radius:20px;padding:2px 10px;font-family:var(--mono);font-size:12px;color:var(--text-sub);cursor:pointer;transition:border-color .15s,color .15s}
.trace-toggle:hover{border-color:var(--accent);color:var(--accent)}
.trace-toggle .chev{display:inline-block;font-size:9px;transition:transform .15s}
.trace-toggle.open .chev{transform:rotate(90deg)}
tr.trace-detail td{background:var(--surface2);padding:10px 14px 14px}
.tracebox-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.tracebox-head span{font-size:11px;font-weight:500;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em}
.copybtn{background:var(--surface);border:1px solid var(--bd-em);border-radius:6px;padding:3px 10px;font-size:12px;font-family:var(--font);color:var(--text-sub);cursor:pointer;transition:border-color .15s,color .15s}
.copybtn:hover{border-color:var(--accent);color:var(--accent)}
.tracelist{max-height:160px;overflow-y:auto;font-family:var(--mono);font-size:12px;line-height:1.6;color:var(--text-sub);white-space:pre-wrap;word-break:break-all;background:var(--surface);border:1px solid var(--bd);border-radius:6px;padding:8px 10px}

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
  <div class="ctrl trctrl">
    <label>Trace ids</label>
    <input type="file" id="csvfile" accept=".csv,text/csv">
    <span class="hint" id="csvstat"></span>
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
// trace ids stay in the CSV rather than being embedded here - on a real export
// they are most of the file. TR holds, per row, where that row's ids sit in that
// exact CSV and how many there are, so expanding a row reads only its own few
// hundred bytes. Offsets arrive delta-encoded; prefix-sum them once, here.
const TR=D.traces||null;
const TR_OFF=TR?TR.off:null, TR_LEN=TR?TR.len:null, TR_N=TR?TR.n:null;
let csvFile=null;
if(TR){
  for(let i=1;i<TR_OFF.length;i++) TR_OFF[i]+=TR_OFF[i-1];
  // rows and the TR arrays are written in the same order, so a row's position
  // in ROWS is its key into them; stash it rather than paying for it in JSON
  for(let i=0;i<ROWS.length;i++) ROWS[i][6]=i;
}
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
let colCount=4;
const $=id=>document.getElementById(id);
function esc(s){return s.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}
function escAttr(s){return s.replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}

// header columns
(function buildHead(){
  let cols=[['Template',0],['Source',1],['Target',2],['Support',3]];
  if(HAS_CONF) cols.push(['Confidence',4]);
  if(HAS_INT) cols.push(['Interest',5]);
  if(HAS_TRACES) cols.push(['Traces',6]);
  colCount=cols.length;
  $('head').innerHTML=cols.map(c=>'<th data-c="'+c[1]+'">'+c[0]+' <span class="arrow" data-a="'+c[1]+'"></span></th>').join('');
})();

// copy text to the clipboard, falling back to a hidden textarea when the
// Clipboard API is unavailable (e.g. a file:// viewer without a secure context)
function copyText(text,btn){
  const done=()=>{const orig=btn.textContent;btn.textContent='Copied';setTimeout(()=>{btn.textContent=orig;},1200);};
  const fallback=()=>{
    const ta=document.createElement('textarea');
    ta.value=text;ta.style.position='fixed';ta.style.opacity='0';
    document.body.appendChild(ta);ta.focus();ta.select();
    try{document.execCommand('copy');done();}catch(e){}
    document.body.removeChild(ta);
  };
  if(navigator.clipboard && navigator.clipboard.writeText){
    navigator.clipboard.writeText(text).then(done).catch(fallback);
  } else fallback();
}

// read one row's ids straight out of the linked CSV - a single slice of that
// row's own bytes, no scan. A slice that comes back holding a comma or a newline
// is not a trace field, which means the file is not the one this report indexed.
function readIds(k){
  const len=TR_LEN[k];
  if(!len) return Promise.resolve([]);
  const blob=csvFile.slice(TR_OFF[k],TR_OFF[k]+len);
  const parse=t=>{
    if(/[\r\n,]/.test(t)) throw new Error('span does not hold a trace field');
    return t?t.split('|'):[];
  };
  if(blob.text) return blob.text().then(parse);
  return new Promise((res,rej)=>{           // pre-2019 browsers
    const fr=new FileReader();
    fr.onload=()=>{try{res(parse(fr.result));}catch(err){rej(err);}};
    fr.onerror=()=>rej(fr.error);
    fr.readAsText(blob);
  });
}

// expand/collapse a row's full trace-id list on demand, so a constraint backed
// by thousands of traces never has to be rendered into the table itself
function bindTraceToggle(){
  $('tbody').addEventListener('click',e=>{
    const btn=e.target.closest('.trace-toggle');
    if(!btn) return;
    const tr=btn.closest('tr');
    const next=tr.nextElementSibling;
    if(next && next.classList.contains('trace-detail')){
      next.remove(); btn.classList.remove('open'); return;
    }
    document.querySelectorAll('tr.trace-detail').forEach(d=>d.remove());
    document.querySelectorAll('.trace-toggle.open').forEach(b=>b.classList.remove('open'));
    const k=parseInt(btn.dataset.tk);
    const det=document.createElement('tr');
    det.className='trace-detail';
    const td=document.createElement('td');
    td.colSpan=colCount;
    td.innerHTML='<div class="tracebox-head"><span>'+TR_N[k].toLocaleString()+' trace id(s)</span>'+
      '<button type="button" class="copybtn" disabled>Copy</button></div><div class="tracelist"></div>';
    const list=td.querySelector('.tracelist'), copy=td.querySelector('.copybtn');
    det.appendChild(td);
    tr.after(det);
    btn.classList.add('open');
    if(!csvFile){ list.textContent='Link '+(TR.file||'the CSV')+' above to load the ids.'; return; }
    list.textContent='Reading…';
    readIds(k).then(ids=>{
      const text=ids.join(', ');
      list.textContent=text;
      copy.disabled=false;
      copy.addEventListener('click',ev=>copyText(text,ev.currentTarget));
    }).catch(()=>{
      list.textContent='Could not read the ids: '+(TR.file||'the CSV')+' is not the file this report was built from.';
    });
  });
}
bindTraceToggle();

// the page cannot open a path by itself - a browser only reads a file the user
// hands it - so the CSV is linked once per session and held for the slices. The
// size check keeps a regenerated CSV from being read with stale offsets.
function setCsvStat(msg,cls){
  const el=$('csvstat'); el.textContent=msg; el.className='hint'+(cls?' '+cls:'');
}
if(HAS_TRACES){
  const idle=()=>setCsvStat('link '+TR.file+' to read ids');
  idle();
  $('csvfile').addEventListener('change',e=>{
    const f=e.target.files&&e.target.files[0];
    csvFile=null;
    document.querySelectorAll('tr.trace-detail').forEach(d=>d.remove());
    document.querySelectorAll('.trace-toggle.open').forEach(b=>b.classList.remove('open'));
    if(!f){ idle(); return; }
    if(TR.size && f.size!==TR.size){
      setCsvStat('not this file: expected '+TR.size.toLocaleString()+' bytes, got '+f.size.toLocaleString(),'bad');
      return;
    }
    csvFile=f;
    setCsvStat('linked '+f.name,'ok');
  });
} else {
  document.querySelectorAll('.trctrl').forEach(e=>e.style.display='none');
}

// lowercased option sets per field, used to tell an exact pick from free text
const srcRuleSet=new Set(), srcValSet=new Set(), tgtRuleSet=new Set(), tgtValSet=new Set();
// distinct label indices seen on each side, kept so the value lists can be
// rebuilt whenever that side's rule filter changes
const SRC_IDX=[], TGT_IDX=[];
// distinct non-empty strings across a side's label indices, via a lookup array;
// when `rf` is given, only labels whose rule passes it contribute
function strs(idxSet,arr,rf){
  const s=new Set();
  for(const i of idxSet){
    if(rf && !match(rf,LAB_RULE_LC[i])) continue;
    const v=arr[i];if(v!=='')s.add(v);
  }
  return [...s].sort((a,b)=>a<b?-1:(a>b?1:0));
}
// searchable <input> backed by <datalist>; record lowercased options for exact match
function fillList(listEl,inputEl,arr,knownLC,noun){
  listEl.innerHTML=arr.map(n=>'<option value="'+escAttr(n)+'"></option>').join('');
  knownLC.clear();
  for(const n of arr) knownLC.add(n.toLowerCase());
  inputEl.placeholder=noun?(arr.length+' '+noun+' — type to search')
                          :('All '+arr.length+' — type to search');
}
// populate Template <select> and the rule/value search lists from values present
(function fillDropdowns(){
  const tSet=new Set(), srcI=new Set(), tgtI=new Set();
  for(const r of ROWS){tSet.add(r[0]);srcI.add(r[1]);tgtI.add(r[2]);}
  SRC_IDX.push(...srcI); TGT_IDX.push(...tgtI);
  // templates: plain <select>
  const tArr=[...tSet].map(i=>[TNAME[i],i]).sort((a,b)=>a[0]<b[0]?-1:(a[0]>b[0]?1:0));
  $('tmpl').innerHTML='<option value="-1">All ('+tArr.length+')</option>'+
    tArr.map(([n,i])=>'<option value="'+i+'">'+esc(n)+'</option>').join('');
  fillList($('srcRuleList'),$('srcRule'),strs(SRC_IDX,LAB_RULE),srcRuleSet);
  fillList($('tgtRuleList'),$('tgtRule'),strs(TGT_IDX,LAB_RULE),tgtRuleSet);
})();

// hold each side's value list to the values that actually occur with the rule
// filtered on that side, so picking an activity narrows its value dropdown
// instead of offering every value in the log. Rebuilt only when the rule text
// changes; a value pick the new rule never takes is dropped rather than left
// behind filtering the table down to nothing.
const lastRule={src:null,tgt:null};
function syncValueLists(){
  for(const side of ['src','tgt']){
    const ruleEl=$(side+'Rule'), key=ruleEl.value.trim().toLowerCase();
    if(lastRule[side]===key) continue;
    lastRule[side]=key;
    const valEl=$(side+'Val');
    const valSet=side==='src'?srcValSet:tgtValSet;
    const rf=fieldFilter(ruleEl,side==='src'?srcRuleSet:tgtRuleSet);
    const cur=valEl.value.trim().toLowerCase();
    const wasPick=valSet.has(cur);
    fillList($(side+'ValList'),valEl,
             strs(side==='src'?SRC_IDX:TGT_IDX,LAB_VAL,rf),valSet,'values');
    if(cur && wasPick && !valSet.has(cur)) valEl.value='';
  }
}

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
  syncValueLists();
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
    else if(c===6){av=TR_N[a[6]];bv=TR_N[b[6]];}
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
  slice.forEach(r=>{
    let traceCell='';
    if(HAS_TRACES){
      const n=TR_N[r[6]];
      traceCell='<td class="num">'+(n?('<button type="button" class="trace-toggle" data-tk="'+r[6]+'">'+n.toLocaleString()+' <span class="chev">&#9656;</span></button>'):'0')+'</td>';
    }
    h+='<tr><td><span class="tag tg'+(r[0]%8)+'">'+esc(TNAME[r[0]])+'</span></td><td>'+esc(LAB_DISP[r[1]])+'</td><td>'+esc(LAB_DISP[r[2]])+'</td><td class="sup">'+fmt(r[3])+'</td>'+(HAS_CONF?'<td class="sup">'+fmt(r[4])+'</td>':'')+(HAS_INT?'<td class="sup">'+fmt(r[5])+'</td>':'')+traceCell+'</tr>';
  });
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
