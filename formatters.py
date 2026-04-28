def format_3sig(val):
    if val in (None, ""):
        return "--"
    try:
        val = float(val)
    except Exception:
        return "--"
    if val == 0:
        return "0"
    abs_val = abs(val)
    if abs_val >= 100:
        res = f"{val:.0f}"
    elif abs_val >= 10:
        res = f"{val:.1f}"
    else:
        res = f"{val:.2f}"
    if "." in res:
        res = res.rstrip("0").rstrip(".")
    return res


def format_percent(val):
    if val in (None, ""):
        return "--"
    return f"{format_3sig(float(val) * 100)}%"


def format_money(val):
    if val in (None, ""):
        return "--"
    try:
        val = float(val)
    except Exception:
        return "--"
    if val == 0:
        return "0"
    abs_val = abs(val)
    if abs_val >= 1e12:
        return format_3sig(val / 1e12) + "T"
    if abs_val >= 1e9:
        return format_3sig(val / 1e9) + "B"
    if abs_val >= 1e6:
        return format_3sig(val / 1e6) + "M"
    return format_3sig(val)


def parse_abbrev_to_raw(value):
    if not value or value == "--":
        return 0.0
    s = str(value).strip().upper().replace(",", "")
    mult = 1.0
    if s.endswith("T"):
        mult = 1e12
        s = s[:-1]
    elif s.endswith("B"):
        mult = 1e9
        s = s[:-1]
    elif s.endswith("M"):
        mult = 1e6
        s = s[:-1]
    elif s.endswith("K"):
        mult = 1e3
        s = s[:-1]
    try:
        return float(s) * mult
    except Exception:
        return 0.0


def parse_money_to_raw(value):
    if value in (None, "", "--"):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return parse_abbrev_to_raw(value)
