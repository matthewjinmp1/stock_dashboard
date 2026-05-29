import re

from formatters import format_3sig, format_money, format_percent, parse_money_to_raw


def unwrap_annual(statement):
    s = statement or {}
    if "annual" in s:
        return s["annual"] or {}
    return s


def latest_row_raw(statement, labels):
    flat = unwrap_annual(statement)
    labels_lower = {label.lower() for label in labels}
    for row in flat.get("rows", []):
        if row.get("label", "").lower() in labels_lower:
            for value in row.get("values", []):
                raw = parse_money_to_raw(value)
                if raw:
                    return raw
    return 0.0


def statement_latest_value(statement, labels):
    flat = unwrap_annual(statement)
    labels_lower = {label.lower() for label in labels}
    for row in flat.get("rows", []):
        if row.get("label", "").lower() in labels_lower:
            for value in row.get("values", []):
                if value not in (None, "", "--"):
                    return value
    return "--"


def camel_to_label(key):
    return re.sub(r"(?<!^)(?=[A-Z])", " ", key).replace("And", "and")


def statement_type_name(item):
    meta_type = (item.get("meta", {}) or {}).get("type", [""])
    return meta_type[0] if meta_type else ""


def series_points(item, key):
    points = item.get(key, [])
    out = []
    for idx, point in enumerate(points):
        value = (point.get("reportedValue", {}) or {}).get("raw")
        if value is None:
            continue
        out.append({
            "date": point.get("asOfDate") or f"idx-{idx:04d}",
            "raw": float(value),
        })
    return out


def build_statement_from_timeseries_results(selected_results, type_map, formatter):
    annual_rows = {}
    quarterly_rows = {}
    period_dates = set()
    quarterly_period_dates = set()

    for item in selected_results or []:
        type_name = statement_type_name(item)
        prefix = "annual" if type_name.startswith("annual") else "quarterly" if type_name.startswith("quarterly") else ""
        if not prefix:
            continue
        base_key = type_name[len(prefix):]
        label = type_map.get(base_key)
        if not label:
            continue
        points = series_points(item, type_name)
        if not points:
            continue
        if prefix == "annual":
            if label not in annual_rows:
                annual_rows[label] = sorted(points, key=lambda p: p["date"], reverse=True)
                for point in points:
                    if not point["date"].startswith("idx-"):
                        period_dates.add(point["date"])
        else:
            if label not in quarterly_rows:
                quarterly_rows[label] = sorted(points, key=lambda p: p["date"], reverse=True)
                for point in points:
                    if not point["date"].startswith("idx-"):
                        quarterly_period_dates.add(point["date"])

    sorted_periods = sorted(period_dates, reverse=True)[:4]
    periods = ["TTM"] + sorted_periods
    rows = []

    trailing_map = {
        "TotalRevenue": "trailingTotalRevenue",
        "CostOfRevenue": "trailingCostOfRevenue",
        "GrossProfit": "trailingGrossProfit",
        "OperatingIncome": "trailingOperatingIncome",
        "NetIncome": "trailingNetIncome",
        "OperatingCashFlow": "trailingOperatingCashFlow",
        "FreeCashFlow": "trailingFreeCashFlow",
        "CapitalExpenditure": "trailingCapitalExpenditure",
        "Ebitda": "trailingEbitda",
        "BasicEPS": "trailingBasicEPS",
        "DilutedEPS": "trailingDilutedEPS",
    }

    ttm_official_lookup = {}
    for item in selected_results or []:
        type_name = statement_type_name(item)
        if type_name.startswith("trailing"):
            points = series_points(item, type_name)
            if points:
                for base, trailing in trailing_map.items():
                    if type_name == trailing:
                        label = type_map.get(base)
                        if label:
                            ttm_official_lookup[label] = points[0]["raw"]
                        break

    q_sorted_periods = sorted(quarterly_period_dates, reverse=True)[:5]
    q_rows_out = []

    seen_labels = set()
    ordered_labels = []
    for label in type_map.values():
        if label in seen_labels:
            continue
        if label in annual_rows or label in quarterly_rows:
            ordered_labels.append(label)
            seen_labels.add(label)
    for lbl in list(annual_rows.keys()) + list(quarterly_rows.keys()):
        if lbl not in seen_labels:
            ordered_labels.append(lbl)
            seen_labels.add(lbl)

    for label in ordered_labels:
        annual_points = annual_rows.get(label, [])
        annual_by_date = {p["date"]: p["raw"] for p in annual_points}
        quarter_points = quarterly_rows.get(label, [])
        quarter_by_date = {p["date"]: p["raw"] for p in quarter_points}

        ttm_raw = ttm_official_lookup.get(label)
        if ttm_raw is None:
            if len(quarter_points) >= 4 and can_sum_ttm_label(label):
                latest_four = quarter_points[:4]
                ttm_raw = sum(point["raw"] for point in latest_four)
            elif annual_points:
                ttm_raw = annual_points[0]["raw"]

        values = [formatter(ttm_raw) if ttm_raw is not None else "--"]
        for period in sorted_periods:
            raw = annual_by_date.get(period)
            values.append(formatter(raw) if raw is not None else "--")
        rows.append({"label": label, "values": values})

        q_values = []
        for period in q_sorted_periods:
            raw = quarter_by_date.get(period)
            q_values.append(formatter(raw) if raw is not None else "--")
        q_rows_out.append({"label": label, "values": q_values})

    res = {
        "annual": {"periods": periods if rows else [], "rows": rows},
        "quarterly": {"periods": q_sorted_periods if q_rows_out else [], "rows": q_rows_out},
    }
    res["annual"] = prune_sparse_periods(res["annual"])
    res["quarterly"] = prune_sparse_periods(res["quarterly"])
    return res


def prune_sparse_periods(stmt):
    periods = stmt.get("periods") or []
    rows = stmt.get("rows") or []
    if not periods or not rows:
        return stmt

    valid_indices = [0]
    for i in range(1, len(periods)):
        non_empty_count = sum(1 for row in rows if i < len(row["values"]) and row["values"][i] != "--")
        if non_empty_count >= 1 and (non_empty_count / len(rows)) >= 0.10:
            valid_indices.append(i)

    if len(valid_indices) == len(periods):
        return stmt

    return {
        "periods": [periods[i] for i in valid_indices],
        "rows": [{"label": r["label"], "values": [r["values"][i] for i in valid_indices]} for r in rows],
    }


def merge_statement_rows(primary, secondary):
    def _merge(p, s):
        p = p or {"periods": [], "rows": []}
        s = s or {"periods": [], "rows": []}
        periods = []
        for period in p.get("periods", []) + s.get("periods", []):
            if period not in periods:
                periods.append(period)

        labels = []
        rows_by_label = {}
        for statement in (p, s):
            source_periods = statement.get("periods", [])
            for row in statement.get("rows", []):
                label = row.get("label")
                if not label:
                    continue
                if label not in labels:
                    labels.append(label)
                target = rows_by_label.setdefault(label, {period: "--" for period in periods})
                for idx, value in enumerate(row.get("values", [])):
                    if idx >= len(source_periods):
                        continue
                    period = source_periods[idx]
                    if value and value != "--" and target[period] == "--":
                        target[period] = value

        return {
            "periods": periods,
            "rows": [{"label": label, "values": [rows_by_label[label][period] for period in periods]} for label in labels],
        }

    if "annual" in (primary or {}) or "quarterly" in (primary or {}) or "annual" in (secondary or {}):
        return {
            "annual": _merge((primary or {}).get("annual"), (secondary or {}).get("annual")),
            "quarterly": _merge((primary or {}).get("quarterly"), (secondary or {}).get("quarterly")),
        }
    return _merge(primary, secondary)


def _row_by_label(statement, labels):
    labels_lower = {label.lower() for label in labels}
    for row in (statement or {}).get("rows", []):
        if str(row.get("label", "")).lower() in labels_lower:
            return row
    return None


def _period_value(statement, row, period):
    periods = (statement or {}).get("periods") or []
    if not row or period not in periods:
        return None
    idx = periods.index(period)
    values = row.get("values") or []
    if idx >= len(values) or values[idx] in (None, "", "--"):
        return None
    return parse_money_to_raw(values[idx])


def add_shareholder_return(cash_flow_statement, formatter=None):
    formatter = formatter or format_money
    cash_flow_statement = cash_flow_statement or {}

    for period_key in ("annual", "quarterly"):
        cash_flow = cash_flow_statement.get(period_key)
        if not isinstance(cash_flow, dict):
            continue
        rows = cash_flow.get("rows") or []
        if any(row.get("label") == "Shareholder Return" for row in rows):
            continue

        buyback_row = _row_by_label(cash_flow, [
            "Repurchase Of Capital Stock",
            "Common Stock Payments",
            "Net Common Stock Issuance",
        ])
        dividend_row = _row_by_label(cash_flow, [
            "Cash Dividends Paid",
            "Common Stock Dividend Paid",
        ])
        if not buyback_row and not dividend_row:
            continue

        values = []
        for period in cash_flow.get("periods") or []:
            buybacks_raw = _period_value(cash_flow, buyback_row, period)
            dividends_raw = _period_value(cash_flow, dividend_row, period)
            if buybacks_raw is None and dividends_raw is None:
                values.append("--")
                continue
            shareholder_return_raw = abs(buybacks_raw or 0) + abs(dividends_raw or 0)
            values.append(formatter(shareholder_return_raw))

        insert_anchor = dividend_row or buyback_row
        insert_idx = rows.index(insert_anchor) + 1
        rows.insert(insert_idx, {"label": "Shareholder Return", "values": values})
        cash_flow["rows"] = rows

    return cash_flow_statement


def add_dividend_per_share(cash_flow_statement, income_statement, formatter=None):
    formatter = formatter or format_3sig
    cash_flow_statement = cash_flow_statement or {}
    income_statement = income_statement or {}

    for period_key in ("annual", "quarterly"):
        cash_flow = cash_flow_statement.get(period_key)
        income = income_statement.get(period_key)
        if not isinstance(cash_flow, dict) or not isinstance(income, dict):
            continue
        rows = cash_flow.get("rows") or []
        if any(row.get("label") == "Dividend Per Share" for row in rows):
            continue

        dividend_row = _row_by_label(cash_flow, [
            "Cash Dividends Paid",
            "Common Stock Dividend Paid",
        ])
        diluted_shares_row = _row_by_label(income, [
            "Diluted Average Shares",
            "Diluted Avg Shares",
            "Diluted Shares",
        ])
        if not dividend_row or not diluted_shares_row:
            continue

        values = []
        for period in cash_flow.get("periods") or []:
            dividends_raw = _period_value(cash_flow, dividend_row, period)
            diluted_shares_raw = _period_value(income, diluted_shares_row, period)
            if dividends_raw is None or not diluted_shares_raw:
                values.append("--")
                continue
            values.append(formatter(abs(dividends_raw) / diluted_shares_raw))

        insert_idx = rows.index(dividend_row) + 1
        rows.insert(insert_idx, {"label": "Dividend Per Share", "values": values})
        cash_flow["rows"] = rows

    return cash_flow_statement


def add_adjusted_operating_income(income_statement, cash_flow_statement, formatter=None):
    formatter = formatter or format_money
    income_statement = income_statement or {}
    cash_flow_statement = cash_flow_statement or {}

    for period_key in ("annual", "quarterly"):
        income = income_statement.get(period_key)
        cash_flow = cash_flow_statement.get(period_key)
        if not isinstance(income, dict) or not isinstance(cash_flow, dict):
            continue
        rows = income.get("rows") or []
        if any(row.get("label") == "Adjusted Operating Income" for row in rows):
            continue

        operating_row = _row_by_label(income, ["Operating Income"])
        da_row = _row_by_label(cash_flow, [
            "Depreciation & Amortization",
            "Depreciation And Amortization",
            "Depreciation, Amortization & Depletion",
            "Reconciled Depreciation",
            "Depreciation",
        ])
        capex_row = _row_by_label(cash_flow, [
            "Capital Expenditures",
            "Capital Expenditure",
            "Purchase Of PP&E",
            "Purchase Of PPE",
        ])
        if not operating_row:
            continue

        values = []
        for period in income.get("periods") or []:
            operating_raw = _period_value(income, operating_row, period)
            if operating_raw is None:
                values.append("--")
                continue
            da_raw = _period_value(cash_flow, da_row, period) or 0
            capex_raw = _period_value(cash_flow, capex_row, period) or 0
            adjusted_raw = operating_raw + max(da_raw - abs(capex_raw), 0)
            values.append(formatter(adjusted_raw))

        insert_idx = rows.index(operating_row) + 1
        rows.insert(insert_idx, {"label": "Adjusted Operating Income", "values": values})
        income["rows"] = rows

    return income_statement


def _parse_tax_rate(value):
    if value in (None, "", "--"):
        return None
    try:
        if isinstance(value, str) and "%" in value:
            return float(value.replace("%", "").replace(",", "").strip()) / 100
        rate = float(str(value).replace(",", "").strip())
        return rate / 100 if rate > 1 else rate
    except Exception:
        return None


def _sane_statement_tax_rate(value):
    rate = _parse_tax_rate(value)
    return rate if rate is not None and 0 <= rate <= 0.40 else 0.20


def add_adjusted_net_income(income_statement, formatter=None):
    formatter = formatter or format_money
    income_statement = income_statement or {}

    for period_key in ("annual", "quarterly"):
        income = income_statement.get(period_key)
        if not isinstance(income, dict):
            continue
        rows = income.get("rows") or []
        adjusted_op_row = _row_by_label(income, ["Adjusted Operating Income"])
        if not adjusted_op_row or any(row.get("label") == "Adjusted Net Income" for row in rows):
            continue
        tax_rate_row = _row_by_label(income, ["Tax Rate", "Tax Rate For Calcs"])

        values = []
        for idx, period in enumerate(income.get("periods") or []):
            adjusted_op_raw = _period_value(income, adjusted_op_row, period)
            if adjusted_op_raw is None:
                values.append("--")
                continue
            tax_rate_value = (tax_rate_row.get("values") or [])[idx] if tax_rate_row else None
            tax_rate = _sane_statement_tax_rate(tax_rate_value)
            values.append(formatter(adjusted_op_raw * (1 - tax_rate)))

        insert_idx = rows.index(adjusted_op_row) + 1
        rows.insert(insert_idx, {"label": "Adjusted Net Income", "values": values})
        income["rows"] = rows

    return income_statement


def add_adjusted_eps(income_statement, formatter=None):
    formatter = formatter or format_3sig
    income_statement = income_statement or {}

    for period_key in ("annual", "quarterly"):
        income = income_statement.get(period_key)
        if not isinstance(income, dict):
            continue
        rows = income.get("rows") or []
        adjusted_net_row = _row_by_label(income, ["Adjusted Net Income"])
        diluted_shares_row = _row_by_label(income, [
            "Diluted Average Shares",
            "Diluted Avg Shares",
            "Diluted Shares",
        ])
        if not adjusted_net_row or not diluted_shares_row or any(row.get("label") == "Adjusted EPS" for row in rows):
            continue

        values = []
        for period in income.get("periods") or []:
            adjusted_net_raw = _period_value(income, adjusted_net_row, period)
            diluted_shares_raw = _period_value(income, diluted_shares_row, period)
            if adjusted_net_raw is None or not diluted_shares_raw:
                values.append("--")
                continue
            values.append(formatter(adjusted_net_raw / diluted_shares_raw))

        insert_idx = rows.index(adjusted_net_row) + 1
        rows.insert(insert_idx, {"label": "Adjusted EPS", "values": values})
        income["rows"] = rows

    return income_statement


def add_tax_rate(income_statement, formatter=None):
    formatter = formatter or format_percent
    income_statement = income_statement or {}

    for period_key in ("annual", "quarterly"):
        income = income_statement.get(period_key)
        if not isinstance(income, dict):
            continue
        rows = income.get("rows") or []

        tax_row = _row_by_label(income, ["Tax Provision"])
        pretax_row = _row_by_label(income, ["Pretax Income", "Income Before Tax"])
        existing_tax_rate_row = _row_by_label(income, ["Tax Rate", "Tax Rate For Calcs"])
        if not tax_row or not pretax_row:
            if existing_tax_rate_row:
                existing_tax_rate_row["label"] = "Tax Rate"
            continue

        values = []
        for period in income.get("periods") or []:
            tax_raw = _period_value(income, tax_row, period)
            pretax_raw = _period_value(income, pretax_row, period)
            if tax_raw is None or not pretax_raw:
                values.append("--")
                continue
            values.append(formatter(tax_raw / abs(pretax_raw)))

        if existing_tax_rate_row:
            existing_tax_rate_row["label"] = "Tax Rate"
            existing_tax_rate_row["values"] = values
        else:
            insert_idx = rows.index(tax_row) + 1
            rows.insert(insert_idx, {"label": "Tax Rate", "values": values})
        income["rows"] = rows

    return income_statement


def ordered_df_index(df, order_map):
    if order_map is None:
        return list(df.index)
    ordered_keys = list(order_map.keys())
    index_list = list(df.index)
    normalized_index = {lbl.replace(" ", "").lower(): lbl for lbl in index_list}
    seen = set()
    result = []
    for key in ordered_keys:
        key_norm = key.lower()
        if key in index_list and key not in seen:
            result.append(key)
            seen.add(key)
        elif key_norm in normalized_index:
            lbl = normalized_index[key_norm]
            if lbl not in seen:
                result.append(lbl)
                seen.add(lbl)
    for idx_label in index_list:
        if idx_label not in seen:
            result.append(idx_label)
    return result


def resolve_display_label(label, order_map):
    if order_map:
        if label in order_map:
            return order_map[label]
        label_norm = label.replace(" ", "").lower()
        for key, display in order_map.items():
            if key.lower() == label_norm:
                return display
    if " " in str(label):
        return str(label)
    return camel_to_label(str(label))


def is_ttm_column(col):
    label = str(col).strip().lower().replace("_", " ")
    normalized = label.replace(" ", "")
    return normalized in {"ttm", "trailing", "trailingtwelvemonths"}


def df_history_columns(df):
    return sorted([c for c in df.columns if not is_ttm_column(c)], reverse=True)


def df_official_ttm_value(annual_df, row_labels):
    if annual_df is None or annual_df.empty:
        return None
    import pandas as pd
    ttm_cols = [c for c in annual_df.columns if is_ttm_column(c)]
    if not ttm_cols:
        return None
    for label in row_labels:
        if label in annual_df.index:
            for col in ttm_cols:
                val = annual_df.loc[label, col]
                if pd.notna(val):
                    return float(val)
    return None


def df_with_ttm_column(annual_df, ttm_df):
    if ttm_df is None or ttm_df.empty:
        return annual_df
    import pandas as pd
    if annual_df is None or annual_df.empty:
        result = pd.DataFrame(index=ttm_df.index)
    else:
        result = annual_df.copy()

    ttm_cols = sorted(ttm_df.columns, reverse=True)
    if not ttm_cols:
        return result
    if "TTM" in result.columns:
        result = result.drop(columns=["TTM"])
    result["TTM"] = ttm_df[ttm_cols[0]]
    return result


def can_sum_ttm_label(label):
    normalized = str(label).replace(" ", "").lower()
    non_additive_tokens = (
        "averageshares",
        "shares",
        "eps",
        "pershare",
        "perbasicshare",
        "perdilutedshare",
        "rate",
        "margin",
    )
    return not any(token in normalized for token in non_additive_tokens)


def format_statement_value(label, value, formatter=None):
    if value is None:
        return "--"
    formatter = formatter or format_money
    normalized = str(label).replace(" ", "").lower()
    if "rate" in normalized or "margin" in normalized:
        return format_percent(value)
    return formatter(value)


def should_display_blank_statement_row_as_zero(df, label, cols):
    if df is None or label not in df.index or not cols:
        return False
    import pandas as pd
    return not any(pd.notna(df.loc[label, c]) for c in cols if c in df.columns)


def df_to_statement(df, formatter=None, ttm_label="TTM", order_map=None, quarterly_df=None):
    formatter = formatter or format_money
    if df is None or df.empty:
        return {"periods": [], "rows": []}
    import pandas as pd
    cols = df_history_columns(df)
    ttm_cols = [c for c in df.columns if is_ttm_column(c)]
    if not cols and not ttm_cols:
        return {"periods": [], "rows": []}

    ordered_index = ordered_df_index(df, order_map)
    active_labels = ordered_index
    if not active_labels:
        return {"periods": [], "rows": []}

    if cols:
        active_df = df.loc[active_labels]
        cols = [c for c in cols if active_df[c].notna().any()]
    if not cols and not ttm_cols:
        return {"periods": [], "rows": []}
    cols = cols[:4]
    periods = [ttm_label] + [c.strftime("%Y-%m-%d") if hasattr(c, "strftime") else str(c) for c in cols]
    rows = []
    for label in active_labels:
        raw_values = df.loc[label, cols].tolist()
        ttm_val = df_official_ttm_value(df, [label])
        if quarterly_df is not None:
            ttm_val = ttm_val if ttm_val is not None else df_ttm_value(quarterly_df, df, [label])

        if ttm_val is None and raw_values:
            ttm_val = raw_values[0]

        blank_row_is_zero = should_display_blank_statement_row_as_zero(df, label, cols + ttm_cols)
        formatted = [format_statement_value(label, 0, formatter) if blank_row_is_zero else format_statement_value(label, ttm_val, formatter) if pd.notna(ttm_val) else "--"]
        for v in raw_values:
            formatted.append(format_statement_value(label, v, formatter) if pd.notna(v) else format_statement_value(label, 0, formatter) if blank_row_is_zero else "--")
        rows.append({"label": resolve_display_label(label, order_map), "values": formatted})
    return {"periods": periods, "rows": rows}


def df_to_quarterly_statement(df, formatter=None, order_map=None):
    formatter = formatter or format_money
    if df is None or df.empty:
        return {"periods": [], "rows": []}
    import pandas as pd
    cols = df_history_columns(df)
    if not cols:
        return {"periods": [], "rows": []}

    ordered_index = ordered_df_index(df, order_map)
    active_labels = ordered_index
    if not active_labels:
        return {"periods": [], "rows": []}

    active_df = df.loc[active_labels]
    cols = [c for c in cols if active_df[c].notna().any()]
    if not cols:
        return {"periods": [], "rows": []}
    cols = cols[:5]
    periods = [c.strftime("%Y-%m-%d") if hasattr(c, "strftime") else str(c) for c in cols]
    rows = []
    for label in active_labels:
        raw_values = df.loc[label, cols].tolist()
        blank_row_is_zero = should_display_blank_statement_row_as_zero(df, label, cols)
        formatted = [format_statement_value(label, v, formatter) if pd.notna(v) else format_statement_value(label, 0, formatter) if blank_row_is_zero else "--" for v in raw_values]
        rows.append({"label": resolve_display_label(label, order_map), "values": formatted})
    return {"periods": periods, "rows": rows}


def df_raw_value(df, row_labels, col_index=0):
    if df is None or df.empty:
        return 0.0
    import pandas as pd
    for label in row_labels:
        if label in df.index:
            cols = df_history_columns(df)
            if col_index < len(cols):
                val = df.loc[label, cols[col_index]]
                if pd.notna(val):
                    return float(val)
    return 0.0


def df_ttm_value(quarterly_df, annual_df, row_labels, absolute=False):
    import pandas as pd
    official_ttm = df_official_ttm_value(annual_df, row_labels)
    if official_ttm is not None:
        return abs(official_ttm) if absolute else official_ttm
    if quarterly_df is not None and not quarterly_df.empty:
        cols = df_history_columns(quarterly_df)
        for label in row_labels:
            if label in quarterly_df.index:
                if not can_sum_ttm_label(label):
                    continue
                vals = [quarterly_df.loc[label, c] for c in cols[:4]]
                valid = [float(v) for v in vals if pd.notna(v)]
                if len(valid) >= 4:
                    total = sum(valid)
                    return abs(total) if absolute else total
    val = df_raw_value(annual_df, row_labels, 0)
    return abs(val) if absolute else val
