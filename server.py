import http.server
import socketserver
import urllib.request
import re
import json
import os
import datetime
import time
import io
import subprocess
from urllib.parse import urlparse, parse_qs
from urllib.error import URLError, HTTPError

PORT = int(os.environ.get("PORT", "3000"))
CACHE_FILE = 'cache.json'
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "900"))
PAYLOAD_VERSION = 6
YAHOO_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

FETCH_RESULT_FIELDS = [
    "income", "margin", "gross_margin", "ev_cy_ebit", "ev_ny_ebit", "adj_income",
    "capex", "da", "ev", "ev_adj_ebit", "cy_growth", "ny_growth", "gp_3y_growth",
    "gp_3y_start", "gp_3y_end", "gp_3y_label", "rnd_adj_income", "cy_adj_inc",
    "ny_adj_inc", "market_cap", "net_cash", "derived_enterprise_value", "revenue",
    "operating_margin", "da_minus_capex", "cy_revenue", "ny_revenue", "gross_ppe",
    "adj_ebit_gross_ppe", "capex_adj_income", "investment_capex", "roc",
    "net_working_capital", "net_fixed_assets", "receivables", "inventory",
    "accounts_payable", "financial_currency", "usd_fx_rate", "company_name",
    "income_statement", "balance_statement", "cash_flow_statement", "current_price",
    "target_mean_price", "target_low_price", "target_high_price", "target_move",
    "recommendation_mean", "recommendation_key", "analyst_recommendations",
    "valuation_basis", "valuation_prefix", "valuation_numerator_label",
    "current_year_eps", "next_year_eps", "year_ago_eps", "current_year_eps_growth",
    "next_year_eps_growth", "price_current_eps", "price_cy_eps", "price_ny_eps",
]

INCOME_STATEMENT_TYPES = {
    "TotalRevenue": "Total Revenue",
    "CostOfRevenue": "Cost of Revenue",
    "GrossProfit": "Gross Profit",
    "ResearchAndDevelopment": "Research & Development",
    "SellingGeneralAndAdministration": "Selling, General & Administrative",
    "SellingAndMarketingExpense": "Selling & Marketing Expense",
    "GeneralAndAdministrativeExpense": "General & Administrative Expense",
    "OtherGandA": "Other G&A",
    "OtherOperatingExpenses": "Other Operating Expenses",
    "OperatingExpense": "Operating Expense",
    "OperatingIncome": "Operating Income",
    "TotalOperatingIncomeAsReported": "Total Operating Income As Reported",
    "InterestExpense": "Interest Expense",
    "InterestIncome": "Interest Income",
    "NetInterestIncome": "Net Interest Income",
    "NetNonOperatingInterestIncomeExpense": "Net Non Operating Interest Income Expense",
    "OtherIncomeExpense": "Other Income Expense",
    "PretaxIncome": "Pretax Income",
    "TaxProvision": "Tax Provision",
    "NetIncome": "Net Income",
    "NetIncomeCommonStockholders": "Net Income Common Stockholders",
    "DilutedAverageShares": "Diluted Average Shares",
    "BasicAverageShares": "Basic Average Shares",
    "DilutedEPS": "Diluted EPS",
    "BasicEPS": "Basic EPS",
    "EBIT": "EBIT",
    "EBITDA": "EBITDA",
    "TaxRateForCalcs": "Tax Rate For Calcs",
    "TotalExpenses": "Total Expenses",
    "TotalUnusualItems": "Total Unusual Items",
    "TotalUnusualItemsExcludingGoodwill": "Total Unusual Items Excluding Goodwill",
    "SpecialIncomeCharges": "Special Income Charges",
    "WriteOff": "Write Off",
}

BALANCE_STATEMENT_TYPES = {
    "CashAndCashEquivalents": "Cash & Cash Equivalents",
    "CashCashEquivalentsAndShortTermInvestments": "Cash, Equivalents & Short Term Investments",
    "OtherShortTermInvestments": "Other Short Term Investments",
    "AccountsReceivable": "Accounts Receivable",
    "Inventory": "Inventory",
    "AccountsPayable": "Accounts Payable",
    "CurrentDebt": "Current Debt",
    "LongTermDebt": "Long Term Debt",
    "TotalDebt": "Total Debt",
    "GrossPPE": "Gross PP&E",
    "NetPPE": "Net PP&E",
    "TotalAssets": "Total Assets",
    "TotalLiabilitiesNetMinorityInterest": "Total Liabilities",
    "StockholdersEquity": "Stockholders Equity",
}

CASH_FLOW_STATEMENT_TYPES = {
    "OperatingCashFlow": "Operating Cash Flow",
    "CapitalExpenditure": "Capital Expenditures",
    "DepreciationAndAmortization": "Depreciation And Amortization",
    "FreeCashFlow": "Free Cash Flow",
    "RepurchaseOfCapitalStock": "Repurchase Of Capital Stock",
    "IssuanceOfCapitalStock": "Issuance Of Capital Stock",
    "CashDividendsPaid": "Cash Dividends Paid",
}

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {}

def save_cache(cache_data):
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache_data, f)
    except:
        pass

class Handler(http.server.SimpleHTTPRequestHandler):
    def build_test_payload(self, pulled_at=None):
        today = datetime.date.today().isoformat()
        pulled_at = pulled_at or datetime.datetime.now().isoformat(timespec="seconds")
        income_statement = {
            "periods": ["TTM", "2025-12-31", "2024-12-31", "2023-12-31", "2022-12-31"],
            "rows": [
                {"label": "Total Revenue", "values": ["100B", "92B", "84B", "76B", "68B"]},
                {"label": "Cost of Revenue", "values": ["40B", "37B", "35B", "32B", "24B"]},
                {"label": "Gross Profit", "values": ["60B", "55B", "49B", "44B", "44B"]},
                {"label": "Research & Development", "values": ["12B", "11B", "10B", "8B", "7B"]},
                {"label": "Selling, General & Administrative", "values": ["14B", "13B", "12B", "11B", "10B"]},
                {"label": "Operating Income", "values": ["30B", "27.6B", "25.2B", "22.8B", "20.4B"]},
                {"label": "Interest Expense", "values": ["1.2B", "1.1B", "1.0B", "900M", "800M"]},
                {"label": "Pretax Income", "values": ["29B", "26.8B", "24.5B", "22B", "19.5B"]},
                {"label": "Tax Provision", "values": ["5.8B", "5.36B", "4.9B", "4.4B", "3.9B"]},
                {"label": "Net Income", "values": ["23.2B", "21.4B", "19.6B", "17.6B", "15.6B"]},
                {"label": "Diluted Average Shares", "values": ["2.32B", "2.35B", "2.4B", "2.45B", "2.5B"]},
                {"label": "Diluted EPS", "values": ["10", "9.1", "8.17", "7.18", "6.24"]},
                {"label": "EBITDA", "values": ["35B", "32.6B", "30.2B", "27.8B", "25.4B"]},
                {"label": "Tax Rate For Calcs", "values": ["0.20", "0.20", "0.20", "0.20", "0.20"]},
            ],
        }
        balance_statement = {
            "periods": ["MRQ", "2025-12-31", "2024-12-31", "2023-12-31", "2022-12-31"],
            "rows": [
                {"label": "Cash & Cash Equivalents", "values": ["25B", "24B", "21B", "18B", "15B"]},
                {"label": "Other Short Term Investments", "values": ["20B", "18B", "16B", "12B", "10B"]},
                {"label": "Cash, Equivalents & Short Term Investments", "values": ["45B", "42B", "37B", "30B", "25B"]},
                {"label": "Accounts Receivable", "values": ["15B", "14B", "13B", "12B", "11B"]},
                {"label": "Inventory", "values": ["10B", "9B", "8B", "7B", "6B"]},
                {"label": "Accounts Payable", "values": ["8B", "7.5B", "7B", "6.5B", "6B"]},
                {"label": "Current Debt", "values": ["5B", "4B", "4B", "3B", "3B"]},
                {"label": "Long Term Debt", "values": ["20B", "22B", "24B", "25B", "26B"]},
                {"label": "Total Debt", "values": ["25B", "26B", "28B", "28B", "29B"]},
                {"label": "Gross PP&E", "values": ["80B", "76B", "70B", "65B", "60B"]},
                {"label": "Net PP&E", "values": ["50B", "48B", "45B", "42B", "39B"]},
                {"label": "Total Assets", "values": ["180B", "170B", "158B", "145B", "132B"]},
                {"label": "Total Liabilities", "values": ["70B", "68B", "66B", "62B", "58B"]},
                {"label": "Stockholders Equity", "values": ["110B", "102B", "92B", "83B", "74B"]},
            ],
        }
        cash_flow_statement = {
            "periods": ["TTM", "2025-12-31", "2024-12-31", "2023-12-31", "2022-12-31"],
            "rows": [
                {"label": "Operating Cash Flow", "values": ["34B", "32B", "29B", "25B", "22B"]},
                {"label": "Capital Expenditures", "values": ["-7B", "-6.5B", "-6B", "-5.5B", "-5B"]},
                {"label": "Depreciation And Amortization", "values": ["5B", "5B", "4.8B", "4.5B", "4.2B"]},
                {"label": "Free Cash Flow", "values": ["27B", "25.5B", "23B", "19.5B", "17B"]},
                {"label": "Repurchase Of Capital Stock", "values": ["-8B", "-7B", "-6B", "-4B", "-3B"]},
                {"label": "Cash Dividends Paid", "values": ["-3B", "-2.8B", "-2.5B", "-2.2B", "-2B"]},
            ],
        }
        revenue_raw = 100e9
        gross_profit_raw = 60e9
        operating_income_raw = 30e9
        capex_raw = 7e9
        da_raw = 5e9
        gross_ppe_raw = 80e9
        net_fixed_assets_raw = 50e9
        receivables_raw = 15e9
        inventory_raw = 10e9
        accounts_payable_raw = 8e9
        rnd_raw = 12e9
        market_cap_raw = 500e9
        cash_bucket_raw = 45e9
        total_debt_raw = 25e9
        cy_growth_raw = 0.10
        ny_growth_raw = 0.12
        gp_3y_start_raw = 44e9
        gp_3y_end_raw = gross_profit_raw

        da_minus_capex_raw = max(da_raw - capex_raw, 0)
        investment_capex_raw = max(capex_raw - da_raw, 0)
        adj_income_raw = operating_income_raw + da_minus_capex_raw
        adj_margin_ratio = adj_income_raw / revenue_raw
        gross_margin_ratio = gross_profit_raw / revenue_raw
        cy_revenue_raw = revenue_raw * (1 + cy_growth_raw)
        ny_revenue_raw = cy_revenue_raw * (1 + ny_growth_raw)
        cy_adj_inc_raw = cy_revenue_raw * adj_margin_ratio
        ny_adj_inc_raw = ny_revenue_raw * adj_margin_ratio
        gp_3y_growth_raw = (gp_3y_end_raw / gp_3y_start_raw) ** (1 / 3) - 1
        net_cash_raw = cash_bucket_raw - total_debt_raw
        derived_ev_raw = market_cap_raw - net_cash_raw
        net_working_capital_raw = receivables_raw + inventory_raw - accounts_payable_raw
        roc_denominator_raw = net_working_capital_raw + net_fixed_assets_raw

        return {
            "ticker": "TEST",
            "shortFloat": "4.2%",
            "income": self._format_money(operating_income_raw),
            "margin": self._format_percent(adj_margin_ratio),
            "grossMargin": self._format_percent(gross_margin_ratio),
            "ev_cy_ebit": self._format_3sig(derived_ev_raw / cy_adj_inc_raw),
            "ev_ny_ebit": self._format_3sig(derived_ev_raw / ny_adj_inc_raw),
            "adj_income": self._format_money(adj_income_raw),
            "capex": self._format_money(capex_raw),
            "da": self._format_money(da_raw),
            "ev": self._format_money(derived_ev_raw),
            "ev_adj_ebit": self._format_3sig(derived_ev_raw / adj_income_raw),
            "cy_growth": self._format_percent(cy_growth_raw),
            "ny_growth": self._format_percent(ny_growth_raw),
            "gp_3y_growth": self._format_percent(gp_3y_growth_raw),
            "gp_3y_start": self._format_money(gp_3y_start_raw),
            "gp_3y_end": self._format_money(gp_3y_end_raw),
            "gp_3y_label": "3Y Annual GP Growth",
            "rndAdjIncome": self._format_percent(rnd_raw / adj_income_raw),
            "cy_adj_inc": self._format_money(cy_adj_inc_raw),
            "ny_adj_inc": self._format_money(ny_adj_inc_raw),
            "marketCap": self._format_money(market_cap_raw),
            "netCash": self._format_money(net_cash_raw),
            "derivedEnterpriseValue": self._format_money(derived_ev_raw),
            "revenue": self._format_money(revenue_raw),
            "operating_margin": self._format_percent(operating_income_raw / revenue_raw),
            "da_minus_capex": self._format_money(da_minus_capex_raw),
            "cy_revenue": self._format_money(cy_revenue_raw),
            "ny_revenue": self._format_money(ny_revenue_raw),
            "grossPpe": self._format_money(gross_ppe_raw),
            "adjEbitGrossPpe": self._format_percent(adj_income_raw / gross_ppe_raw),
            "capexAdjIncome": self._format_percent(investment_capex_raw / adj_income_raw),
            "investmentCapex": self._format_money(investment_capex_raw),
            "roc": self._format_percent(adj_income_raw / roc_denominator_raw),
            "netWorkingCapital": self._format_money(net_working_capital_raw),
            "netFixedAssets": self._format_money(net_fixed_assets_raw),
            "receivables": self._format_money(receivables_raw),
            "inventory": self._format_money(inventory_raw),
            "accountsPayable": self._format_money(accounts_payable_raw),
            "financialCurrency": "CNY",
            "usdFxRate": 0.138,
            "companyName": "Test Fixture Corporation",
            "incomeStatement": income_statement,
            "balanceStatement": balance_statement,
            "cashFlowStatement": cash_flow_statement,
            "currentPrice": "100",
            "targetMeanPrice": "125",
            "targetLowPrice": "90",
            "targetHighPrice": "160",
            "targetMove": "25%",
            "recommendationMean": "1.8",
            "recommendationKey": "buy",
            "analystRecommendations": {
                "period": "0m",
                "strongBuy": 5,
                "buy": 8,
                "hold": 3,
                "sell": 1,
                "strongSell": 0,
            },
            "valuationBasis": "derivedEnterpriseValue",
            "valuationPrefix": "EV",
            "valuationNumeratorLabel": "Derived Enterprise Value",
            "currentYearEps": "10",
            "nextYearEps": "12",
            "yearAgoEps": "8",
            "currentYearEpsGrowth": "25%",
            "nextYearEpsGrowth": "20%",
            "priceCurrentEps": "12.5",
            "priceCyEps": "10",
            "priceNyEps": "8.33",
            "payloadVersion": PAYLOAD_VERSION,
            "evSource": "test-fixture",
            "marketCapSource": "test-fixture",
            "dataDate": today,
            "pulledAt": pulled_at,
            "fetchCount": 0,
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory="public", **kwargs)

    def do_GET(self):
        if self.path.startswith('/api/short-interest/'):
            parsed = urlparse(self.path)
            ticker = parsed.path.split('/')[-1].upper()
            qs = parse_qs(parsed.query)
            refresh = qs.get("refresh", ["0"])[0] == "1"
            self.handle_api_request(ticker, refresh=refresh)
        else:
            super().do_GET()

    def _counted_open(self, opener, url, timeout=3):
        if not hasattr(self, "_fetch_count_lock"):
            self._fetch_count_lock = None
        if not hasattr(self, "_request_fetch_count"):
            self._request_fetch_count = 0
        if self._fetch_count_lock:
            with self._fetch_count_lock:
                self._request_fetch_count += 1
        else:
            self._request_fetch_count += 1
        if isinstance(url, str) and (
            url.startswith("https://query1.finance.yahoo.com/")
            or url.startswith("https://stockanalysis.com/")
        ):
            try:
                result = subprocess.run(
                    [
                        "curl",
                        "-fsSL",
                        "--compressed",
                        "--max-time",
                        str(timeout),
                        "-A",
                        YAHOO_USER_AGENT,
                        "-H",
                        "Accept: application/json,text/plain,*/*",
                        "-H",
                        "Accept-Language: en-US,en;q=0.9",
                        url,
                    ],
                    check=True,
                    capture_output=True,
                )
                return io.BytesIO(result.stdout)
            except subprocess.CalledProcessError as exc:
                raise URLError(exc.stderr.decode("utf-8", errors="ignore") or str(exc))
        if opener is not None and hasattr(opener, "open"):
            return opener.open(url, timeout=timeout)
        return urllib.request.urlopen(url, timeout=timeout)

    def get_yahoo_crumb(self, opener=None):
        response = self._counted_open(opener, "https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=3)
        return response.read().decode("utf-8", errors="ignore").strip()

    def get_usd_fx_rate(self, currency, opener=None):
        currency = (currency or "USD").upper()
        if currency == "USD":
            return 1.0
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{currency}USD=X?range=1d&interval=1d"
            body = json.loads(self._counted_open(opener, url, timeout=3).read().decode("utf-8"))
            result = body.get("chart", {}).get("result", []) or []
            quote = result[0].get("indicators", {}).get("quote", [{}])[0] if result else {}
            close = quote.get("close", []) or []
            for value in reversed(close):
                if value:
                    return float(value)
        except Exception:
            pass
        return 1.0

    def _format_3sig(self, val):
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
        elif abs_val >= 1:
            res = f"{val:.2f}"
        elif abs_val >= 0.1:
            res = f"{val:.3f}"
        elif abs_val >= 0.01:
            res = f"{val:.4f}"
        else:
            res = f"{val:.3g}"
        if "." in res:
            res = res.rstrip("0").rstrip(".")
        return res

    def _format_percent(self, val):
        if val in (None, ""):
            return "--"
        return f"{self._format_3sig(float(val) * 100)}%"

    def _format_money(self, val):
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
            return self._format_3sig(val / 1e12) + "T"
        if abs_val >= 1e9:
            return self._format_3sig(val / 1e9) + "B"
        if abs_val >= 1e6:
            return self._format_3sig(val / 1e6) + "M"
        return self._format_3sig(val)

    def _raw(self, obj, default=0):
        if isinstance(obj, dict):
            value = obj.get("raw", default)
            if value is None:
                return default
            return value
        return default

    def _eps_value(self, obj):
        if not isinstance(obj, dict):
            return 0.0
        value = obj.get("fmt")
        if value not in (None, "", "--"):
            try:
                return float(str(value).replace(",", ""))
            except Exception:
                pass
        return float(obj.get("raw") or 0)

    def _parse_money_to_raw(self, value):
        if value in (None, "", "--"):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        return self._parse_finviz_abbrev_to_raw(str(value))

    def _empty_fetch_tuple(self, ticker):
        values = {key: "--" for key in FETCH_RESULT_FIELDS}
        values.update({
            "valuation_basis": "unavailable",
            "valuation_prefix": "EV",
            "valuation_numerator_label": "Current Enterprise Value",
            "company_name": ticker,
            "financial_currency": "USD",
            "usd_fx_rate": 1.0,
            "income_statement": {"periods": [], "rows": []},
            "balance_statement": {"periods": [], "rows": []},
            "cash_flow_statement": {"periods": [], "rows": []},
            "analyst_recommendations": {},
        })
        return tuple(values[key] for key in FETCH_RESULT_FIELDS)

    def fetch_yahoo_key_statistics_ev_and_market_cap(self, ticker):
        """
        Pull EV + Market Cap from the public Yahoo Finance Key Statistics page HTML.
        This matches the "Valuation Measures" table the user sees in the browser.
        """
        url = f"https://finance.yahoo.com/quote/{ticker}/key-statistics?p={ticker}"
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        html = urllib.request.urlopen(req, timeout=6).read().decode("utf-8", errors="ignore")

        # Yahoo pages embed a large JSON blob as: root.App.main = {...};
        m = re.search(r"root\.App\.main\s*=\s*({.*?})\s*;\s*\n", html, re.DOTALL)
        if not m:
            raise ValueError("Could not find root.App.main JSON in Yahoo page")

        data = json.loads(m.group(1))
        stores = (
            data.get("context", {})
            .get("dispatcher", {})
            .get("stores", {})
            .get("QuoteSummaryStore", {})
        )
        dks = stores.get("defaultKeyStatistics", {}) or {}
        price = stores.get("price", {}) or {}

        ev_raw = (dks.get("enterpriseValue", {}) or {}).get("raw", 0) or 0
        market_cap_raw = (
            (dks.get("marketCap", {}) or {}).get("raw", 0)
            or (price.get("marketCap", {}) or {}).get("raw", 0)
            or 0
        )

        return float(ev_raw or 0), float(market_cap_raw or 0)

    def _parse_finviz_abbrev_to_raw(self, value):
        if not value or value == "--":
            return 0.0
        s = value.strip().upper().replace(",", "")
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

    def _extract_finviz_metric(self, html, label):
        pattern = rf'{re.escape(label)}.*?</td>.*?<td[^>]*>.*?<b[^>]*>\s*(.+?)\s*</b>'
        m = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
        if not m:
            return "--"
        val = re.sub(r'<[^>]+>', '', m.group(1)).strip()
        return val or "--"

    def fetch_finviz_snapshot_metrics(self, ticker):
        url = f"https://finviz.com/quote.ashx?t={ticker}&p=d"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        html = urllib.request.urlopen(req, timeout=6).read().decode('utf-8', errors='ignore')
        return {
            "short_float": self._extract_finviz_metric(html, "Short Float"),
            "market_cap": self._extract_finviz_metric(html, "Market Cap"),
            "enterprise_value": self._extract_finviz_metric(html, "Enterprise Value"),
            "eps_this_y": self._extract_finviz_metric(html, "EPS this Y"),
            "eps_next_y": self._extract_finviz_metric(html, "EPS next Y"),
        }

    def _latest_row_raw(self, statement, labels):
        labels_lower = {label.lower() for label in labels}
        for row in (statement or {}).get("rows", []):
            if row.get("label", "").lower() in labels_lower:
                for value in row.get("values", []):
                    raw = self._parse_money_to_raw(value)
                    if raw:
                        return raw
        return 0.0

    def _statement_latest_value(self, statement, labels):
        labels_lower = {label.lower() for label in labels}
        for row in (statement or {}).get("rows", []):
            if row.get("label", "").lower() in labels_lower:
                for value in row.get("values", []):
                    if value not in (None, "", "--"):
                        return value
        return "--"

    def _camel_to_label(self, key):
        return re.sub(r"(?<!^)(?=[A-Z])", " ", key).replace("And", "and")

    def _statement_type_name(self, item):
        meta_type = (item.get("meta", {}) or {}).get("type", [""])
        return meta_type[0] if meta_type else ""

    def _series_points(self, item, key):
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

    def build_statement_from_timeseries_results(self, selected_results, type_map, formatter):
        annual_rows = {}
        quarterly_rows = {}
        period_dates = set()

        for item in selected_results or []:
            type_name = self._statement_type_name(item)
            prefix = "annual" if type_name.startswith("annual") else "quarterly" if type_name.startswith("quarterly") else ""
            if not prefix:
                continue
            base_key = type_name[len(prefix):]
            label = type_map.get(base_key)
            if not label:
                continue
            points = self._series_points(item, type_name)
            if not points:
                continue
            if prefix == "annual":
                annual_rows[label] = sorted(points, key=lambda p: p["date"], reverse=True)
                for point in points:
                    if not point["date"].startswith("idx-"):
                        period_dates.add(point["date"])
            else:
                quarterly_rows[label] = sorted(points, key=lambda p: p["date"], reverse=True)

        sorted_periods = sorted(period_dates, reverse=True)
        periods = ["TTM"] + sorted_periods
        rows = []

        ordered_labels = [label for label in type_map.values() if label in annual_rows or label in quarterly_rows]
        for label in ordered_labels:
            annual_points = annual_rows.get(label, [])
            annual_by_date = {p["date"]: p["raw"] for p in annual_points}
            quarter_points = quarterly_rows.get(label, [])

            ttm_raw = None
            if len(quarter_points) >= 4:
                latest_four = quarter_points[:4]
                ttm_raw = sum(point["raw"] for point in latest_four)
            elif annual_points:
                ttm_raw = annual_points[0]["raw"]

            values = [formatter(ttm_raw) if ttm_raw is not None else "--"]
            for period in sorted_periods:
                raw = annual_by_date.get(period)
                values.append(formatter(raw) if raw is not None else "--")
            rows.append({"label": label, "values": values})

        return {"periods": periods if rows else [], "rows": rows}

    def build_income_statement_from_timeseries_results(self, selected_results, _identity_formatter=None, formatter=None):
        formatter = formatter or self._format_money
        return self.build_statement_from_timeseries_results(selected_results, INCOME_STATEMENT_TYPES, formatter)

    def build_balance_sheet_from_timeseries_results(self, selected_results, formatter=None):
        return self.build_statement_from_timeseries_results(selected_results, BALANCE_STATEMENT_TYPES, formatter or self._format_money)

    def build_cash_flow_statement_from_timeseries_results(self, selected_results, formatter=None):
        return self.build_statement_from_timeseries_results(selected_results, CASH_FLOW_STATEMENT_TYPES, formatter or self._format_money)

    def _extract_timeseries_results_from_page(self, ticker, page_opener):
        url = f"https://finance.yahoo.com/quote/{ticker}/financials/"
        html = page_opener.open(url, timeout=10).read().decode("utf-8", errors="ignore")
        results = []
        for match in re.finditer(r'<script[^>]*data-sveltekit-fetched[^>]*>(.*?)</script>', html, re.DOTALL):
            try:
                outer = json.loads(match.group(1))
                body = outer.get("body", "{}")
                if isinstance(body, str):
                    body = json.loads(body)
                results.extend(body.get("timeseries", {}).get("result", []) or [])
            except Exception:
                continue
        return results

    def build_income_statement_from_page(self, ticker, page_opener=None, _identity_formatter=None, formatter=None):
        page_opener = page_opener or urllib.request.build_opener()
        return self.build_income_statement_from_timeseries_results(
            self._extract_timeseries_results_from_page(ticker, page_opener),
            _identity_formatter,
            formatter or self._format_money,
        )

    def build_balance_sheet_from_page(self, ticker, page_opener=None, formatter=None):
        page_opener = page_opener or urllib.request.build_opener()
        return self.build_balance_sheet_from_timeseries_results(
            self._extract_timeseries_results_from_page(ticker, page_opener),
            formatter or self._format_money,
        )

    def build_cash_flow_statement_from_page(self, ticker, page_opener=None, formatter=None):
        page_opener = page_opener or urllib.request.build_opener()
        return self.build_cash_flow_statement_from_timeseries_results(
            self._extract_timeseries_results_from_page(ticker, page_opener),
            formatter or self._format_money,
        )

    def _stockanalysis_array(self, body):
        body = body.replace("void 0", "null")
        try:
            return json.loads(f"[{body}]")
        except Exception:
            return []

    def _normalize_stockanalysis_label(self, label):
        replacements = {
            "Revenue": "Total Revenue",
            "Selling, General & Admin": "Selling, General & Administrative",
            "Provision for Income Taxes": "Tax Provision",
            "Shares Outstanding (Diluted)": "Diluted Average Shares",
            "Shares Outstanding (Basic)": "Basic Average Shares",
            "EPS (Diluted)": "Diluted EPS",
            "EPS (Basic)": "Basic EPS",
            "Cash & Equivalents": "Cash & Cash Equivalents",
            "Cash & Short-Term Investments": "Cash, Equivalents & Short Term Investments",
            "Short-Term Investments": "Other Short Term Investments",
            "Net Property, Plant & Equipment": "Net PP&E",
            "Short-Term Debt": "Current Debt",
            "Long-Term Debt": "Long Term Debt",
            "Shareholders' Equity": "Stockholders Equity",
            "Depreciation & Amortization": "Depreciation And Amortization",
        }
        return replacements.get(label, label)

    def _format_stockanalysis_value(self, value, fmt):
        if value is None:
            return "--"
        if fmt in ("percentage", "growth", "inverted-growth"):
            try:
                return self._format_percent(float(value))
            except Exception:
                return "--"
        if fmt in ("pershare", "divpershare", "reduce_precision"):
            return self._format_3sig(value)
        return self._format_money(value)

    def build_statement_from_stockanalysis_page(self, ticker, statement_kind):
        paths = {
            "income": f"https://stockanalysis.com/stocks/{ticker.lower()}/financials/",
            "balance": f"https://stockanalysis.com/stocks/{ticker.lower()}/financials/balance-sheet/",
            "cash": f"https://stockanalysis.com/stocks/{ticker.lower()}/financials/cash-flow-statement/",
        }
        url = paths.get(statement_kind)
        if not url:
            return {"periods": [], "rows": []}
        html = self._counted_open(None, url, timeout=8).read().decode("utf-8", errors="ignore")
        data_match = re.search(r"financialData:\{(.*?)\},map:\[", html, re.DOTALL)
        map_match = re.search(r"\},map:\[(.*?)\],full_count", html, re.DOTALL)
        if not data_match or not map_match:
            return {"periods": [], "rows": []}

        field_values = {}
        for match in re.finditer(r"([A-Za-z_][A-Za-z0-9_]*):\[(.*?)\](?=,[A-Za-z_][A-Za-z0-9_]*:|$)", data_match.group(1), re.DOTALL):
            values = self._stockanalysis_array(match.group(2))
            if values:
                field_values[match.group(1)] = values

        periods = [str(period) for period in field_values.get("datekey", [])]
        rows = []
        for object_match in re.finditer(r"\{(.*?)\}", map_match.group(1), re.DOTALL):
            body = object_match.group(1)
            id_match = re.search(r'id:"([^"]+)"', body)
            title_match = re.search(r'title:"([^"]+)"', body)
            if not id_match or not title_match:
                continue
            field_id = id_match.group(1)
            raw_values = field_values.get(field_id)
            if not raw_values:
                continue
            fmt_match = re.search(r'format:"([^"]+)"', body)
            fmt = fmt_match.group(1) if fmt_match else ""
            label = self._normalize_stockanalysis_label(title_match.group(1))
            values = [self._format_stockanalysis_value(value, fmt) for value in raw_values[:len(periods)]]
            rows.append({"label": label, "values": values})

        return {"periods": periods if rows else [], "rows": rows}

    def _merge_statement_rows(self, primary, secondary):
        primary = primary or {"periods": [], "rows": []}
        secondary = secondary or {"periods": [], "rows": []}
        periods = []
        for period in primary.get("periods", []) + secondary.get("periods", []):
            if period not in periods:
                periods.append(period)

        labels = []
        rows_by_label = {}
        for statement in (primary, secondary):
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
                    if period not in target:
                        target[period] = "--"
                    if target.get(period) in (None, "", "--") and value not in (None, "", "--"):
                        target[period] = value

        preferred_order = []
        for mapping in (INCOME_STATEMENT_TYPES, BALANCE_STATEMENT_TYPES, CASH_FLOW_STATEMENT_TYPES):
            for label in mapping.values():
                if label in labels and label not in preferred_order:
                    preferred_order.append(label)
        for label in labels:
            if label not in preferred_order:
                preferred_order.append(label)

        rows = [{"label": label, "values": [rows_by_label[label].get(period, "--") for period in periods]} for label in preferred_order]
        return {"periods": periods, "rows": rows}

    def fetch_yahoo_finance_data(self, ticker, finviz_ev_raw=0, finviz_market_cap_raw=0, finviz_metrics=None):
        finviz_metrics = finviz_metrics or {}
        try:
            from http.cookiejar import CookieJar

            def yahoo_opener():
                cj = CookieJar()
                opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
                opener.addheaders = [
                    ("User-Agent", YAHOO_USER_AGENT),
                    ("Accept", "application/json,text/plain,*/*"),
                    ("Accept-Language", "en-US,en;q=0.9"),
                    ("Connection", "close"),
                ]
                return opener

            data_opener = yahoo_opener()

            res = {}
            fd = {}
            et = []
            dks = {}
            price = {}

            now = int(time.time())
            statement_keys = (
                set(INCOME_STATEMENT_TYPES.keys())
                | set(BALANCE_STATEMENT_TYPES.keys())
                | set(CASH_FLOW_STATEMENT_TYPES.keys())
                | {
                    "CapitalExpenditure", "DepreciationAndAmortization",
                    "CashCashEquivalentsAndShortTermInvestments",
                    "OtherShortTermInvestments", "CashAndCashEquivalents",
                    "CurrentDebt", "LongTermDebt", "TotalDebt",
                }
            )
            type_names = [
                f"{period}{key}"
                for key in sorted(statement_keys)
                for period in ("annual", "quarterly")
            ]
            ts_res = []
            try:
                chunk_size = 35
                for idx in range(0, len(type_names), chunk_size):
                    ts_types = ",".join(type_names[idx:idx + chunk_size])
                    ts_url = (
                        f"https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{ticker}"
                        f"?symbol={ticker}&type={ts_types}&period1={now - 86400 * 365 * 6}&period2={now}"
                    )
                    ts_data = json.loads(self._counted_open(data_opener, ts_url, timeout=5).read().decode("utf-8"))
                    ts_res.extend((ts_data.get("timeseries", {}) or {}).get("result", []) or [])
            except Exception as e:
                print("Yahoo timeseries warning:", e)
                ts_res = []

            def series_points(type_name):
                for item in ts_res:
                    if self._statement_type_name(item) == type_name:
                        return sorted(self._series_points(item, type_name), key=lambda p: p["date"], reverse=True)
                return []

            def series_sum(type_name, absolute=False):
                points = series_points(type_name)
                if len(points) >= 4:
                    value = sum(point["raw"] for point in points[:4])
                elif points:
                    value = points[0]["raw"]
                else:
                    return 0.0
                return abs(value) if absolute else value

            def series_latest(type_name, absolute=False):
                points = series_points(type_name)
                if not points:
                    return 0.0
                value = points[0]["raw"]
                return abs(value) if absolute else value

            income_statement = self.build_income_statement_from_timeseries_results(ts_res)
            balance_statement = self.build_balance_sheet_from_timeseries_results(ts_res)
            cash_flow_statement = self.build_cash_flow_statement_from_timeseries_results(ts_res)

            if not income_statement.get("rows"):
                try:
                    income_statement = self.build_statement_from_stockanalysis_page(ticker, "income")
                except Exception as e:
                    print("StockAnalysis income warning:", e)
            if not balance_statement.get("rows"):
                try:
                    balance_statement = self.build_statement_from_stockanalysis_page(ticker, "balance")
                except Exception as e:
                    print("StockAnalysis balance warning:", e)
            if not cash_flow_statement.get("rows"):
                try:
                    cash_flow_statement = self.build_statement_from_stockanalysis_page(ticker, "cash")
                except Exception as e:
                    print("StockAnalysis cash flow warning:", e)

            try:
                income_statement = self._merge_statement_rows(
                    income_statement,
                    self.build_income_statement_from_page(ticker, data_opener),
                )
            except Exception:
                pass
            try:
                balance_statement = self._merge_statement_rows(
                    balance_statement,
                    self.build_balance_sheet_from_page(ticker, data_opener),
                )
            except Exception:
                pass
            try:
                cash_flow_statement = self._merge_statement_rows(
                    cash_flow_statement,
                    self.build_cash_flow_statement_from_page(ticker, data_opener),
                )
            except Exception:
                pass

            chart_meta = {}
            try:
                chart_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&interval=1d"
                chart = json.loads(self._counted_open(data_opener, chart_url, timeout=3).read().decode("utf-8"))
                chart_results = (chart.get("chart", {}) or {}).get("result", []) or []
                chart_meta = chart_results[0].get("meta", {}) if chart_results else {}
            except Exception:
                chart_meta = {}

            try:
                quote_opener = yahoo_opener()
                crumb = self.get_yahoo_crumb(quote_opener)
                modules = ",".join([
                    "financialData", "earningsTrend", "defaultKeyStatistics", "price",
                    "recommendationTrend",
                ])
                url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules={modules}&crumb={crumb}"
                data = json.loads(self._counted_open(quote_opener, url, timeout=3).read().decode("utf-8"))
                results = data.get("quoteSummary", {}).get("result") or []
                res = results[0] if results else {}
            except Exception as e:
                # Yahoo's quoteSummary endpoint is crumb/cookie sensitive. Keep going:
                # the timeseries and chart endpoints still provide the core dashboard.
                print("Yahoo quoteSummary warning:", e)

            fd = res.get("financialData", {}) or {}
            et = (res.get("earningsTrend", {}) or {}).get("trend", []) or []
            
            if not et:
                try:
                    analysis_url = f"https://finance.yahoo.com/quote/{ticker}/analyst-insights/"
                    html = self._counted_open(None, analysis_url, timeout=8).read().decode("utf-8", errors="ignore")
                    for match in re.finditer(r'<script[^>]*data-sveltekit-fetched[^>]*>(.*?)</script>', html, re.DOTALL):
                        try:
                            body = json.loads(match.group(1)).get("body", "{}")
                            if isinstance(body, str): body = json.loads(body)
                            et_fallback = body.get("quoteSummary", {}).get("result", [{}])[0].get("earningsTrend", {}).get("trend", [])
                            if et_fallback:
                                et = et_fallback
                                break
                        except Exception:
                            pass
                    if not et:
                        print("Fallback analysis page: no earningsTrend found in SvelteKit JSON.")
                except Exception as e:
                    print("Fallback analysis page error:", e)

            dks = res.get("defaultKeyStatistics", {}) or {}
            price = res.get("price", {}) or {}

            if not res and not ts_res and not chart_meta:
                if not income_statement.get("rows") and not balance_statement.get("rows") and not cash_flow_statement.get("rows"):
                    return self._empty_fetch_tuple(ticker)


            revenue_raw = series_sum("quarterlyTotalRevenue") or self._raw(fd.get("totalRevenue")) or self._latest_row_raw(income_statement, ["Total Revenue", "Revenue"])
            operating_income_raw = series_sum("quarterlyOperatingIncome") or self._latest_row_raw(income_statement, ["Operating Income"])
            capex_raw = series_sum("quarterlyCapitalExpenditure", absolute=True) or abs(self._latest_row_raw(cash_flow_statement, ["Capital Expenditures", "Capital Expenditure"]))
            da_raw = series_sum("quarterlyDepreciationAndAmortization") or self._latest_row_raw(cash_flow_statement, ["Depreciation And Amortization"])
            gross_ppe_raw = series_latest("annualGrossPPE") or self._latest_row_raw(balance_statement, ["Gross PP&E", "Gross PPE", "Property, Plant & Equipment", "Net PP&E", "Net PPE"])
            net_fixed_assets_raw = series_latest("annualNetPPE") or self._latest_row_raw(balance_statement, ["Net PP&E", "Net PPE", "Property, Plant & Equipment"])
            receivables_raw = series_latest("annualAccountsReceivable") or self._latest_row_raw(balance_statement, ["Accounts Receivable"])
            inventory_raw = series_latest("annualInventory") or self._latest_row_raw(balance_statement, ["Inventory"])
            accounts_payable_raw = series_latest("annualAccountsPayable") or self._latest_row_raw(balance_statement, ["Accounts Payable"])

            da_minus_capex_raw = max(da_raw - capex_raw, 0)
            investment_capex_raw = max(capex_raw - da_raw, 0)
            adj_income_raw = operating_income_raw + da_minus_capex_raw
            adj_margin_ratio = (adj_income_raw / revenue_raw) if revenue_raw else 0
            operating_margin_ratio = (operating_income_raw / revenue_raw) if revenue_raw else self._raw(fd.get("operatingMargins"))

            gross_margin_ratio = self._raw(fd.get("grossMargins"), None)
            if gross_margin_ratio is None and revenue_raw:
                gross_profit_raw = self._latest_row_raw(income_statement, ["Gross Profit"])
                gross_margin_ratio = gross_profit_raw / revenue_raw if gross_profit_raw else 0

            def annual_statement_points(statement, labels):
                labels_lower = {label.lower() for label in labels}
                periods = statement.get("periods", []) or []
                for row in statement.get("rows", []) or []:
                    if row.get("label", "").lower() not in labels_lower:
                        continue
                    points = []
                    for idx, period in enumerate(periods):
                        if idx >= len(row.get("values", [])):
                            continue
                        period_label = str(period).upper()
                        if period_label in ("TTM", "MRQ", "LATEST"):
                            continue
                        raw = self._parse_money_to_raw(row["values"][idx])
                        if raw:
                            points.append((period, raw))
                    return points
                return []

            def three_year_growth(statement):
                gross_points = annual_statement_points(statement, ["Gross Profit"])
                points = gross_points
                label = "3Y Annual GP Growth"
                if len(points) < 2:
                    points = annual_statement_points(statement, ["Total Revenue"])
                    label = "3Y Annual Sales Growth"
                if len(points) < 2:
                    return None, 0.0, 0.0, label
                def parse_period_date(period):
                    text = str(period)
                    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
                        try:
                            return datetime.datetime.strptime(text, fmt).date()
                        except ValueError:
                            continue
                    return None

                dated_points = [
                    (parse_period_date(period), idx, raw)
                    for idx, (period, raw) in enumerate(points)
                ]
                dated_points.sort(key=lambda point: (point[0] or datetime.date.min, point[1]))
                end_date, end_idx, end = dated_points[-1]
                start_date, start_idx, start = dated_points[max(0, len(dated_points) - 4)]
                if not start:
                    growth = None
                else:
                    years = (end_date - start_date).days / 365.25 if end_date and start_date else end_idx - start_idx
                    years = years if years > 0 else 1
                    growth = (end / abs(start)) ** (1 / years) - 1
                return growth, start, end, label

            def latest_annual_growth(statement, labels):
                def parse_period_date(period):
                    text = str(period)
                    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
                        try:
                            return datetime.datetime.strptime(text, fmt).date()
                        except ValueError:
                            continue
                    return None

                points = annual_statement_points(statement, labels)
                if len(points) < 2:
                    return None, 0.0
                dated_points = [
                    (parse_period_date(period), idx, raw)
                    for idx, (period, raw) in enumerate(points)
                ]
                dated_points.sort(key=lambda point: (point[0] or datetime.date.min, point[1]))
                _latest_date, _latest_idx, latest = dated_points[-1]
                _prior_date, _prior_idx, prior = dated_points[-2]
                if not prior:
                    return None, latest
                return (latest / abs(prior)) - 1, latest

            gp_3y_growth_raw, gp_3y_start_raw, gp_3y_end_raw, gp_3y_label = three_year_growth(income_statement)
            rnd_raw = self._latest_row_raw(income_statement, ["Research & Development", "Research and Development"])

            cy_revenue_raw = ny_revenue_raw = 0
            cy_growth_raw = ny_growth_raw = None
            cy_eps_raw = ny_eps_raw = year_ago_eps_raw = 0
            cy_eps_growth_raw = ny_eps_growth_raw = None
            for trend in et:
                revenue_est = trend.get("revenueEstimate", {}) or {}
                earnings_est = trend.get("earningsEstimate", {}) or {}
                if trend.get("period") == "0y":
                    cy_revenue_raw = self._raw(revenue_est.get("avg"))
                    cy_growth_raw = self._raw(revenue_est.get("growth"), None)
                    cy_eps_raw = self._eps_value(earnings_est.get("avg"))
                    year_ago_eps_raw = self._eps_value(earnings_est.get("yearAgoEps"))
                    cy_eps_growth_raw = self._raw(earnings_est.get("growth"), None)
                elif trend.get("period") == "+1y":
                    ny_revenue_raw = self._raw(revenue_est.get("avg"))
                    ny_growth_raw = self._raw(revenue_est.get("growth"), None)
                    ny_eps_raw = self._eps_value(earnings_est.get("avg"))
                    ny_eps_growth_raw = self._raw(earnings_est.get("growth"), None)
            if cy_eps_growth_raw is None:
                finviz_cy_eps = finviz_metrics.get("eps_this_y")
                if finviz_cy_eps and finviz_cy_eps != "--":
                    try:
                        cy_eps_growth_raw = float(finviz_cy_eps.strip('%')) / 100
                    except Exception:
                        pass
            if ny_eps_growth_raw is None:
                finviz_ny_eps = finviz_metrics.get("eps_next_y")
                if finviz_ny_eps and finviz_ny_eps != "--":
                    try:
                        ny_eps_growth_raw = float(finviz_ny_eps.strip('%')) / 100
                    except Exception:
                        pass
                        
            if cy_growth_raw is None or ny_growth_raw is None:
                try:
                    forecast_url = f"https://stockanalysis.com/stocks/{ticker.lower()}/forecast/"
                    html = self._counted_open(None, forecast_url, timeout=8).read().decode("utf-8", errors="ignore")
                    data_match = re.search(r"financialData:\{(.*?)\},map:\[", html, re.DOTALL)
                    if data_match:
                        data_str = data_match.group(1)
                        m_rev = re.search(r"revenueGrowth:\[(.*?)\]", data_str)
                        if m_rev:
                            arr = self._stockanalysis_array(m_rev.group(1))
                            if len(arr) > 0 and cy_growth_raw is None:
                                cy_growth_raw = arr[0]
                            if len(arr) > 1 and ny_growth_raw is None:
                                ny_growth_raw = arr[1]
                except Exception as e:
                    print("Fallback StockAnalysis forecast error:", e)

            if cy_growth_raw is None or ny_growth_raw is None or not cy_revenue_raw or not ny_revenue_raw:
                revenue_growth_fallback, latest_annual_revenue_raw = latest_annual_growth(
                    income_statement,
                    ["Total Revenue", "Revenue"],
                )
                revenue_base_raw = latest_annual_revenue_raw or revenue_raw
                if cy_growth_raw is None:
                    if cy_revenue_raw and revenue_base_raw:
                        cy_growth_raw = (cy_revenue_raw / abs(revenue_base_raw)) - 1
                    elif revenue_growth_fallback is not None:
                        cy_growth_raw = revenue_growth_fallback
                if not cy_revenue_raw and revenue_base_raw and cy_growth_raw is not None:
                    cy_revenue_raw = revenue_base_raw * (1 + cy_growth_raw)
                if ny_growth_raw is None and ny_revenue_raw and cy_revenue_raw:
                    ny_growth_raw = (ny_revenue_raw / abs(cy_revenue_raw)) - 1
                if not ny_revenue_raw and cy_revenue_raw and ny_growth_raw is not None:
                    ny_revenue_raw = cy_revenue_raw * (1 + ny_growth_raw)

            statement_currency = "USD"
            for item in ts_res:
                type_name = self._statement_type_name(item)
                points = self._series_points(item, type_name) if type_name else []
                if points:
                    raw_points = item.get(type_name, []) or []
                    if raw_points:
                        statement_currency = (
                            raw_points[0].get("currencyCode")
                            or statement_currency
                        )
                        break

            financial_currency = (
                fd.get("financialCurrency")
                or price.get("currency")
                or chart_meta.get("currency")
                or statement_currency
                or "USD"
            ).upper()
            quote_currency = (price.get("currency") or chart_meta.get("currency") or "USD").upper()
            usd_fx_rate = self.get_usd_fx_rate(financial_currency, data_opener)
            eps_fx = usd_fx_rate if quote_currency == "USD" and financial_currency != "USD" else 1.0
            cy_eps_raw *= eps_fx
            ny_eps_raw *= eps_fx
            year_ago_eps_raw *= eps_fx

            if cy_eps_raw and year_ago_eps_raw and abs(cy_eps_raw - year_ago_eps_raw) < 1e-9:
                eps_row = next((row for row in income_statement.get("rows", []) if row.get("label") in ("Diluted EPS", "Basic EPS")), None)
                if eps_row and len(eps_row.get("values", [])) > 1:
                    fallback = self._parse_money_to_raw(eps_row["values"][1])
                    if fallback:
                        year_ago_eps_raw = fallback

            market_cap_raw = float(finviz_market_cap_raw or 0) or self._raw(price.get("marketCap")) or self._raw(dks.get("marketCap"))
            cash_bucket_raw = self._latest_row_raw(balance_statement, ["Cash, Equivalents & Short Term Investments", "Cash & Short Term Investments"])
            if not cash_bucket_raw:
                cash_bucket_raw = self._latest_row_raw(balance_statement, ["Cash & Cash Equivalents", "Cash And Cash Equivalents"]) + self._latest_row_raw(balance_statement, ["Other Short Term Investments", "Short Term Investments"])
            total_debt_raw = self._latest_row_raw(balance_statement, ["Total Debt"])
            if not total_debt_raw:
                total_debt_raw = self._latest_row_raw(balance_statement, ["Current Debt", "Short Term Debt"]) + self._latest_row_raw(balance_statement, ["Long Term Debt"])
            net_cash_raw = cash_bucket_raw - total_debt_raw if cash_bucket_raw or total_debt_raw else (market_cap_raw - float(finviz_ev_raw or 0) if finviz_ev_raw and market_cap_raw else 0)
            derived_enterprise_value_raw = market_cap_raw - net_cash_raw if market_cap_raw else 0

            valuation_raw = float(finviz_ev_raw or 0)
            valuation_basis = "enterpriseValue" if valuation_raw else "marketCap"
            valuation_prefix = "EV" if valuation_raw else "Mkt Cap"
            valuation_numerator_label = "Current Enterprise Value" if valuation_raw else "Current Market Cap"
            if not valuation_raw:
                valuation_raw = market_cap_raw

            cy_adj_inc_raw = cy_revenue_raw * adj_margin_ratio if cy_revenue_raw and adj_margin_ratio else 0
            ny_adj_inc_raw = ny_revenue_raw * adj_margin_ratio if ny_revenue_raw and adj_margin_ratio else 0
            nwc_raw = receivables_raw + inventory_raw - accounts_payable_raw
            roc_denominator_raw = nwc_raw + net_fixed_assets_raw
            current_price_raw = (
                self._raw(price.get("regularMarketPrice"))
                or self._raw(fd.get("currentPrice"))
                or chart_meta.get("regularMarketPrice")
                or 0
            )
            target_mean_raw = self._raw(fd.get("targetMeanPrice"))
            target_low_raw = self._raw(fd.get("targetLowPrice"))
            target_high_raw = self._raw(fd.get("targetHighPrice"))
            target_move_raw = ((target_mean_raw - current_price_raw) / current_price_raw) if target_mean_raw and current_price_raw else None

            analyst_recommendations = ((res.get("recommendationTrend", {}) or {}).get("trend", []) or [{}])[0]
            company_name = price.get("longName") or price.get("shortName") or chart_meta.get("longName") or chart_meta.get("shortName") or ticker

            values = {
                "income": self._format_money(operating_income_raw),
                "margin": self._format_percent(adj_margin_ratio) if adj_margin_ratio else "--",
                "gross_margin": self._format_percent(gross_margin_ratio) if gross_margin_ratio is not None else "--",
                "ev_cy_ebit": self._format_3sig(valuation_raw / cy_adj_inc_raw) if valuation_raw and cy_adj_inc_raw else "--",
                "ev_ny_ebit": self._format_3sig(valuation_raw / ny_adj_inc_raw) if valuation_raw and ny_adj_inc_raw else "--",
                "adj_income": self._format_money(adj_income_raw),
                "capex": self._format_money(capex_raw),
                "da": self._format_money(da_raw),
                "ev": self._format_money(valuation_raw),
                "ev_adj_ebit": self._format_3sig(valuation_raw / adj_income_raw) if valuation_raw and adj_income_raw else "--",
                "cy_growth": self._format_percent(cy_growth_raw) if cy_growth_raw is not None else "--",
                "ny_growth": self._format_percent(ny_growth_raw) if ny_growth_raw is not None else "--",
                "gp_3y_growth": self._format_percent(gp_3y_growth_raw) if gp_3y_growth_raw is not None else "--",
                "gp_3y_start": self._format_money(gp_3y_start_raw) if gp_3y_start_raw else "--",
                "gp_3y_end": self._format_money(gp_3y_end_raw) if gp_3y_end_raw else "--",
                "gp_3y_label": gp_3y_label,
                "rnd_adj_income": self._format_percent(rnd_raw / adj_income_raw) if rnd_raw and adj_income_raw else "--",
                "cy_adj_inc": self._format_money(cy_adj_inc_raw),
                "ny_adj_inc": self._format_money(ny_adj_inc_raw),
                "market_cap": self._format_money(market_cap_raw),
                "net_cash": self._format_money(net_cash_raw),
                "derived_enterprise_value": self._format_money(derived_enterprise_value_raw),
                "revenue": self._format_money(revenue_raw),
                "operating_margin": self._format_percent(operating_margin_ratio) if operating_margin_ratio else "--",
                "da_minus_capex": self._format_money(da_minus_capex_raw) if da_minus_capex_raw else "0",
                "cy_revenue": self._format_money(cy_revenue_raw),
                "ny_revenue": self._format_money(ny_revenue_raw),
                "gross_ppe": self._format_money(gross_ppe_raw),
                "adj_ebit_gross_ppe": self._format_percent(adj_income_raw / gross_ppe_raw) if adj_income_raw and gross_ppe_raw else "--",
                "capex_adj_income": self._format_percent(investment_capex_raw / adj_income_raw) if adj_income_raw else "--",
                "investment_capex": self._format_money(investment_capex_raw) if investment_capex_raw else "0",
                "roc": self._format_percent(adj_income_raw / roc_denominator_raw) if adj_income_raw and roc_denominator_raw else "--",
                "net_working_capital": self._format_money(nwc_raw),
                "net_fixed_assets": self._format_money(net_fixed_assets_raw),
                "receivables": self._format_money(receivables_raw),
                "inventory": self._format_money(inventory_raw),
                "accounts_payable": self._format_money(accounts_payable_raw),
                "financial_currency": financial_currency,
                "usd_fx_rate": usd_fx_rate,
                "company_name": company_name,
                "income_statement": income_statement,
                "balance_statement": balance_statement,
                "cash_flow_statement": cash_flow_statement,
                "current_price": self._format_3sig(current_price_raw),
                "target_mean_price": self._format_3sig(target_mean_raw),
                "target_low_price": self._format_3sig(target_low_raw),
                "target_high_price": self._format_3sig(target_high_raw),
                "target_move": self._format_percent(target_move_raw) if target_move_raw is not None else "--",
                "recommendation_mean": self._format_3sig(self._raw(fd.get("recommendationMean"))),
                "recommendation_key": fd.get("recommendationKey") or "--",
                "analyst_recommendations": analyst_recommendations,
                "valuation_basis": valuation_basis,
                "valuation_prefix": valuation_prefix,
                "valuation_numerator_label": valuation_numerator_label,
                "current_year_eps": self._format_3sig(cy_eps_raw),
                "next_year_eps": self._format_3sig(ny_eps_raw),
                "year_ago_eps": self._format_3sig(year_ago_eps_raw),
                "current_year_eps_growth": self._format_percent(cy_eps_growth_raw) if cy_eps_growth_raw is not None else "--",
                "next_year_eps_growth": self._format_percent(ny_eps_growth_raw) if ny_eps_growth_raw is not None else "--",
                "price_current_eps": self._format_3sig(current_price_raw / year_ago_eps_raw) if current_price_raw and year_ago_eps_raw else "--",
                "price_cy_eps": self._format_3sig(current_price_raw / cy_eps_raw) if current_price_raw and cy_eps_raw else "--",
                "price_ny_eps": self._format_3sig(current_price_raw / ny_eps_raw) if current_price_raw and ny_eps_raw else "--",
            }
            return tuple(values[key] for key in FETCH_RESULT_FIELDS)
        except Exception as e:
            print("Yahoo error:", e)
            return self._empty_fetch_tuple(ticker)

    def handle_api_request(self, ticker, refresh=False):
        if not ticker:
            self._send_response(400, {"error": "Ticker is required"})
            return

        self._request_fetch_count = 0
        cache = load_cache()
        today = datetime.date.today().isoformat()
        now_dt = datetime.datetime.now()
        pulled_at = now_dt.isoformat(timespec="seconds")

        if ticker.upper() == "TEST":
            self._send_response(200, self.build_test_payload(pulled_at=pulled_at))
            return

        def cache_has_missing_ttm_anchor(payload):
            statement = payload.get("incomeStatement") or {}
            periods = statement.get("periods") or []
            if not periods or periods[0] != "TTM":
                return False
            labels = {"Total Revenue", "Gross Profit", "Operating Income"}
            for row in statement.get("rows", []):
                if row.get("label") in labels:
                    values = row.get("values") or []
                    if values and values[0] in (None, "", "--"):
                        return True
            return False

        def cache_is_usable(payload):
            return (
                isinstance(payload, dict)
                and payload.get("payloadVersion") == PAYLOAD_VERSION
                and payload.get("marketCap") not in (None, "", "--")
                and payload.get("incomeStatement")
                and payload.get("balanceStatement")
                and payload.get("cashFlowStatement")
                and not cache_has_missing_ttm_anchor(payload)
            )

        if not refresh and ticker in cache and cache[ticker].get('date') == today:
            cached_payload = cache[ticker].get('data', {})
            if cache_is_usable(cached_payload):
                if 'dataDate' not in cached_payload:
                    cached_payload = {**cached_payload, "dataDate": cache[ticker].get('date', today)}
                if 'pulledAt' not in cached_payload or not cached_payload.get('pulledAt'):
                    cached_payload = {**cached_payload, "pulledAt": cache[ticker].get('pulledAt')}
                cached_payload = {**cached_payload, "fetchCount": 0}
                self._send_response(200, cached_payload)
                return
        elif refresh and ticker in cache:
            # Force refresh: drop cached entry so a new fetch overwrites it.
            try:
                del cache[ticker]
                save_cache(cache)
            except Exception:
                pass

        finviz_metrics = {"short_float": "--", "market_cap": "--", "enterprise_value": "--"}
        try:
            finviz_metrics = self.fetch_finviz_snapshot_metrics(ticker)
        except HTTPError as e:
            if e.code != 404:
                print("Finviz error:", e)
        except Exception as e:
            print("Finviz error:", e)

        finviz_market_cap_raw = self._parse_finviz_abbrev_to_raw(finviz_metrics.get("market_cap"))
        finviz_enterprise_value_raw = self._parse_finviz_abbrev_to_raw(finviz_metrics.get("enterprise_value"))
        result = dict(zip(FETCH_RESULT_FIELDS, self.fetch_yahoo_finance_data(
            ticker,
            finviz_ev_raw=finviz_enterprise_value_raw,
            finviz_market_cap_raw=finviz_market_cap_raw,
            finviz_metrics=finviz_metrics,
        )))

        if result.get("company_name") == ticker and result.get("valuation_basis") == "unavailable":
            self._send_response(404, {"error": "Data not found for this ticker."})
            return

        payload = {
            "ticker": ticker,
            "shortFloat": finviz_metrics.get("short_float") or "--",
            "income": result["income"],
            "margin": result["margin"],
            "grossMargin": result["gross_margin"],
            "ev_cy_ebit": result["ev_cy_ebit"],
            "ev_ny_ebit": result["ev_ny_ebit"],
            "adj_income": result["adj_income"],
            "capex": result["capex"],
            "da": result["da"],
            "ev": result["ev"],
            "ev_adj_ebit": result["ev_adj_ebit"],
            "cy_growth": result["cy_growth"],
            "ny_growth": result["ny_growth"],
            "gp_3y_growth": result["gp_3y_growth"],
            "gp_3y_start": result["gp_3y_start"],
            "gp_3y_end": result["gp_3y_end"],
            "gp_3y_label": result["gp_3y_label"],
            "rndAdjIncome": result["rnd_adj_income"],
            "cy_adj_inc": result["cy_adj_inc"],
            "ny_adj_inc": result["ny_adj_inc"],
            "marketCap": result["market_cap"],
            "netCash": result["net_cash"],
            "derivedEnterpriseValue": result["derived_enterprise_value"],
            "revenue": result["revenue"],
            "operating_margin": result["operating_margin"],
            "da_minus_capex": result["da_minus_capex"],
            "cy_revenue": result["cy_revenue"],
            "ny_revenue": result["ny_revenue"],
            "grossPpe": result["gross_ppe"],
            "adjEbitGrossPpe": result["adj_ebit_gross_ppe"],
            "capexAdjIncome": result["capex_adj_income"],
            "investmentCapex": result["investment_capex"],
            "roc": result["roc"],
            "netWorkingCapital": result["net_working_capital"],
            "netFixedAssets": result["net_fixed_assets"],
            "receivables": result["receivables"],
            "inventory": result["inventory"],
            "accountsPayable": result["accounts_payable"],
            "financialCurrency": result["financial_currency"],
            "usdFxRate": result["usd_fx_rate"],
            "companyName": result["company_name"],
            "incomeStatement": result["income_statement"],
            "balanceStatement": result["balance_statement"],
            "cashFlowStatement": result["cash_flow_statement"],
            "currentPrice": result["current_price"],
            "targetMeanPrice": result["target_mean_price"],
            "targetLowPrice": result["target_low_price"],
            "targetHighPrice": result["target_high_price"],
            "targetMove": result["target_move"],
            "recommendationMean": result["recommendation_mean"],
            "recommendationKey": result["recommendation_key"],
            "analystRecommendations": result["analyst_recommendations"],
            "valuationBasis": result["valuation_basis"],
            "valuationPrefix": result["valuation_prefix"],
            "valuationNumeratorLabel": result["valuation_numerator_label"],
            "currentYearEps": result["current_year_eps"],
            "nextYearEps": result["next_year_eps"],
            "yearAgoEps": result["year_ago_eps"],
            "currentYearEpsGrowth": result["current_year_eps_growth"],
            "nextYearEpsGrowth": result["next_year_eps_growth"],
            "priceCurrentEps": result["price_current_eps"],
            "priceCyEps": result["price_cy_eps"],
            "priceNyEps": result["price_ny_eps"],
            "payloadVersion": PAYLOAD_VERSION,
            "evSource": "finviz" if finviz_enterprise_value_raw and result["valuation_basis"] == "enterpriseValue" else "unavailable",
            "marketCapSource": "yahoo",
            "dataDate": today,
            "pulledAt": pulled_at,
            "fetchCount": getattr(self, "_request_fetch_count", 0),
        }

        cache[ticker] = {'date': today, 'pulledAt': pulled_at, 'data': payload}
        save_cache(cache)
        self._send_response(200, payload)

    def _send_response(self, status, payload):
        self.send_response(status)
        self.send_header("Content-type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode('utf-8'))

class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True
    def server_bind(self):
        import socket
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass
        self.socket.bind(self.server_address)

if __name__ == '__main__':
    with ReusableTCPServer(("127.0.0.1", PORT), Handler) as httpd:
        print("Serving at port", PORT)
        httpd.serve_forever()
