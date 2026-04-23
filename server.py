import http.server
import socketserver
import urllib.request
import re
import json
import os
import sqlite3
import datetime
import time
import io
import subprocess
import html as html_lib
from urllib.parse import urlparse, parse_qs
from urllib.error import URLError, HTTPError

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

PORT = int(os.environ.get("PORT", "3000"))
CACHE_DB_FILE = os.environ.get("CACHE_DB_FILE", "cache.db")
LEGACY_CACHE_FILE = "cache.json"
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "900"))
PAYLOAD_VERSION = 9
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

# Ordered dicts: keys define the preferred display order.
# Include BOTH CamelCase (timeseries API) and spaced (yfinance DataFrame) variants.
INCOME_STATEMENT_TYPES = {
    # Revenue & Cost
    "TotalRevenue": "Total Revenue",
    "Total Revenue": "Total Revenue",
    "Operating Revenue": "Operating Revenue",
    "CostOfRevenue": "Cost of Revenue",
    "Cost Of Revenue": "Cost of Revenue",
    "Reconciled Cost Of Revenue": "Cost of Revenue (Reconciled)",
    "GrossProfit": "Gross Profit",
    "Gross Profit": "Gross Profit",
    # Operating expenses
    "ResearchAndDevelopment": "Research & Development",
    "Research And Development": "Research & Development",
    "SellingGeneralAndAdministration": "Selling, General & Administrative",
    "Selling General And Administration": "Selling, General & Administrative",
    "SellingAndMarketingExpense": "Selling & Marketing Expense",
    "Selling And Marketing Expense": "Selling & Marketing Expense",
    "GeneralAndAdministrativeExpense": "General & Administrative Expense",
    "General And Administrative Expense": "General & Administrative Expense",
    "OtherGandA": "Other G&A",
    "Other Gand A": "Other G&A",
    "OtherOperatingExpenses": "Other Operating Expenses",
    "OperatingExpense": "Operating Expense",
    "Operating Expense": "Operating Expense",
    "TotalExpenses": "Total Expenses",
    "Total Expenses": "Total Expenses",
    # Operating income
    "OperatingIncome": "Operating Income",
    "Operating Income": "Operating Income",
    "TotalOperatingIncomeAsReported": "Total Operating Income As Reported",
    "Total Operating Income As Reported": "Total Operating Income As Reported",
    # Interest & non-operating
    "InterestExpense": "Interest Expense",
    "Interest Expense": "Interest Expense",
    "Interest Expense Non Operating": "Interest Expense Non Operating",
    "InterestIncome": "Interest Income",
    "Interest Income": "Interest Income",
    "Interest Income Non Operating": "Interest Income Non Operating",
    "NetInterestIncome": "Net Interest Income",
    "Net Interest Income": "Net Interest Income",
    "NetNonOperatingInterestIncomeExpense": "Net Non Operating Interest Income Expense",
    "Net Non Operating Interest Income Expense": "Net Non Operating Interest Income Expense",
    "OtherIncomeExpense": "Other Income Expense",
    "Other Income Expense": "Other Income Expense",
    "Other Non Operating Income Expenses": "Other Non Operating Income Expenses",
    "SpecialIncomeCharges": "Special Income Charges",
    "Special Income Charges": "Special Income Charges",
    "Gain On Sale Of Security": "Gain On Sale Of Security",
    # Pre-tax & tax
    "PretaxIncome": "Pretax Income",
    "Pretax Income": "Pretax Income",
    "TaxProvision": "Tax Provision",
    "Tax Provision": "Tax Provision",
    "TaxRateForCalcs": "Tax Rate For Calcs",
    "Tax Rate For Calcs": "Tax Rate For Calcs",
    # Net income
    "NetIncome": "Net Income",
    "Net Income": "Net Income",
    "Net Income Continuous Operations": "Net Income Continuous Operations",
    "Net Income Including Noncontrolling Interests": "Net Income Including Noncontrolling Interests",
    "NetIncomeCommonStockholders": "Net Income Common Stockholders",
    "Net Income Common Stockholders": "Net Income Common Stockholders",
    "Net Income From Continuing Operation Net Minority Interest": "Net Income From Continuing Operations",
    "Net Income From Continuing And Discontinued Operation": "Net Income (Continuing & Discontinued)",
    "Diluted NI Availto Com Stockholders": "Diluted NI Avail to Common Stockholders",
    "Normalized Income": "Normalized Income",
    # Shares & EPS
    "DilutedAverageShares": "Diluted Average Shares",
    "Diluted Average Shares": "Diluted Average Shares",
    "BasicAverageShares": "Basic Average Shares",
    "Basic Average Shares": "Basic Average Shares",
    "DilutedEPS": "Diluted EPS",
    "Diluted EPS": "Diluted EPS",
    "BasicEPS": "Basic EPS",
    "Basic EPS": "Basic EPS",
    # EBIT/EBITDA
    "EBIT": "EBIT",
    "EBITDA": "EBITDA",
    "Normalized EBITDA": "Normalized EBITDA",
    "Reconciled Depreciation": "Reconciled Depreciation",
    # Unusual items
    "TotalUnusualItems": "Total Unusual Items",
    "Total Unusual Items": "Total Unusual Items",
    "TotalUnusualItemsExcludingGoodwill": "Total Unusual Items Excluding Goodwill",
    "Total Unusual Items Excluding Goodwill": "Total Unusual Items Excluding Goodwill",
    "Tax Effect Of Unusual Items": "Tax Effect Of Unusual Items",
    "WriteOff": "Write Off",
    "Write Off": "Write Off",
}

BALANCE_STATEMENT_TYPES = {
    # Cash & investments
    "Cash And Cash Equivalents": "Cash & Cash Equivalents",
    "CashAndCashEquivalents": "Cash & Cash Equivalents",
    "Cash Equivalents": "Cash Equivalents",
    "Cash Financial": "Cash Financial",
    "Other Short Term Investments": "Other Short Term Investments",
    "OtherShortTermInvestments": "Other Short Term Investments",
    "Cash Cash Equivalents And Short Term Investments": "Cash, Equivalents & Short Term Investments",
    "CashCashEquivalentsAndShortTermInvestments": "Cash, Equivalents & Short Term Investments",
    # Receivables & inventory
    "Accounts Receivable": "Accounts Receivable",
    "AccountsReceivable": "Accounts Receivable",
    "Gross Accounts Receivable": "Gross Accounts Receivable",
    "Allowance For Doubtful Accounts Receivable": "Allowance For Doubtful Accounts Receivable",
    "Receivables": "Receivables",
    "Inventory": "Inventory",
    "Raw Materials": "Raw Materials",
    "Work In Process": "Work In Process",
    "Finished Goods": "Finished Goods",
    # Current assets
    "Other Current Assets": "Other Current Assets",
    "Hedging Assets Current": "Hedging Assets Current",
    "Current Assets": "Current Assets",
    # PP&E
    "Gross PPE": "Gross PP&E",
    "GrossPPE": "Gross PP&E",
    "Land And Improvements": "Land & Improvements",
    "Buildings And Improvements": "Buildings & Improvements",
    "Machinery Furniture Equipment": "Machinery, Furniture & Equipment",
    "Other Properties": "Other Properties",
    "Leases": "Leases",
    "Properties": "Properties",
    "Accumulated Depreciation": "Accumulated Depreciation",
    "Net PPE": "Net PP&E",
    "NetPPE": "Net PP&E",
    # Intangibles & investments
    "Goodwill": "Goodwill",
    "Other Intangible Assets": "Other Intangible Assets",
    "Goodwill And Other Intangible Assets": "Goodwill & Other Intangible Assets",
    "Investments And Advances": "Investments & Advances",
    "Long Term Equity Investment": "Long Term Equity Investment",
    "Available For Sale Securities": "Available For Sale Securities",
    "Investmentin Financial Assets": "Investment in Financial Assets",
    "Financial Assets": "Financial Assets",
    "Other Non Current Assets": "Other Non Current Assets",
    "Total Non Current Assets": "Total Non Current Assets",
    "TotalAssets": "Total Assets",
    "Total Assets": "Total Assets",
    # Payables & current liabilities
    "Accounts Payable": "Accounts Payable",
    "AccountsPayable": "Accounts Payable",
    "Income Tax Payable": "Income Tax Payable",
    "Total Tax Payable": "Total Tax Payable",
    "Payables": "Payables",
    "Payables And Accrued Expenses": "Payables & Accrued Expenses",
    "Current Deferred Revenue": "Current Deferred Revenue",
    "Current Deferred Liabilities": "Current Deferred Liabilities",
    "Other Current Liabilities": "Other Current Liabilities",
    "Current Liabilities": "Current Liabilities",
    # Debt
    "Current Debt": "Current Debt",
    "CurrentDebt": "Current Debt",
    "Current Debt And Capital Lease Obligation": "Current Debt & Capital Lease Obligation",
    "Commercial Paper": "Commercial Paper",
    "Other Current Borrowings": "Other Current Borrowings",
    "Long Term Debt": "Long Term Debt",
    "LongTermDebt": "Long Term Debt",
    "Long Term Capital Lease Obligation": "Long Term Capital Lease Obligation",
    "Long Term Debt And Capital Lease Obligation": "Long Term Debt & Capital Lease Obligation",
    "Capital Lease Obligations": "Capital Lease Obligations",
    "Total Debt": "Total Debt",
    "TotalDebt": "Total Debt",
    "Net Debt": "Net Debt",
    # Non-current liabilities
    "Non Current Deferred Taxes Liabilities": "Non Current Deferred Tax Liabilities",
    "Non Current Deferred Revenue": "Non Current Deferred Revenue",
    "Non Current Deferred Liabilities": "Non Current Deferred Liabilities",
    "Other Non Current Liabilities": "Other Non Current Liabilities",
    "Tradeand Other Payables Non Current": "Trade & Other Payables Non Current",
    "Total Non Current Liabilities Net Minority Interest": "Total Non Current Liabilities",
    "TotalLiabilitiesNetMinorityInterest": "Total Liabilities",
    "Total Liabilities Net Minority Interest": "Total Liabilities",
    # Equity
    "Common Stock": "Common Stock",
    "Capital Stock": "Capital Stock",
    "Retained Earnings": "Retained Earnings",
    "Other Equity Adjustments": "Other Equity Adjustments",
    "Gains Losses Not Affecting Retained Earnings": "Gains/Losses Not Affecting Retained Earnings",
    "StockholdersEquity": "Stockholders Equity",
    "Stockholders Equity": "Stockholders Equity",
    "Common Stock Equity": "Common Stock Equity",
    "Total Equity Gross Minority Interest": "Total Equity Gross Minority Interest",
    # Summary
    "Tangible Book Value": "Tangible Book Value",
    "Net Tangible Assets": "Net Tangible Assets",
    "Working Capital": "Working Capital",
    "Invested Capital": "Invested Capital",
    "Total Capitalization": "Total Capitalization",
    "Share Issued": "Shares Issued",
    "Ordinary Shares Number": "Ordinary Shares Number",
}

CASH_FLOW_STATEMENT_TYPES = {
    # Operating
    "Net Income From Continuing Operations": "Net Income From Continuing Operations",
    "Operating Gains Losses": "Operating Gains/Losses",
    "Gain Loss On Investment Securities": "Gain/Loss On Investment Securities",
    "DepreciationAndAmortization": "Depreciation & Amortization",
    "Depreciation And Amortization": "Depreciation & Amortization",
    "Depreciation Amortization Depletion": "Depreciation, Amortization & Depletion",
    "Depreciation": "Depreciation",
    "Deferred Income Tax": "Deferred Income Tax",
    "Deferred Tax": "Deferred Tax",
    "Asset Impairment Charge": "Asset Impairment Charge",
    "Unrealized Gain Loss On Investment Securities": "Unrealized Gain/Loss On Investments",
    "Stock Based Compensation": "Stock Based Compensation",
    "Change In Receivables": "Change In Receivables",
    "Changes In Account Receivables": "Changes In Account Receivables",
    "Change In Inventory": "Change In Inventory",
    "Change In Account Payable": "Change In Account Payable",
    "Change In Payable": "Change In Payable",
    "Change In Payables And Accrued Expense": "Change In Payables & Accrued Expense",
    "Change In Tax Payable": "Change In Tax Payable",
    "Change In Income Tax Payable": "Change In Income Tax Payable",
    "Change In Other Current Assets": "Change In Other Current Assets",
    "Change In Other Current Liabilities": "Change In Other Current Liabilities",
    "Change In Other Working Capital": "Change In Other Working Capital",
    "Change In Working Capital": "Change In Working Capital",
    "OperatingCashFlow": "Operating Cash Flow",
    "Operating Cash Flow": "Operating Cash Flow",
    "Cash Flow From Continuing Operating Activities": "Cash Flow From Continuing Operating Activities",
    # Investing
    "CapitalExpenditure": "Capital Expenditures",
    "Capital Expenditure": "Capital Expenditures",
    "Purchase Of PPE": "Purchase Of PP&E",
    "Net PPE Purchase And Sale": "Net PP&E Purchase & Sale",
    "Purchase Of Business": "Purchase Of Business",
    "Net Business Purchase And Sale": "Net Business Purchase & Sale",
    "Purchase Of Investment": "Purchase Of Investment",
    "Sale Of Investment": "Sale Of Investment",
    "Net Investment Purchase And Sale": "Net Investment Purchase & Sale",
    "Net Other Investing Changes": "Net Other Investing Changes",
    "Investing Cash Flow": "Investing Cash Flow",
    "Cash Flow From Continuing Investing Activities": "Cash Flow From Continuing Investing Activities",
    # Financing
    "Long Term Debt Issuance": "Long Term Debt Issuance",
    "Long Term Debt Payments": "Long Term Debt Payments",
    "Net Long Term Debt Issuance": "Net Long Term Debt Issuance",
    "Short Term Debt Issuance": "Short Term Debt Issuance",
    "Net Short Term Debt Issuance": "Net Short Term Debt Issuance",
    "Net Issuance Payments Of Debt": "Net Issuance/Payments Of Debt",
    "Issuance Of Debt": "Issuance Of Debt",
    "Repayment Of Debt": "Repayment Of Debt",
    "Common Stock Issuance": "Common Stock Issuance",
    "Common Stock Payments": "Common Stock Payments",
    "Net Common Stock Issuance": "Net Common Stock Issuance",
    "IssuanceOfCapitalStock": "Issuance Of Capital Stock",
    "Issuance Of Capital Stock": "Issuance Of Capital Stock",
    "RepurchaseOfCapitalStock": "Repurchase Of Capital Stock",
    "Repurchase Of Capital Stock": "Repurchase Of Capital Stock",
    "CashDividendsPaid": "Cash Dividends Paid",
    "Cash Dividends Paid": "Cash Dividends Paid",
    "Common Stock Dividend Paid": "Common Stock Dividend Paid",
    "Net Other Financing Charges": "Net Other Financing Charges",
    "Financing Cash Flow": "Financing Cash Flow",
    "Cash Flow From Continuing Financing Activities": "Cash Flow From Continuing Financing Activities",
    # Summary
    "Changes In Cash": "Changes In Cash",
    "Effect Of Exchange Rate Changes": "Effect Of Exchange Rate Changes",
    "Beginning Cash Position": "Beginning Cash Position",
    "End Cash Position": "End Cash Position",
    "FreeCashFlow": "Free Cash Flow",
    "Free Cash Flow": "Free Cash Flow",
}

def init_cache_db(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ticker_cache (
            ticker TEXT PRIMARY KEY,
            data_date TEXT NOT NULL,
            pulled_at TEXT,
            payload_version INTEGER,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ticker_cache_data_date ON ticker_cache(data_date)"
    )


def load_legacy_cache():
    if os.path.exists(LEGACY_CACHE_FILE):
        try:
            with open(LEGACY_CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def migrate_legacy_cache_if_needed(conn):
    try:
        row = conn.execute("SELECT COUNT(*) FROM ticker_cache").fetchone()
        if row and row[0]:
            return
        legacy_cache = load_legacy_cache()
        if legacy_cache:
            write_cache_rows(conn, legacy_cache)
    except Exception:
        pass


def write_cache_rows(conn, cache_data):
    now = datetime.datetime.now().isoformat()
    rows = []
    for ticker, entry in (cache_data or {}).items():
        if not isinstance(entry, dict):
            continue
        payload = entry.get("data", {})
        rows.append(
            (
                str(ticker).upper(),
                entry.get("date") or datetime.date.today().isoformat(),
                entry.get("pulledAt"),
                payload.get("payloadVersion") if isinstance(payload, dict) else None,
                json.dumps(payload),
                now,
            )
        )

    conn.executemany(
        """
        INSERT OR REPLACE INTO ticker_cache
            (ticker, data_date, pulled_at, payload_version, payload_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def load_cache():
    try:
        with sqlite3.connect(CACHE_DB_FILE) as conn:
            init_cache_db(conn)
            migrate_legacy_cache_if_needed(conn)
            cache = {}
            for ticker, data_date, pulled_at, payload_json in conn.execute(
                """
                SELECT ticker, data_date, pulled_at, payload_json
                FROM ticker_cache
                ORDER BY ticker
                """
            ):
                try:
                    payload = json.loads(payload_json)
                except Exception:
                    payload = {}
                cache[ticker] = {
                    "date": data_date,
                    "pulledAt": pulled_at,
                    "data": payload,
                }
            return cache
    except Exception as exc:
        print(f"Cache DB read failed: {exc}")
        return {}


def save_cache(cache_data):
    try:
        with sqlite3.connect(CACHE_DB_FILE) as conn:
            init_cache_db(conn)
            conn.execute("DELETE FROM ticker_cache")
            write_cache_rows(conn, cache_data)
    except Exception as exc:
        print(f"Cache DB write failed: {exc}")

class Handler(http.server.SimpleHTTPRequestHandler):
    _yahoo_crumb_cache = None
    _yahoo_crumb_cache_at = 0

    def build_test_payload(self, pulled_at=None):
        today = datetime.date.today().isoformat()
        pulled_at = pulled_at or datetime.datetime.now().isoformat(timespec="seconds")
        income_statement = {
            "annual": {
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
            },
            "quarterly": {
                "periods": ["LATEST", "2026-03-31", "2025-12-31", "2025-09-30", "2025-06-30", "2025-03-31"],
                "rows": [
                    {"label": "Total Revenue", "values": ["27B", "27B", "26B", "24.5B", "23B", "22.5B"]},
                    {"label": "Cost of Revenue", "values": ["10.8B", "10.8B", "10.4B", "9.8B", "9.2B", "9B"]},
                    {"label": "Gross Profit", "values": ["16.2B", "16.2B", "15.6B", "14.7B", "13.8B", "13.5B"]},
                    {"label": "Research & Development", "values": ["3.2B", "3.2B", "3.1B", "2.9B", "2.8B", "2.7B"]},
                    {"label": "Selling, General & Administrative", "values": ["3.7B", "3.7B", "3.5B", "3.4B", "3.3B", "3.2B"]},
                    {"label": "Operating Income", "values": ["8.1B", "8.1B", "7.8B", "7.2B", "6.8B", "6.5B"]},
                    {"label": "Interest Expense", "values": ["310M", "310M", "300M", "290M", "280M", "270M"]},
                    {"label": "Pretax Income", "values": ["7.9B", "7.9B", "7.6B", "7B", "6.6B", "6.3B"]},
                    {"label": "Tax Provision", "values": ["1.58B", "1.58B", "1.52B", "1.4B", "1.32B", "1.26B"]},
                    {"label": "Net Income", "values": ["6.32B", "6.32B", "6.08B", "5.6B", "5.28B", "5.04B"]},
                    {"label": "Diluted Average Shares", "values": ["2.31B", "2.31B", "2.32B", "2.33B", "2.34B", "2.35B"]},
                    {"label": "Diluted EPS", "values": ["2.74", "2.74", "2.62", "2.4", "2.26", "2.14"]},
                    {"label": "EBITDA", "values": ["9.4B", "9.4B", "9.1B", "8.5B", "8B", "7.7B"]},
                    {"label": "Tax Rate For Calcs", "values": ["0.20", "0.20", "0.20", "0.20", "0.20", "0.20"]},
                ],
            },
        }
        balance_statement = {
            "annual": {
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
            },
            "quarterly": {
                "periods": ["LATEST", "2026-03-31", "2025-12-31", "2025-09-30", "2025-06-30", "2025-03-31"],
                "rows": [
                    {"label": "Cash & Cash Equivalents", "values": ["26B", "26B", "25B", "23B", "21B", "20B"]},
                    {"label": "Other Short Term Investments", "values": ["21B", "21B", "20B", "19B", "17B", "16B"]},
                    {"label": "Cash, Equivalents & Short Term Investments", "values": ["47B", "47B", "45B", "42B", "38B", "36B"]},
                    {"label": "Accounts Receivable", "values": ["16B", "16B", "15B", "14.5B", "14B", "13.5B"]},
                    {"label": "Inventory", "values": ["10.5B", "10.5B", "10B", "9.5B", "9B", "8.5B"]},
                    {"label": "Accounts Payable", "values": ["8.5B", "8.5B", "8B", "7.8B", "7.5B", "7.2B"]},
                    {"label": "Current Debt", "values": ["5.2B", "5.2B", "5B", "4.8B", "4.5B", "4.2B"]},
                    {"label": "Long Term Debt", "values": ["19B", "19B", "20B", "21B", "22B", "23B"]},
                    {"label": "Total Debt", "values": ["24.2B", "24.2B", "25B", "25.8B", "26.5B", "27.2B"]},
                    {"label": "Gross PP&E", "values": ["82B", "82B", "80B", "78B", "76B", "74B"]},
                    {"label": "Net PP&E", "values": ["51B", "51B", "50B", "49B", "48B", "47B"]},
                    {"label": "Total Assets", "values": ["185B", "185B", "180B", "175B", "168B", "162B"]},
                    {"label": "Total Liabilities", "values": ["71B", "71B", "70B", "69B", "68B", "67B"]},
                    {"label": "Stockholders Equity", "values": ["114B", "114B", "110B", "106B", "100B", "95B"]},
                ],
            },
        }
        cash_flow_statement = {
            "annual": {
                "periods": ["TTM", "2025-12-31", "2024-12-31", "2023-12-31", "2022-12-31"],
                "rows": [
                    {"label": "Operating Cash Flow", "values": ["34B", "32B", "29B", "25B", "22B"]},
                    {"label": "Capital Expenditures", "values": ["-7B", "-6.5B", "-6B", "-5.5B", "-5B"]},
                    {"label": "Depreciation And Amortization", "values": ["5B", "5B", "4.8B", "4.5B", "4.2B"]},
                    {"label": "Free Cash Flow", "values": ["27B", "25.5B", "23B", "19.5B", "17B"]},
                    {"label": "Repurchase Of Capital Stock", "values": ["-8B", "-7B", "-6B", "-4B", "-3B"]},
                    {"label": "Cash Dividends Paid", "values": ["-3B", "-2.8B", "-2.5B", "-2.2B", "-2B"]},
                ],
            },
            "quarterly": {
                "periods": ["LATEST", "2026-03-31", "2025-12-31", "2025-09-30", "2025-06-30", "2025-03-31"],
                "rows": [
                    {"label": "Operating Cash Flow", "values": ["9.2B", "9.2B", "8.8B", "8.3B", "7.8B", "7.5B"]},
                    {"label": "Capital Expenditures", "values": ["-1.9B", "-1.9B", "-1.8B", "-1.7B", "-1.6B", "-1.5B"]},
                    {"label": "Depreciation And Amortization", "values": ["1.3B", "1.3B", "1.28B", "1.25B", "1.22B", "1.2B"]},
                    {"label": "Free Cash Flow", "values": ["7.3B", "7.3B", "7B", "6.6B", "6.2B", "6B"]},
                    {"label": "Repurchase Of Capital Stock", "values": ["-2.2B", "-2.2B", "-2B", "-1.8B", "-1.6B", "-1.5B"]},
                    {"label": "Cash Dividends Paid", "values": ["-800M", "-800M", "-780M", "-750M", "-720M", "-700M"]},
                ],
            },
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
            or url.startswith("https://finance.yahoo.com/")
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
        cached = getattr(Handler, "_yahoo_crumb_cache", None)
        cached_at = getattr(Handler, "_yahoo_crumb_cache_at", 0) or 0
        if cached and time.time() - cached_at < 60 * 60:
            return cached
        response = self._counted_open(opener, "https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=3)
        crumb = response.read().decode("utf-8", errors="ignore").strip()
        if crumb:
            Handler._yahoo_crumb_cache = crumb
            Handler._yahoo_crumb_cache_at = time.time()
        return crumb

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
        empty_stmt = {"annual": {"periods": [], "rows": []}, "quarterly": {"periods": [], "rows": []}}
        values = {key: "--" for key in FETCH_RESULT_FIELDS}
        values.update({
            "valuation_basis": "unavailable",
            "valuation_prefix": "EV",
            "valuation_numerator_label": "Current Enterprise Value",
            "company_name": ticker,
            "financial_currency": "USD",
            "usd_fx_rate": 1.0,
            "income_statement": empty_stmt,
            "balance_statement": {**empty_stmt},
            "cash_flow_statement": {**empty_stmt},
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

    def _unwrap_annual(self, statement):
        """Get the annual sub-object from a nested statement, or the statement itself if flat."""
        s = statement or {}
        if "annual" in s:
            return s["annual"] or {}
        return s

    def _latest_row_raw(self, statement, labels):
        flat = self._unwrap_annual(statement)
        labels_lower = {label.lower() for label in labels}
        for row in flat.get("rows", []):
            if row.get("label", "").lower() in labels_lower:
                for value in row.get("values", []):
                    raw = self._parse_money_to_raw(value)
                    if raw:
                        return raw
        return 0.0

    def _statement_latest_value(self, statement, labels):
        flat = self._unwrap_annual(statement)
        labels_lower = {label.lower() for label in labels}
        for row in flat.get("rows", []):
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

        quarterly_period_dates = set()
        for item in selected_results or []:
            type_name = self._statement_type_name(item)
            prefix = "annual" if type_name.startswith("annual") else "quarterly" if type_name.startswith("quarterly") else ""
            if not prefix:
                continue
            base_key = type_name[len(prefix):]
            label = type_map.get(base_key)
            if not label:
                label = self._camel_to_label(base_key)
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
                for point in points:
                    if not point["date"].startswith("idx-"):
                        quarterly_period_dates.add(point["date"])

        sorted_periods = sorted(period_dates, reverse=True)
        periods = ["TTM"] + sorted_periods
        rows = []

        q_sorted_periods = sorted(quarterly_period_dates, reverse=True)
        q_periods = ["LATEST"] + q_sorted_periods
        q_rows_out = []

        ordered_labels = [label for label in type_map.values() if label in annual_rows or label in quarterly_rows]
        for lbl in annual_rows.keys():
            if lbl not in ordered_labels:
                ordered_labels.append(lbl)
        for lbl in quarterly_rows.keys():
            if lbl not in ordered_labels:
                ordered_labels.append(lbl)

        for label in ordered_labels:
            annual_points = annual_rows.get(label, [])
            annual_by_date = {p["date"]: p["raw"] for p in annual_points}
            quarter_points = quarterly_rows.get(label, [])
            quarter_by_date = {p["date"]: p["raw"] for p in quarter_points}

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

            q_latest_raw = quarter_points[0]["raw"] if quarter_points else None
            q_values = [formatter(q_latest_raw) if q_latest_raw is not None else "--"]
            for period in q_sorted_periods:
                raw = quarter_by_date.get(period)
                q_values.append(formatter(raw) if raw is not None else "--")
            q_rows_out.append({"label": label, "values": q_values})

        return {
            "annual": {"periods": periods if rows else [], "rows": rows},
            "quarterly": {"periods": q_periods if q_rows_out else [], "rows": q_rows_out}
        }

    def build_income_statement_from_timeseries_results(self, selected_results, _identity_formatter=None, formatter=None):
        formatter = formatter or self._format_money
        return self.build_statement_from_timeseries_results(selected_results, INCOME_STATEMENT_TYPES, formatter)

    def build_balance_sheet_from_timeseries_results(self, selected_results, formatter=None):
        return self.build_statement_from_timeseries_results(selected_results, BALANCE_STATEMENT_TYPES, formatter or self._format_money)

    def build_cash_flow_statement_from_timeseries_results(self, selected_results, formatter=None):
        return self.build_statement_from_timeseries_results(selected_results, CASH_FLOW_STATEMENT_TYPES, formatter or self._format_money)

    def _extract_timeseries_results_from_page(self, ticker, page_opener):
        cache_key = ((ticker or "").upper(), id(page_opener))
        page_cache = getattr(self, "_yahoo_statement_page_cache", None)
        if page_cache is None:
            page_cache = {}
            self._yahoo_statement_page_cache = page_cache
        if cache_key in page_cache:
            return page_cache[cache_key]
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
        page_cache[cache_key] = results
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

    def _stockanalysis_estimate_points(self, html, metric_key, percent=False):
        charts_match = re.search(r"estimatesCharts:\{(.*?)\},recommendations:", html, re.DOTALL)
        if not charts_match:
            return []
        charts = charts_match.group(1)
        if metric_key == "revenueGrowth":
            section_match = re.search(r"revenueGrowth:\{(.*?)\}\s*$", charts, re.DOTALL)
        else:
            section_match = re.search(rf"{re.escape(metric_key)}:\{{(.*?)\}},[A-Za-z]+:", charts, re.DOTALL)
        if not section_match:
            return []

        points = []
        for date, raw_value in re.findall(
            r'"(\d{4}-\d{2}-\d{2})":\{[^{}]*?avg:([-+]?\d+(?:\.\d+)?)',
            section_match.group(1),
        ):
            try:
                value = float(raw_value)
            except Exception:
                continue
            points.append((date, value / 100 if percent else value))
        return sorted(points, key=lambda point: point[0])

    def _extract_yahoo_analysis_trends_from_html(self, html):
        json_trends = []
        for match in re.finditer(r'<script[^>]*data-sveltekit-fetched[^>]*>(.*?)</script>', html, re.DOTALL):
            try:
                body = json.loads(match.group(1)).get("body", "{}")
                if isinstance(body, str):
                    body = json.loads(body)
                trends = (
                    body.get("quoteSummary", {})
                    .get("result", [{}])[0]
                    .get("earningsTrend", {})
                    .get("trend", [])
                )
                if trends and not json_trends:
                    json_trends = trends
            except Exception:
                continue

        table_trends = self._extract_yahoo_sales_growth_from_analysis_text(html)
        if table_trends:
            if not json_trends:
                return table_trends

            by_period = {}
            order = []
            for trend in json_trends:
                period = trend.get("period")
                if not period:
                    continue
                by_period[period] = dict(trend)
                order.append(period)

            for table_trend in table_trends:
                period = table_trend.get("period")
                if not period:
                    continue
                merged = dict(by_period.get(period, {"period": period}))
                revenue_estimate = dict(merged.get("revenueEstimate", {}) or {})
                table_revenue_estimate = table_trend.get("revenueEstimate", {}) or {}
                if "growth" in table_revenue_estimate:
                    revenue_estimate["growth"] = table_revenue_estimate["growth"]
                merged["revenueEstimate"] = revenue_estimate
                if period not in by_period:
                    order.append(period)
                by_period[period] = merged

            return [by_period[period] for period in order if period in by_period]
        if json_trends:
            return json_trends
        return []

    def _extract_yahoo_sales_growth_from_analysis_text(self, html):
        text_sources = [html]
        for match in re.finditer(r'<script[^>]*data-sveltekit-fetched[^>]*>(.*?)</script>', html, re.DOTALL):
            try:
                body = json.loads(match.group(1)).get("body", "")
                if isinstance(body, str):
                    text_sources.append(body)
            except Exception:
                continue

        for source in text_sources:
            text = html_lib.unescape(str(source))
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\\u0025", "%", text)
            text = re.sub(r"\\u002F", "/", text)
            text = re.sub(r"\\+", " ", text)
            text = re.sub(r"\s+", " ", text).strip()

            label_match = re.search(r"Sales\s+Growth\s*\(\s*year\s*/\s*est\s*\)", text, re.IGNORECASE)
            if not label_match:
                continue

            segment = text[label_match.end():label_match.end() + 320]
            percent_values = re.findall(r"[+\-\u2212]?\d+(?:\.\d+)?\s*%", segment)
            if len(percent_values) >= 4:
                cy_growth, ny_growth = percent_values[2], percent_values[3]
            elif len(percent_values) >= 2:
                cy_growth, ny_growth = percent_values[-2], percent_values[-1]
            else:
                continue

            def to_raw(value):
                value = value.replace("\u2212", "-").replace("%", "").strip()
                return float(value) / 100

            try:
                return [
                    {"period": "0y", "revenueEstimate": {"growth": {"raw": to_raw(cy_growth)}}},
                    {"period": "+1y", "revenueEstimate": {"growth": {"raw": to_raw(ny_growth)}}},
                ]
            except Exception:
                continue
        return []

    def _fetch_yahoo_analysis_trends(self, ticker):
        for page in ("analysis", "analyst-insights"):
            try:
                url = f"https://finance.yahoo.com/quote/{ticker}/{page}/"
                html = self._counted_open(None, url, timeout=8).read().decode("utf-8", errors="ignore")
                trends = self._extract_yahoo_analysis_trends_from_html(html)
                if trends:
                    return trends
            except Exception as e:
                print(f"Yahoo analysis page warning ({page}):", e)
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
            return {"annual": {"periods": [], "rows": []}, "quarterly": {"periods": [], "rows": []}}
        html = self._counted_open(None, url, timeout=8).read().decode("utf-8", errors="ignore")
        data_match = re.search(r"financialData:\{(.*?)\},map:\[", html, re.DOTALL)
        map_match = re.search(r"\},map:\[(.*?)\],full_count", html, re.DOTALL)
        if not data_match or not map_match:
            return {"annual": {"periods": [], "rows": []}, "quarterly": {"periods": [], "rows": []}}

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

        return {
            "annual": {"periods": periods if rows else [], "rows": rows},
            "quarterly": {"periods": [], "rows": []}
        }

    def _merge_statement_rows(self, primary, secondary):
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
                        p_val = source_periods[idx]
                        if value and value != "--":
                            target[p_val] = value

            sorted_rows = []
            for label in labels:
                target = rows_by_label[label]
                sorted_rows.append({"label": label, "values": [target[period] for period in periods]})
            return {"periods": periods, "rows": sorted_rows}

        if "annual" in (primary or {}) or "quarterly" in (primary or {}) or "annual" in (secondary or {}):
            return {
                "annual": _merge((primary or {}).get("annual"), (secondary or {}).get("annual")),
                "quarterly": _merge((primary or {}).get("quarterly"), (secondary or {}).get("quarterly"))
            }
        else:
            return _merge(primary, secondary)

    def _ordered_df_index(self, df, order_map):
        """Return the DataFrame index sorted by order_map key order, with extras at the end."""
        if order_map is None:
            return list(df.index)
        ordered_keys = list(order_map.keys())
        index_list = list(df.index)
        # Build a lowercase-no-spaces lookup for matching
        normalized_index = {lbl.replace(" ", "").lower(): lbl for lbl in index_list}
        seen = set()
        result = []
        for key in ordered_keys:
            key_norm = key.lower()
            # Direct match
            if key in index_list and key not in seen:
                result.append(key)
                seen.add(key)
            elif key_norm in normalized_index:
                lbl = normalized_index[key_norm]
                if lbl not in seen:
                    result.append(lbl)
                    seen.add(lbl)
        # Append remaining items not in order_map
        for idx_label in index_list:
            if idx_label not in seen:
                result.append(idx_label)
        return result

    def _resolve_display_label(self, label, order_map):
        """Get the display label for a DataFrame index label, using order_map if available."""
        if order_map:
            # Direct match
            if label in order_map:
                return order_map[label]
            # Match by removing spaces (case-insensitive)
            label_norm = label.replace(" ", "").lower()
            for key, display in order_map.items():
                if key.lower() == label_norm:
                    return display
        # Already contains spaces = already human-readable from yfinance, use as-is
        if " " in str(label):
            return str(label)
        # CamelCase → spaced (only for true CamelCase labels)
        return re.sub(r"(?<!^)(?=[A-Z])", " ", str(label)).replace("And", "and")

    def _df_to_statement(self, df, formatter=None, ttm_label="TTM", order_map=None):
        """Convert a pandas DataFrame (rows=line items, columns=dates) to our statement format."""
        formatter = formatter or self._format_money
        if df is None or df.empty:
            return {"periods": [], "rows": []}
        import pandas as pd
        # Columns are dates, sort descending; drop mostly-empty columns
        cols = sorted(df.columns, reverse=True)
        min_fill = max(1, len(df) * 0.25)
        cols = [c for c in cols if df[c].notna().sum() >= min_fill]
        if not cols:
            return {"periods": [], "rows": []}
        periods = [ttm_label] + [c.strftime("%Y-%m-%d") if hasattr(c, "strftime") else str(c) for c in cols]
        rows = []
        ordered_index = self._ordered_df_index(df, order_map)
        for label in ordered_index:
            raw_values = df.loc[label, cols].tolist()
            # Skip rows where the most recent period has no data (discontinued items)
            if not raw_values or pd.isna(raw_values[0]):
                continue
            ttm_val = raw_values[0]
            formatted = [formatter(ttm_val) if pd.notna(ttm_val) else "--"]
            for v in raw_values:
                formatted.append(formatter(v) if pd.notna(v) else "--")
            display_label = self._resolve_display_label(label, order_map)
            rows.append({"label": display_label, "values": formatted})
        return {"periods": periods, "rows": rows}

    def _df_to_quarterly_statement(self, df, formatter=None, order_map=None):
        """Convert a quarterly DataFrame to our statement format with LATEST anchor."""
        formatter = formatter or self._format_money
        if df is None or df.empty:
            return {"periods": [], "rows": []}
        import pandas as pd
        # Columns are dates, sort descending; drop mostly-empty columns
        cols = sorted(df.columns, reverse=True)
        min_fill = max(1, len(df) * 0.25)
        cols = [c for c in cols if df[c].notna().sum() >= min_fill]
        if not cols:
            return {"periods": [], "rows": []}
        periods = ["LATEST"] + [c.strftime("%Y-%m-%d") if hasattr(c, "strftime") else str(c) for c in cols]
        rows = []
        ordered_index = self._ordered_df_index(df, order_map)
        for label in ordered_index:
            raw_values = df.loc[label, cols].tolist()
            # Skip rows where the most recent period has no data (discontinued items)
            if not raw_values or pd.isna(raw_values[0]):
                continue
            latest_val = raw_values[0]
            formatted = [formatter(latest_val) if pd.notna(latest_val) else "--"]
            for v in raw_values:
                formatted.append(formatter(v) if pd.notna(v) else "--")
            display_label = self._resolve_display_label(label, order_map)
            rows.append({"label": display_label, "values": formatted})
        return {"periods": periods, "rows": rows}

    def _df_raw_value(self, df, row_labels, col_index=0):
        """Get a raw numeric value from a DataFrame by row label and column index."""
        if df is None or df.empty:
            return 0.0
        import pandas as pd
        for label in row_labels:
            if label in df.index:
                cols = sorted(df.columns, reverse=True)
                if col_index < len(cols):
                    val = df.loc[label, cols[col_index]]
                    if pd.notna(val):
                        return float(val)
        return 0.0

    def _df_ttm_value(self, quarterly_df, annual_df, row_labels, absolute=False):
        """Calculate TTM from last 4 quarters, falling back to latest annual."""
        import pandas as pd
        if quarterly_df is not None and not quarterly_df.empty:
            cols = sorted(quarterly_df.columns, reverse=True)
            for label in row_labels:
                if label in quarterly_df.index:
                    vals = [quarterly_df.loc[label, c] for c in cols[:4]]
                    valid = [float(v) for v in vals if pd.notna(v)]
                    if len(valid) >= 4:
                        total = sum(valid)
                        return abs(total) if absolute else total
        # Fallback to latest annual
        val = self._df_raw_value(annual_df, row_labels, 0)
        return abs(val) if absolute else val

    def fetch_yfinance_data(self, ticker, finviz_ev_raw=0, finviz_market_cap_raw=0, finviz_metrics=None):
        """Fetch all data using yfinance package. Returns the same tuple as fetch_yahoo_finance_data."""
        import pandas as pd
        finviz_metrics = finviz_metrics or {}
        try:
            stock = yf.Ticker(ticker)
            info = stock.info or {}

            # Financial statements
            annual_income = stock.financials
            quarterly_income = stock.quarterly_financials
            annual_balance = stock.balance_sheet
            quarterly_balance = stock.quarterly_balance_sheet
            annual_cashflow = stock.cashflow
            quarterly_cashflow = stock.quarterly_cashflow

            income_statement = {
                "annual": self._df_to_statement(annual_income, order_map=INCOME_STATEMENT_TYPES),
                "quarterly": self._df_to_quarterly_statement(quarterly_income, order_map=INCOME_STATEMENT_TYPES),
            }
            balance_statement = {
                "annual": self._df_to_statement(annual_balance, ttm_label="MRQ", order_map=BALANCE_STATEMENT_TYPES),
                "quarterly": self._df_to_quarterly_statement(quarterly_balance, order_map=BALANCE_STATEMENT_TYPES),
            }
            cash_flow_statement = {
                "annual": self._df_to_statement(annual_cashflow, order_map=CASH_FLOW_STATEMENT_TYPES),
                "quarterly": self._df_to_quarterly_statement(quarterly_cashflow, order_map=CASH_FLOW_STATEMENT_TYPES),
            }

            # Core metrics from DataFrames (TTM using quarterly sums)
            revenue_raw = self._df_ttm_value(quarterly_income, annual_income, ["Total Revenue", "TotalRevenue"]) or info.get("totalRevenue", 0) or 0
            operating_income_raw = self._df_ttm_value(quarterly_income, annual_income, ["Operating Income", "OperatingIncome"]) or info.get("operatingIncome", 0) or 0
            gross_profit_raw = self._df_ttm_value(quarterly_income, annual_income, ["Gross Profit", "GrossProfit"]) or info.get("grossProfits", 0) or 0
            capex_raw = abs(self._df_ttm_value(quarterly_cashflow, annual_cashflow, ["Capital Expenditure", "CapitalExpenditure"], absolute=True))
            da_raw = self._df_ttm_value(quarterly_cashflow, annual_cashflow, ["Depreciation And Amortization", "DepreciationAndAmortization", "Reconciled Depreciation", "ReconciledDepreciation"])
            if not da_raw:
                da_raw = self._df_ttm_value(quarterly_income, annual_income, ["Reconciled Depreciation", "ReconciledDepreciation"])
            da_raw = abs(da_raw) if da_raw else 0

            gross_ppe_raw = self._df_raw_value(annual_balance, ["Gross PPE", "GrossPPE"]) or self._df_raw_value(annual_balance, ["Net PPE", "NetPPE"])
            net_fixed_assets_raw = self._df_raw_value(annual_balance, ["Net PPE", "NetPPE"])
            receivables_raw = self._df_raw_value(annual_balance, ["Accounts Receivable", "AccountsReceivable", "Net Receivables"])
            inventory_raw = self._df_raw_value(annual_balance, ["Inventory"])
            accounts_payable_raw = self._df_raw_value(annual_balance, ["Accounts Payable", "AccountsPayable"])

            da_minus_capex_raw = max(da_raw - capex_raw, 0)
            investment_capex_raw = max(capex_raw - da_raw, 0)
            adj_income_raw = operating_income_raw + da_minus_capex_raw
            adj_margin_ratio = (adj_income_raw / revenue_raw) if revenue_raw else 0
            operating_margin_ratio = (operating_income_raw / revenue_raw) if revenue_raw else info.get("operatingMargins", 0) or 0
            gross_margin_ratio = info.get("grossMargins", None)
            if gross_margin_ratio is None and revenue_raw and gross_profit_raw:
                gross_margin_ratio = gross_profit_raw / revenue_raw

            # R&D
            rnd_raw = self._df_ttm_value(quarterly_income, annual_income, ["Research And Development", "ResearchAndDevelopment", "Research Development"]) or 0

            # 3-year growth from annual income statement
            gp_3y_growth_raw, gp_3y_start_raw, gp_3y_end_raw, gp_3y_label = None, 0, 0, "3Y Annual GP Growth"
            if annual_income is not None and not annual_income.empty:
                cols = sorted(annual_income.columns, reverse=True)
                gp_label_candidates = ["Gross Profit", "GrossProfit"]
                rev_label_candidates = ["Total Revenue", "TotalRevenue"]
                for candidates, label_out in [(gp_label_candidates, "3Y Annual GP Growth"), (rev_label_candidates, "3Y Annual Sales Growth")]:
                    for lbl in candidates:
                        if lbl in annual_income.index:
                            vals = [(c, annual_income.loc[lbl, c]) for c in cols if pd.notna(annual_income.loc[lbl, c])]
                            if len(vals) >= 2:
                                end_val = float(vals[0][1])
                                start_idx = min(3, len(vals) - 1)
                                start_val = float(vals[start_idx][1])
                                end_date = vals[0][0]
                                start_date = vals[start_idx][0]
                                years = (end_date - start_date).days / 365.25 if hasattr(end_date, "days") or hasattr(start_date, "year") else start_idx
                                try:
                                    years = (end_date - start_date).days / 365.25
                                except Exception:
                                    years = start_idx
                                years = max(years, 1)
                                if start_val and abs(start_val) > 0:
                                    gp_3y_growth_raw = (end_val / abs(start_val)) ** (1 / years) - 1
                                    gp_3y_start_raw = start_val
                                    gp_3y_end_raw = end_val
                                    gp_3y_label = label_out
                                break
                    if gp_3y_growth_raw is not None:
                        break

            # Analyst estimates from info
            cy_eps_raw = info.get("forwardEps", 0) or 0
            year_ago_eps_raw = info.get("trailingEps", 0) or 0
            ny_eps_raw = 0
            cy_eps_growth_raw = None
            ny_eps_growth_raw = None
            if cy_eps_raw and year_ago_eps_raw and year_ago_eps_raw != 0:
                cy_eps_growth_raw = (cy_eps_raw / abs(year_ago_eps_raw)) - 1

            # Revenue estimates from info
            cy_revenue_raw = info.get("revenueEstimates", {}).get("avg", 0) if isinstance(info.get("revenueEstimates"), dict) else 0
            ny_revenue_raw = 0
            cy_growth_raw = info.get("revenueGrowth", None)
            ny_growth_raw = None

            # Try earnings_estimate for better estimates
            try:
                ee = stock.earnings_estimate
                if ee is not None and not ee.empty:
                    if "0y" in ee.index:
                        if "avg" in ee.columns and pd.notna(ee.loc["0y", "avg"]):
                            cy_eps_raw = float(ee.loc["0y", "avg"])
                        if "yearAgoEps" in ee.columns and pd.notna(ee.loc["0y", "yearAgoEps"]):
                            year_ago_eps_raw = float(ee.loc["0y", "yearAgoEps"])
                        if "growth" in ee.columns and pd.notna(ee.loc["0y", "growth"]):
                            cy_eps_growth_raw = float(ee.loc["0y", "growth"])
                    if "+1y" in ee.index:
                        if "avg" in ee.columns and pd.notna(ee.loc["+1y", "avg"]):
                            ny_eps_raw = float(ee.loc["+1y", "avg"])
                        if "growth" in ee.columns and pd.notna(ee.loc["+1y", "growth"]):
                            ny_eps_growth_raw = float(ee.loc["+1y", "growth"])
            except Exception:
                pass

            # Try revenue_estimate for better revenue forecasts
            try:
                re_est = stock.revenue_estimate
                if re_est is not None and not re_est.empty:
                    if "0y" in re_est.index:
                        if "avg" in re_est.columns and pd.notna(re_est.loc["0y", "avg"]):
                            cy_revenue_raw = float(re_est.loc["0y", "avg"])
                        if "growth" in re_est.columns and pd.notna(re_est.loc["0y", "growth"]):
                            cy_growth_raw = float(re_est.loc["0y", "growth"])
                    if "+1y" in re_est.index:
                        if "avg" in re_est.columns and pd.notna(re_est.loc["+1y", "avg"]):
                            ny_revenue_raw = float(re_est.loc["+1y", "avg"])
                        if "growth" in re_est.columns and pd.notna(re_est.loc["+1y", "growth"]):
                            ny_growth_raw = float(re_est.loc["+1y", "growth"])
            except Exception:
                pass

            # Finviz EPS fallbacks
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

            # Currency
            financial_currency = (info.get("financialCurrency") or info.get("currency") or "USD").upper()
            usd_fx_rate = self.get_usd_fx_rate(financial_currency)
            quote_currency = (info.get("currency") or "USD").upper()
            eps_fx = usd_fx_rate if quote_currency == "USD" and financial_currency != "USD" else 1.0
            cy_eps_raw = (cy_eps_raw or 0) * eps_fx
            ny_eps_raw = (ny_eps_raw or 0) * eps_fx
            year_ago_eps_raw = (year_ago_eps_raw or 0) * eps_fx

            # Market cap and valuation
            market_cap_raw = float(finviz_market_cap_raw or 0) or info.get("marketCap", 0) or 0
            cash_bucket_raw = self._latest_row_raw(balance_statement, ["Cash, Equivalents & Short Term Investments", "Cash & Short Term Investments", "Cash Cash Equivalents and Short Term Investments"])
            if not cash_bucket_raw:
                cash_bucket_raw = self._latest_row_raw(balance_statement, ["Cash & Cash Equivalents", "Cash and Cash Equivalents"]) + self._latest_row_raw(balance_statement, ["Other Short Term Investments", "Short Term Investments"])
            if not cash_bucket_raw:
                cash_bucket_raw = self._df_raw_value(annual_balance, ["Cash Cash Equivalents And Short Term Investments", "CashCashEquivalentsAndShortTermInvestments", "Cash And Cash Equivalents", "CashAndCashEquivalents"])
            total_debt_raw = self._df_raw_value(annual_balance, ["Total Debt", "TotalDebt"])
            if not total_debt_raw:
                total_debt_raw = self._df_raw_value(annual_balance, ["Current Debt", "CurrentDebt"]) + self._df_raw_value(annual_balance, ["Long Term Debt", "LongTermDebt"])
            net_cash_raw = cash_bucket_raw - total_debt_raw if cash_bucket_raw or total_debt_raw else (market_cap_raw - float(finviz_ev_raw or 0) if finviz_ev_raw and market_cap_raw else 0)
            derived_enterprise_value_raw = market_cap_raw - net_cash_raw if market_cap_raw else 0

            valuation_raw = float(finviz_ev_raw or 0) or info.get("enterpriseValue", 0) or 0
            valuation_basis = "enterpriseValue" if valuation_raw else "marketCap"
            valuation_prefix = "EV" if valuation_raw else "Mkt Cap"
            valuation_numerator_label = "Current Enterprise Value" if valuation_raw else "Current Market Cap"
            if not valuation_raw:
                valuation_raw = market_cap_raw

            cy_adj_inc_raw = cy_revenue_raw * adj_margin_ratio if cy_revenue_raw and adj_margin_ratio else 0
            ny_adj_inc_raw = ny_revenue_raw * adj_margin_ratio if ny_revenue_raw and adj_margin_ratio else 0
            nwc_raw = receivables_raw + inventory_raw - accounts_payable_raw
            roc_denominator_raw = nwc_raw + net_fixed_assets_raw

            current_price_raw = info.get("currentPrice", 0) or info.get("regularMarketPrice", 0) or 0
            target_mean_raw = info.get("targetMeanPrice", 0) or 0
            target_low_raw = info.get("targetLowPrice", 0) or 0
            target_high_raw = info.get("targetHighPrice", 0) or 0
            target_move_raw = ((target_mean_raw - current_price_raw) / current_price_raw) if target_mean_raw and current_price_raw else None

            recommendation_mean = info.get("recommendationMean", 0) or 0
            recommendation_key = info.get("recommendationKey", "--") or "--"

            # Analyst recommendations breakdown
            analyst_recommendations = {}
            try:
                recs = stock.recommendations
                if recs is not None and not recs.empty:
                    latest = recs.iloc[-1] if len(recs) > 0 else {}
                    analyst_recommendations = {
                        "strongBuy": int(latest.get("strongBuy", 0) or 0),
                        "buy": int(latest.get("buy", 0) or 0),
                        "hold": int(latest.get("hold", 0) or 0),
                        "sell": int(latest.get("sell", 0) or 0),
                        "strongSell": int(latest.get("strongSell", 0) or 0),
                    }
            except Exception:
                pass

            company_name = info.get("longName") or info.get("shortName") or ticker

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
                "cy_adj_inc": self._format_money(cy_adj_inc_raw) if cy_adj_inc_raw else "--",
                "ny_adj_inc": self._format_money(ny_adj_inc_raw) if ny_adj_inc_raw else "--",
                "market_cap": self._format_money(market_cap_raw),
                "net_cash": self._format_money(net_cash_raw),
                "derived_enterprise_value": self._format_money(derived_enterprise_value_raw),
                "revenue": self._format_money(revenue_raw),
                "operating_margin": self._format_percent(operating_margin_ratio) if operating_margin_ratio else "--",
                "da_minus_capex": self._format_money(da_minus_capex_raw) if da_minus_capex_raw else "0",
                "cy_revenue": self._format_money(cy_revenue_raw) if cy_revenue_raw else "--",
                "ny_revenue": self._format_money(ny_revenue_raw) if ny_revenue_raw else "--",
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
                "recommendation_mean": self._format_3sig(recommendation_mean),
                "recommendation_key": recommendation_key,
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
            print(f"yfinance fetch error for {ticker}: {e}")
            raise

    def fetch_yahoo_finance_data(self, ticker, finviz_ev_raw=0, finviz_market_cap_raw=0, finviz_metrics=None):
        # Try yfinance first if available
        if HAS_YFINANCE:
            try:
                result = self.fetch_yfinance_data(ticker, finviz_ev_raw, finviz_market_cap_raw, finviz_metrics)
                print(f"[yfinance] Successfully fetched data for {ticker}")
                return result
            except Exception as e:
                print(f"[yfinance] Failed for {ticker}, falling back to manual: {e}")

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
            self._yahoo_statement_page_cache = {}

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
                chunk_size = 70
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

            if not self._unwrap_annual(income_statement).get("rows"):
                try:
                    income_statement = self.build_statement_from_stockanalysis_page(ticker, "income")
                except Exception as e:
                    print("StockAnalysis income warning:", e)
            if not self._unwrap_annual(balance_statement).get("rows"):
                try:
                    balance_statement = self.build_statement_from_stockanalysis_page(ticker, "balance")
                except Exception as e:
                    print("StockAnalysis balance warning:", e)
            if not self._unwrap_annual(cash_flow_statement).get("rows"):
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

            analysis_trends_cache = None
            def yahoo_analysis_trends_once():
                nonlocal analysis_trends_cache
                if analysis_trends_cache is None:
                    analysis_trends_cache = self._fetch_yahoo_analysis_trends(ticker)
                return analysis_trends_cache
            
            if not et:
                et = yahoo_analysis_trends_once()
                if not et:
                    print("Fallback analysis page: no earningsTrend found in SvelteKit JSON.")

            dks = res.get("defaultKeyStatistics", {}) or {}
            price = res.get("price", {}) or {}

            chart_meta = {}
            quote_price_raw = self._raw(price.get("regularMarketPrice")) or self._raw(fd.get("currentPrice"))
            if not quote_price_raw or not price.get("currency"):
                try:
                    chart_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&interval=1d"
                    chart = json.loads(self._counted_open(data_opener, chart_url, timeout=3).read().decode("utf-8"))
                    chart_results = (chart.get("chart", {}) or {}).get("result", []) or []
                    chart_meta = chart_results[0].get("meta", {}) if chart_results else {}
                except Exception:
                    chart_meta = {}

            if not res and not ts_res and not chart_meta:
                if not self._unwrap_annual(income_statement).get("rows") and not self._unwrap_annual(balance_statement).get("rows") and not self._unwrap_annual(cash_flow_statement).get("rows"):
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
                flat = self._unwrap_annual(statement)
                labels_lower = {label.lower() for label in labels}
                periods = flat.get("periods", []) or []
                for row in flat.get("rows", []) or []:
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

            gp_3y_growth_raw, gp_3y_start_raw, gp_3y_end_raw, gp_3y_label = three_year_growth(income_statement)
            rnd_raw = self._latest_row_raw(income_statement, ["Research & Development", "Research and Development"])

            cy_revenue_raw = ny_revenue_raw = 0
            cy_growth_raw = ny_growth_raw = None
            cy_revenue_from_yahoo = ny_revenue_from_yahoo = False
            cy_eps_raw = ny_eps_raw = year_ago_eps_raw = 0
            cy_eps_growth_raw = ny_eps_growth_raw = None

            def yahoo_revenue_growth(revenue_est):
                # Use Yahoo's reported Sales Growth field only. If it is absent,
                # leave the metric empty rather than estimating or backfilling it.
                return self._raw(revenue_est.get("growth"), None)

            def apply_estimate_trends(trends, overwrite=False, source="yahoo"):
                nonlocal cy_revenue_raw, ny_revenue_raw, cy_growth_raw, ny_growth_raw
                nonlocal cy_revenue_from_yahoo, ny_revenue_from_yahoo
                nonlocal cy_eps_raw, ny_eps_raw, year_ago_eps_raw, cy_eps_growth_raw, ny_eps_growth_raw
                for trend in trends or []:
                    revenue_est = trend.get("revenueEstimate", {}) or {}
                    earnings_est = trend.get("earningsEstimate", {}) or {}
                    period = trend.get("period")
                    if period == "0y":
                        revenue_avg = self._raw(revenue_est.get("avg"))
                        revenue_growth = yahoo_revenue_growth(revenue_est)
                        eps_avg = self._eps_value(earnings_est.get("avg"))
                        year_ago_eps = self._eps_value(earnings_est.get("yearAgoEps"))
                        eps_growth = self._raw(earnings_est.get("growth"), None)
                        if revenue_avg and (overwrite or not cy_revenue_raw):
                            cy_revenue_raw = revenue_avg
                            cy_revenue_from_yahoo = source == "yahoo"
                        if revenue_growth is not None and (overwrite or cy_growth_raw is None):
                            cy_growth_raw = revenue_growth
                        if eps_avg and (overwrite or not cy_eps_raw):
                            cy_eps_raw = eps_avg
                        if year_ago_eps and (overwrite or not year_ago_eps_raw):
                            year_ago_eps_raw = year_ago_eps
                        if eps_growth is not None and (overwrite or cy_eps_growth_raw is None):
                            cy_eps_growth_raw = eps_growth
                    elif period == "+1y":
                        revenue_avg = self._raw(revenue_est.get("avg"))
                        revenue_growth = yahoo_revenue_growth(revenue_est)
                        eps_avg = self._eps_value(earnings_est.get("avg"))
                        eps_growth = self._raw(earnings_est.get("growth"), None)
                        if revenue_avg and (overwrite or not ny_revenue_raw):
                            ny_revenue_raw = revenue_avg
                            ny_revenue_from_yahoo = source == "yahoo"
                        if revenue_growth is not None and (overwrite or ny_growth_raw is None):
                            ny_growth_raw = revenue_growth
                        if eps_avg and (overwrite or not ny_eps_raw):
                            ny_eps_raw = eps_avg
                        if eps_growth is not None and (overwrite or ny_eps_growth_raw is None):
                            ny_eps_growth_raw = eps_growth

            apply_estimate_trends(et)

            if cy_growth_raw is None or ny_growth_raw is None or not cy_revenue_raw or not ny_revenue_raw:
                yahoo_analysis_trends = yahoo_analysis_trends_once()
                if yahoo_analysis_trends:
                    apply_estimate_trends(yahoo_analysis_trends, overwrite=True)

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
                        
            if not cy_revenue_raw or not ny_revenue_raw:
                try:
                    forecast_url = f"https://stockanalysis.com/stocks/{ticker.lower()}/forecast/"
                    html = self._counted_open(None, forecast_url, timeout=8).read().decode("utf-8", errors="ignore")
                    forecast_revenues = self._stockanalysis_estimate_points(html, "revenue")
                    if len(forecast_revenues) > 0 and not cy_revenue_raw:
                        cy_revenue_raw = forecast_revenues[0][1]
                    if len(forecast_revenues) > 1 and not ny_revenue_raw:
                        ny_revenue_raw = forecast_revenues[1][1]
                except Exception as e:
                    print("Fallback StockAnalysis forecast error:", e)

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
                "cy_adj_inc": self._format_money(cy_adj_inc_raw) if cy_adj_inc_raw else "--",
                "ny_adj_inc": self._format_money(ny_adj_inc_raw) if ny_adj_inc_raw else "--",
                "market_cap": self._format_money(market_cap_raw),
                "net_cash": self._format_money(net_cash_raw),
                "derived_enterprise_value": self._format_money(derived_enterprise_value_raw),
                "revenue": self._format_money(revenue_raw),
                "operating_margin": self._format_percent(operating_margin_ratio) if operating_margin_ratio else "--",
                "da_minus_capex": self._format_money(da_minus_capex_raw) if da_minus_capex_raw else "0",
                "cy_revenue": self._format_money(cy_revenue_raw) if cy_revenue_raw else "--",
                "ny_revenue": self._format_money(ny_revenue_raw) if ny_revenue_raw else "--",
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
        previous_cache_entry = cache.get(ticker)
        previous_payload = (
            previous_cache_entry.get("data", {})
            if isinstance(previous_cache_entry, dict)
            else {}
        )

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

        def enrich_cached_payload(cached_payload, cached_entry, fetch_count=0, refresh_error=False):
            cached_entry = cached_entry if isinstance(cached_entry, dict) else {}
            payload = dict(cached_payload)
            if "dataDate" not in payload:
                payload["dataDate"] = cached_entry.get("date", today)
            if not payload.get("pulledAt"):
                payload["pulledAt"] = cached_entry.get("pulledAt")
            payload["fetchCount"] = fetch_count
            if refresh_error:
                payload["staleDueToRefreshError"] = True
                payload["refreshError"] = "Data refresh failed; showing cached data."
            return payload

        if not refresh and ticker in cache and cache[ticker].get('date') == today:
            cached_payload = cache[ticker].get('data', {})
            if cache_is_usable(cached_payload):
                self._send_response(200, enrich_cached_payload(cached_payload, cache[ticker], fetch_count=0))
                return

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
            if cache_is_usable(previous_payload):
                self._send_response(
                    200,
                    enrich_cached_payload(
                        previous_payload,
                        previous_cache_entry,
                        fetch_count=getattr(self, "_request_fetch_count", 0),
                        refresh_error=refresh,
                    ),
                )
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
