import http.server
import socketserver
import json
import os
import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, parse_qs

import cache_store
import statements
from formatters import format_3sig, format_money, format_percent, parse_abbrev_to_raw, parse_money_to_raw

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

PORT = int(os.environ.get("PORT", "3000"))
CACHE_DB_FILE = os.environ.get("CACHE_DB_FILE", "cache.db")
LEGACY_CACHE_FILE = "cache.json"
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "900"))
PAYLOAD_VERSION = 13
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
    "short_float", "structured_metrics",
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
    return cache_store.init_cache_db(conn)


def load_legacy_cache():
    return cache_store.load_legacy_cache(LEGACY_CACHE_FILE)


def migrate_legacy_cache_if_needed(conn):
    return cache_store.migrate_legacy_cache_if_needed(conn, LEGACY_CACHE_FILE)


def write_cache_rows(conn, cache_data):
    return cache_store.write_cache_rows(conn, cache_data)


def load_cache():
    return cache_store.load_cache(CACHE_DB_FILE, LEGACY_CACHE_FILE)


def save_cache(cache_data):
    return cache_store.save_cache(CACHE_DB_FILE, cache_data)

class Handler(http.server.SimpleHTTPRequestHandler):
    _yahoo_crumb_cache = None
    _yahoo_crumb_cache_at = 0

    def _metric_value(self, raw, display=None, kind="number", currency=None):
        payload = {
            "raw": raw if raw is not None else None,
            "display": display if display is not None else "--",
            "kind": kind,
        }
        if currency:
            payload["currency"] = currency
        return payload

    def _structured_metrics(self, specs, currency=None):
        metrics = {}
        for key, raw, display, kind in specs:
            metrics[key] = self._metric_value(raw, display, kind, currency if kind == "money" else None)
        return metrics

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
                "periods": ["2026-03-31", "2025-12-31", "2025-09-30", "2025-06-30", "2025-03-31"],
                "rows": [
                    {"label": "Total Revenue", "values": ["27B", "26B", "24.5B", "23B", "22.5B"]},
                    {"label": "Cost of Revenue", "values": ["10.8B", "10.4B", "9.8B", "9.2B", "9B"]},
                    {"label": "Gross Profit", "values": ["16.2B", "15.6B", "14.7B", "13.8B", "13.5B"]},
                    {"label": "Research & Development", "values": ["3.2B", "3.1B", "2.9B", "2.8B", "2.7B"]},
                    {"label": "Selling, General & Administrative", "values": ["3.7B", "3.5B", "3.4B", "3.3B", "3.2B"]},
                    {"label": "Operating Income", "values": ["8.1B", "7.8B", "7.2B", "6.8B", "6.5B"]},
                    {"label": "Interest Expense", "values": ["310M", "300M", "290M", "280M", "270M"]},
                    {"label": "Pretax Income", "values": ["7.9B", "7.6B", "7B", "6.6B", "6.3B"]},
                    {"label": "Tax Provision", "values": ["1.58B", "1.52B", "1.4B", "1.32B", "1.26B"]},
                    {"label": "Net Income", "values": ["6.32B", "6.08B", "5.6B", "5.28B", "5.04B"]},
                    {"label": "Diluted Average Shares", "values": ["2.31B", "2.32B", "2.33B", "2.34B", "2.35B"]},
                    {"label": "Diluted EPS", "values": ["2.74", "2.62", "2.4", "2.26", "2.14"]},
                    {"label": "EBITDA", "values": ["9.4B", "9.1B", "8.5B", "8B", "7.7B"]},
                    {"label": "Tax Rate For Calcs", "values": ["0.20", "0.20", "0.20", "0.20", "0.20"]},
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
                "periods": ["2026-03-31", "2025-12-31", "2025-09-30", "2025-06-30", "2025-03-31"],
                "rows": [
                    {"label": "Cash & Cash Equivalents", "values": ["26B", "25B", "23B", "21B", "20B"]},
                    {"label": "Other Short Term Investments", "values": ["21B", "20B", "19B", "17B", "16B"]},
                    {"label": "Cash, Equivalents & Short Term Investments", "values": ["47B", "45B", "42B", "38B", "36B"]},
                    {"label": "Accounts Receivable", "values": ["16B", "15B", "14.5B", "14B", "13.5B"]},
                    {"label": "Inventory", "values": ["10.5B", "10B", "9.5B", "9B", "8.5B"]},
                    {"label": "Accounts Payable", "values": ["8.5B", "8B", "7.8B", "7.5B", "7.2B"]},
                    {"label": "Current Debt", "values": ["5.2B", "5B", "4.8B", "4.5B", "4.2B"]},
                    {"label": "Long Term Debt", "values": ["19B", "20B", "21B", "22B", "23B"]},
                    {"label": "Total Debt", "values": ["24.2B", "25B", "25.8B", "26.5B", "27.2B"]},
                    {"label": "Gross PP&E", "values": ["82B", "80B", "78B", "76B", "74B"]},
                    {"label": "Net PP&E", "values": ["51B", "50B", "49B", "48B", "47B"]},
                    {"label": "Total Assets", "values": ["185B", "180B", "175B", "168B", "162B"]},
                    {"label": "Total Liabilities", "values": ["71B", "70B", "69B", "68B", "67B"]},
                    {"label": "Stockholders Equity", "values": ["114B", "110B", "106B", "100B", "95B"]},
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
                "periods": ["2026-03-31", "2025-12-31", "2025-09-30", "2025-06-30", "2025-03-31"],
                "rows": [
                    {"label": "Operating Cash Flow", "values": ["9.2B", "8.8B", "8.3B", "7.8B", "7.5B"]},
                    {"label": "Capital Expenditures", "values": ["-1.9B", "-1.8B", "-1.7B", "-1.6B", "-1.5B"]},
                    {"label": "Depreciation And Amortization", "values": ["1.3B", "1.28B", "1.25B", "1.22B", "1.2B"]},
                    {"label": "Free Cash Flow", "values": ["7.3B", "7B", "6.6B", "6.2B", "6B"]},
                    {"label": "Repurchase Of Capital Stock", "values": ["-2.2B", "-2B", "-1.8B", "-1.6B", "-1.5B"]},
                    {"label": "Cash Dividends Paid", "values": ["-800M", "-780M", "-750M", "-720M", "-700M"]},
                ],
            },
        }
        income_statement = self._add_adjusted_operating_income(income_statement, cash_flow_statement)
        income_statement = self._add_tax_rate(income_statement)
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
        metrics = self._structured_metrics([
            ("income", operating_income_raw, self._format_money(operating_income_raw), "money"),
            ("margin", adj_margin_ratio, self._format_percent(adj_margin_ratio), "percent"),
            ("grossMargin", gross_margin_ratio, self._format_percent(gross_margin_ratio), "percent"),
            ("ev_cy_ebit", derived_ev_raw / cy_adj_inc_raw, self._format_3sig(derived_ev_raw / cy_adj_inc_raw), "ratio"),
            ("ev_ny_ebit", derived_ev_raw / ny_adj_inc_raw, self._format_3sig(derived_ev_raw / ny_adj_inc_raw), "ratio"),
            ("adj_income", adj_income_raw, self._format_money(adj_income_raw), "money"),
            ("capex", capex_raw, self._format_money(capex_raw), "money"),
            ("da", da_raw, self._format_money(da_raw), "money"),
            ("ev", derived_ev_raw, self._format_money(derived_ev_raw), "money"),
            ("ev_adj_ebit", derived_ev_raw / adj_income_raw, self._format_3sig(derived_ev_raw / adj_income_raw), "ratio"),
            ("cy_growth", cy_growth_raw, self._format_percent(cy_growth_raw), "percent"),
            ("ny_growth", ny_growth_raw, self._format_percent(ny_growth_raw), "percent"),
            ("gp_3y_growth", gp_3y_growth_raw, self._format_percent(gp_3y_growth_raw), "percent"),
            ("gp_3y_start", gp_3y_start_raw, self._format_money(gp_3y_start_raw), "money"),
            ("gp_3y_end", gp_3y_end_raw, self._format_money(gp_3y_end_raw), "money"),
            ("rndAdjIncome", rnd_raw / adj_income_raw, self._format_percent(rnd_raw / adj_income_raw), "percent"),
            ("cy_adj_inc", cy_adj_inc_raw, self._format_money(cy_adj_inc_raw), "money"),
            ("ny_adj_inc", ny_adj_inc_raw, self._format_money(ny_adj_inc_raw), "money"),
            ("marketCap", market_cap_raw, self._format_money(market_cap_raw), "money"),
            ("netCash", net_cash_raw, self._format_money(net_cash_raw), "money"),
            ("derivedEnterpriseValue", derived_ev_raw, self._format_money(derived_ev_raw), "money"),
            ("revenue", revenue_raw, self._format_money(revenue_raw), "money"),
            ("operating_margin", operating_income_raw / revenue_raw, self._format_percent(operating_income_raw / revenue_raw), "percent"),
            ("da_minus_capex", da_minus_capex_raw, self._format_money(da_minus_capex_raw), "money"),
            ("cy_revenue", cy_revenue_raw, self._format_money(cy_revenue_raw), "money"),
            ("ny_revenue", ny_revenue_raw, self._format_money(ny_revenue_raw), "money"),
            ("grossPpe", gross_ppe_raw, self._format_money(gross_ppe_raw), "money"),
            ("adjEbitGrossPpe", adj_income_raw / gross_ppe_raw, self._format_percent(adj_income_raw / gross_ppe_raw), "percent"),
            ("capexAdjIncome", investment_capex_raw / adj_income_raw, self._format_percent(investment_capex_raw / adj_income_raw), "percent"),
            ("investmentCapex", investment_capex_raw, self._format_money(investment_capex_raw), "money"),
            ("roc", adj_income_raw / roc_denominator_raw, self._format_percent(adj_income_raw / roc_denominator_raw), "percent"),
            ("netWorkingCapital", net_working_capital_raw, self._format_money(net_working_capital_raw), "money"),
            ("netFixedAssets", net_fixed_assets_raw, self._format_money(net_fixed_assets_raw), "money"),
            ("receivables", receivables_raw, self._format_money(receivables_raw), "money"),
            ("inventory", inventory_raw, self._format_money(inventory_raw), "money"),
            ("accountsPayable", accounts_payable_raw, self._format_money(accounts_payable_raw), "money"),
            ("shortFloat", 0.042, "4.2%", "percent"),
            ("currentPrice", 100, "100", "money"),
            ("targetMeanPrice", 125, "125", "money"),
            ("targetLowPrice", 90, "90", "money"),
            ("targetHighPrice", 160, "160", "money"),
            ("targetMove", 0.25, "25%", "percent"),
            ("currentYearEps", 10, "10", "number"),
            ("nextYearEps", 12, "12", "number"),
            ("yearAgoEps", 8, "8", "number"),
            ("currentYearEpsGrowth", 0.25, "25%", "percent"),
            ("nextYearEpsGrowth", 0.20, "20%", "percent"),
            ("priceCurrentEps", 12.5, "12.5", "ratio"),
            ("priceCyEps", 10, "10", "ratio"),
            ("priceNyEps", 8.33, "8.33", "ratio"),
        ], currency="USD")

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
            "metrics": metrics,
            "evSource": "test-fixture",
            "marketCapSource": "test-fixture",
            "dataDate": today,
            "pulledAt": pulled_at,
            "fetchCount": 0,
            "fetchTiming": {
                "source": "test",
                "totalSeconds": 0,
                "stages": [],
            },
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

    def _infer_currency_from_ticker(self, ticker, current_currency):
        """Fallback to infer currency from ticker suffix if API returns USD or missing."""
        if not ticker or not isinstance(ticker, str):
            return current_currency

        ticker_upper = ticker.upper()
        if current_currency in (None, "", "USD"):
            if ticker_upper.endswith(".HK"):
                return "HKD"
            if ticker_upper.endswith(".SS") or ticker_upper.endswith(".SZ"):
                return "CNY"
            if ticker_upper.endswith(".L"):
                return "GBP"
            if ticker_upper.endswith(".TO") or ticker_upper.endswith(".V"):
                return "CAD"
            if ticker_upper.endswith(".DE"):
                return "EUR"
            if ticker_upper.endswith(".AS") or ticker_upper.endswith(".PA") or ticker_upper.endswith(".BR"):
                return "EUR"
        return current_currency or "USD"

    def _format_3sig(self, val):
        return format_3sig(val)

    def _format_percent(self, val):
        return format_percent(val)

    def _format_money(self, val):
        return format_money(val)

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
        return parse_money_to_raw(value)

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
            "structured_metrics": {},
        })
        return tuple(values[key] for key in FETCH_RESULT_FIELDS)

    def _market_cap_from_info(self, info, quote_fx_rate=1.0):
        api_market_cap = info.get("marketCap", 0) or 0
        if api_market_cap:
            return api_market_cap * quote_fx_rate

        raw_price = info.get("currentPrice", 0) or info.get("regularMarketPrice", 0) or 0
        raw_shares = info.get("impliedSharesOutstanding") or info.get("sharesOutstanding") or 0
        return (raw_shares * raw_price * quote_fx_rate) if raw_shares and raw_price else 0

    def _parse_finviz_abbrev_to_raw(self, value):
        return parse_abbrev_to_raw(value)

    def _unwrap_annual(self, statement):
        return statements.unwrap_annual(statement)

    def _latest_row_raw(self, statement, labels):
        return statements.latest_row_raw(statement, labels)

    def _statement_latest_value(self, statement, labels):
        return statements.statement_latest_value(statement, labels)

    def _camel_to_label(self, key):
        return statements.camel_to_label(key)

    def _statement_type_name(self, item):
        return statements.statement_type_name(item)

    def _series_points(self, item, key):
        return statements.series_points(item, key)

    def build_statement_from_timeseries_results(self, selected_results, type_map, formatter):
        return statements.build_statement_from_timeseries_results(selected_results, type_map, formatter)

    def build_income_statement_from_timeseries_results(self, selected_results, _identity_formatter=None, formatter=None):
        formatter = formatter or self._format_money
        return self.build_statement_from_timeseries_results(selected_results, INCOME_STATEMENT_TYPES, formatter)

    def build_balance_sheet_from_timeseries_results(self, selected_results, formatter=None):
        return self.build_statement_from_timeseries_results(selected_results, BALANCE_STATEMENT_TYPES, formatter or self._format_money)

    def build_cash_flow_statement_from_timeseries_results(self, selected_results, formatter=None):
        return self.build_statement_from_timeseries_results(selected_results, CASH_FLOW_STATEMENT_TYPES, formatter or self._format_money)

    def _merge_statement_rows(self, primary, secondary):
        return statements.merge_statement_rows(primary, secondary)

    def _ordered_df_index(self, df, order_map):
        return statements.ordered_df_index(df, order_map)

    def _resolve_display_label(self, label, order_map):
        return statements.resolve_display_label(label, order_map)

    def _is_ttm_column(self, col):
        return statements.is_ttm_column(col)

    def _df_history_columns(self, df):
        return statements.df_history_columns(df)

    def _df_official_ttm_value(self, annual_df, row_labels):
        return statements.df_official_ttm_value(annual_df, row_labels)

    def _df_with_ttm_column(self, annual_df, ttm_df):
        return statements.df_with_ttm_column(annual_df, ttm_df)

    def _can_sum_ttm_label(self, label):
        return statements.can_sum_ttm_label(label)

    def _df_to_statement(self, df, formatter=None, ttm_label="TTM", order_map=None, quarterly_df=None):
        return statements.df_to_statement(df, formatter or self._format_money, ttm_label, order_map, quarterly_df)

    def _df_to_quarterly_statement(self, df, formatter=None, order_map=None):
        return statements.df_to_quarterly_statement(df, formatter or self._format_money, order_map)

    def _df_raw_value(self, df, row_labels, col_index=0):
        return statements.df_raw_value(df, row_labels, col_index)

    def _df_ttm_value(self, quarterly_df, annual_df, row_labels, absolute=False):
        return statements.df_ttm_value(quarterly_df, annual_df, row_labels, absolute)

    def _add_adjusted_operating_income(self, income_statement, cash_flow_statement, formatter=None):
        return statements.add_adjusted_operating_income(income_statement, cash_flow_statement, formatter or self._format_money)

    def _add_tax_rate(self, income_statement, formatter=None):
        return statements.add_tax_rate(income_statement, formatter or self._format_percent)

    def fetch_yfinance_data(self, ticker, finviz_ev_raw=0, finviz_market_cap_raw=0, finviz_metrics=None):
        """Fetch all data using yfinance package. Returns the same tuple as fetch_yahoo_finance_data."""
        import pandas as pd
        finviz_metrics = finviz_metrics or {}
        fetch_started = time.perf_counter()
        timing_stages = []

        def record_stage(key, label, started, status="ok"):
            timing_stages.append({
                "key": key,
                "label": label,
                "seconds": round(time.perf_counter() - started, 3),
                "status": status,
            })

        def timed(key, label, func):
            stage_started = time.perf_counter()
            try:
                return func()
            except Exception:
                record_stage(key, label, stage_started, "error")
                raise
            finally:
                if not any(stage.get("key") == key and stage.get("seconds") is not None for stage in timing_stages):
                    record_stage(key, label, stage_started)

        try:
            stock = yf.Ticker(ticker)
            info = timed("info", "Quote summary / info", lambda: stock.info or {})
            self._request_fetch_count = getattr(self, "_request_fetch_count", 0) + 1

            # Fetch currency rate early so all values can be USD-normalized
            raw_currency = (info.get("financialCurrency") or info.get("currency"))
            financial_currency = self._infer_currency_from_ticker(ticker, raw_currency)
            if financial_currency:
                financial_currency = financial_currency.upper()
            else:
                financial_currency = "USD"
            
            print(f"[FETCH] Ticker: {ticker}, Raw Currency: {raw_currency}, Inferred: {financial_currency}")

            def fetch_fx_rate(currency):
                if currency == "USD":
                    return 1.0
                try:
                    fx_ticker = yf.Ticker(f"{currency}USD=X")
                    fx_info = fx_ticker.fast_info
                    return float(fx_info.last_price or 1.0) or 1.0
                except Exception as e:
                    print(f"yfinance FX warning for {currency}: {e}")
                    return 1.0

            def fetch_statement_frames():
                statement_stock = yf.Ticker(ticker)
                return {
                    "annual_income": timed("annual_income", "Annual income statement", lambda: statement_stock.financials),
                    "ttm_income": timed("ttm_income", "Official TTM income statement", lambda: statement_stock.ttm_income_stmt),
                    "quarterly_income": timed("quarterly_income", "Quarterly income statement", lambda: statement_stock.quarterly_financials),
                    "annual_balance": timed("annual_balance", "Annual balance sheet", lambda: statement_stock.balance_sheet),
                    "quarterly_balance": timed("quarterly_balance", "Quarterly balance sheet", lambda: statement_stock.quarterly_balance_sheet),
                    "annual_cashflow": timed("annual_cashflow", "Annual cash flow", lambda: statement_stock.cashflow),
                    "ttm_cashflow": timed("ttm_cashflow", "Official TTM cash flow", lambda: statement_stock.ttm_cash_flow),
                    "quarterly_cashflow": timed("quarterly_cashflow", "Quarterly cash flow", lambda: statement_stock.quarterly_cashflow),
                }

            def fetch_estimate_frames():
                try:
                    estimate_stock = yf.Ticker(ticker)
                    return {
                        "earnings": timed("earnings_estimate", "Earnings estimates", lambda: estimate_stock.earnings_estimate),
                        "revenue": timed("revenue_estimate", "Revenue estimates", lambda: estimate_stock.revenue_estimate),
                    }
                except Exception as e:
                    print(f"yfinance estimates warning: {e}")
                    return {"earnings": None, "revenue": None}

            def fetch_analyst_recommendations():
                try:
                    rec_stock = yf.Ticker(ticker)
                    recs = timed("recommendations", "Analyst recommendations", lambda: rec_stock.recommendations)
                    if recs is not None and not recs.empty:
                        latest = recs.iloc[-1] if len(recs) > 0 else {}
                        return {
                            "strongBuy": int(latest.get("strongBuy", 0) or 0),
                            "buy": int(latest.get("buy", 0) or 0),
                            "hold": int(latest.get("hold", 0) or 0),
                            "sell": int(latest.get("sell", 0) or 0),
                            "strongSell": int(latest.get("strongSell", 0) or 0),
                        }
                except Exception:
                    pass
                return {}

            quote_currency = self._infer_currency_from_ticker(ticker, info.get("currency")).upper()

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    "statements": executor.submit(lambda: timed("statements_total", "Statements group", fetch_statement_frames)),
                    "estimates": executor.submit(lambda: timed("estimates_total", "Estimates group", fetch_estimate_frames)),
                    "recommendations": executor.submit(fetch_analyst_recommendations),
                }
                self._request_fetch_count += 4  # statements, earnings estimates, revenue estimates, recommendations
                if financial_currency != "USD":
                    futures["financial_fx"] = executor.submit(lambda: timed("financial_fx", f"{financial_currency}/USD FX", lambda: fetch_fx_rate(financial_currency)))
                    self._request_fetch_count += 1
                if quote_currency != "USD" and quote_currency != financial_currency:
                    futures["quote_fx"] = executor.submit(lambda: timed("quote_fx", f"{quote_currency}/USD FX", lambda: fetch_fx_rate(quote_currency)))
                    self._request_fetch_count += 1

                financial_fx_rate = futures["financial_fx"].result() if "financial_fx" in futures else 1.0
                if quote_currency == financial_currency:
                    quote_fx_rate = financial_fx_rate
                else:
                    quote_fx_rate = futures["quote_fx"].result() if "quote_fx" in futures else 1.0
                statement_frames = futures["statements"].result()
                estimate_frames = futures["estimates"].result()
                analyst_recommendations = futures["recommendations"].result()
            
            print(f"[FX] Financial Rate: {financial_fx_rate}, Quote Rate: {quote_fx_rate}")

            build_started = time.perf_counter()

            # Currency-aware formatter: multiplies raw values by FX rate before formatting
            def fx_formatter(val):
                if val is None:
                    return "--"
                try:
                    return self._format_money(float(val) * financial_fx_rate)
                except Exception:
                    return "--"

            annual_income = statement_frames["annual_income"]
            ttm_income = statement_frames["ttm_income"]
            quarterly_income = statement_frames["quarterly_income"]
            annual_balance = statement_frames["annual_balance"]
            quarterly_balance = statement_frames["quarterly_balance"]
            annual_cashflow = statement_frames["annual_cashflow"]
            ttm_cashflow = statement_frames["ttm_cashflow"]
            quarterly_cashflow = statement_frames["quarterly_cashflow"]

            annual_income = self._df_with_ttm_column(annual_income, ttm_income)
            annual_cashflow = self._df_with_ttm_column(annual_cashflow, ttm_cashflow)

            income_statement = {
                "annual": self._df_to_statement(annual_income, formatter=fx_formatter, order_map=INCOME_STATEMENT_TYPES, quarterly_df=quarterly_income),
                "quarterly": self._df_to_quarterly_statement(quarterly_income, formatter=fx_formatter, order_map=INCOME_STATEMENT_TYPES),
            }
            balance_statement = {
                "annual": self._df_to_statement(annual_balance, formatter=fx_formatter, ttm_label="MRQ", order_map=BALANCE_STATEMENT_TYPES),
                "quarterly": self._df_to_quarterly_statement(quarterly_balance, formatter=fx_formatter, order_map=BALANCE_STATEMENT_TYPES),
            }
            cash_flow_statement = {
                "annual": self._df_to_statement(annual_cashflow, formatter=fx_formatter, order_map=CASH_FLOW_STATEMENT_TYPES, quarterly_df=quarterly_cashflow),
                "quarterly": self._df_to_quarterly_statement(quarterly_cashflow, formatter=fx_formatter, order_map=CASH_FLOW_STATEMENT_TYPES),
            }
            income_statement = self._add_adjusted_operating_income(income_statement, cash_flow_statement)
            income_statement = self._add_tax_rate(income_statement)

            # Core metrics from DataFrames (TTM using quarterly sums) — all converted to USD
            last_year_revenue_raw = self._df_raw_value(annual_income, ["Total Revenue", "TotalRevenue"]) * financial_fx_rate
            revenue_raw = (self._df_ttm_value(quarterly_income, annual_income, ["Total Revenue", "TotalRevenue"]) or info.get("totalRevenue", 0) or 0) * financial_fx_rate
            operating_income_raw = (self._df_ttm_value(quarterly_income, annual_income, ["Operating Income", "OperatingIncome"]) or info.get("operatingIncome", 0) or 0) * financial_fx_rate
            gross_profit_raw = (self._df_ttm_value(quarterly_income, annual_income, ["Gross Profit", "GrossProfit"]) or info.get("grossProfits", 0) or 0) * financial_fx_rate
            capex_raw = abs(self._df_ttm_value(quarterly_cashflow, annual_cashflow, ["Capital Expenditure", "CapitalExpenditure"], absolute=True)) * financial_fx_rate
            da_raw = self._df_ttm_value(quarterly_cashflow, annual_cashflow, ["Depreciation And Amortization", "DepreciationAndAmortization", "Reconciled Depreciation", "ReconciledDepreciation"])
            if not da_raw:
                da_raw = self._df_ttm_value(quarterly_income, annual_income, ["Reconciled Depreciation", "ReconciledDepreciation"])
            da_raw = abs(da_raw) * financial_fx_rate if da_raw else 0

            gross_ppe_raw = (self._df_raw_value(annual_balance, ["Gross PPE", "GrossPPE"]) or self._df_raw_value(annual_balance, ["Net PPE", "NetPPE"])) * financial_fx_rate
            net_fixed_assets_raw = self._df_raw_value(annual_balance, ["Net PPE", "NetPPE"]) * financial_fx_rate
            receivables_raw = self._df_raw_value(annual_balance, ["Accounts Receivable", "AccountsReceivable", "Net Receivables"]) * financial_fx_rate
            inventory_raw = self._df_raw_value(annual_balance, ["Inventory"]) * financial_fx_rate
            accounts_payable_raw = self._df_raw_value(annual_balance, ["Accounts Payable", "AccountsPayable"]) * financial_fx_rate

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
                cols = self._df_history_columns(annual_income)
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
                                    # Update label based on actual years used
                                    display_years = round(years) if years >= 0.9 else round(years, 1)
                                    gp_3y_label = f"{display_years}Y {label_out.split(' ', 1)[1]}"
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

            # Revenue estimates from info (native currency — will be converted to USD below)
            cy_revenue_raw = info.get("revenueEstimates", {}).get("avg", 0) if isinstance(info.get("revenueEstimates"), dict) else 0
            ny_revenue_raw = 0
            cy_growth_raw = None
            ny_growth_raw = None

            # Analyst estimates from yfinance only.
            try:
                ee = estimate_frames.get("earnings")
                if ee is not None and not ee.empty:
                    if "0y" in ee.index:
                        cy_eps_raw = float(ee.loc["0y", "avg"]) if "avg" in ee.columns and pd.notna(ee.loc["0y", "avg"]) else cy_eps_raw
                        year_ago_eps_raw = float(ee.loc["0y", "yearAgoEps"]) if "yearAgoEps" in ee.columns and pd.notna(ee.loc["0y", "yearAgoEps"]) else year_ago_eps_raw
                        cy_eps_growth_raw = float(ee.loc["0y", "growth"]) if "growth" in ee.columns and pd.notna(ee.loc["0y", "growth"]) else cy_eps_growth_raw
                    if "+1y" in ee.index:
                        ny_eps_raw = float(ee.loc["+1y", "avg"]) if "avg" in ee.columns and pd.notna(ee.loc["+1y", "avg"]) else ny_eps_raw
                        ny_eps_growth_raw = float(ee.loc["+1y", "growth"]) if "growth" in ee.columns and pd.notna(ee.loc["+1y", "growth"]) else ny_eps_growth_raw

                re_est = estimate_frames.get("revenue")
                if re_est is not None and not re_est.empty:
                    if "0y" in re_est.index:
                        cy_revenue_raw = float(re_est.loc["0y", "avg"]) if "avg" in re_est.columns and pd.notna(re_est.loc["0y", "avg"]) else cy_revenue_raw
                        cy_growth_raw = float(re_est.loc["0y", "growth"]) if "growth" in re_est.columns and pd.notna(re_est.loc["0y", "growth"]) else cy_growth_raw
                    if "+1y" in re_est.index:
                        ny_revenue_raw = float(re_est.loc["+1y", "avg"]) if "avg" in re_est.columns and pd.notna(re_est.loc["+1y", "avg"]) else ny_revenue_raw
                        ny_growth_raw = float(re_est.loc["+1y", "growth"]) if "growth" in re_est.columns and pd.notna(re_est.loc["+1y", "growth"]) else ny_growth_raw
            except Exception as e:
                print(f"yfinance estimates warning: {e}")

            # Convert revenue estimates and 3Y GP values from native currency to USD
            cy_revenue_raw = (cy_revenue_raw or 0) * financial_fx_rate
            ny_revenue_raw = (ny_revenue_raw or 0) * financial_fx_rate
            if cy_growth_raw is None and cy_revenue_raw and last_year_revenue_raw:
                cy_growth_raw = (cy_revenue_raw / abs(last_year_revenue_raw)) - 1
            gp_3y_start_raw = (gp_3y_start_raw or 0) * financial_fx_rate
            gp_3y_end_raw = (gp_3y_end_raw or 0) * financial_fx_rate

            # Apply correct conversion to EPS based on quote vs financial currency
            cy_eps_raw = (cy_eps_raw or 0) * financial_fx_rate
            ny_eps_raw = (ny_eps_raw or 0) * financial_fx_rate
            year_ago_eps_raw = (year_ago_eps_raw or 0) * financial_fx_rate

            # Market cap and valuation use quote_fx_rate
            market_cap_raw = self._market_cap_from_info(info, quote_fx_rate)
            # Balance sheet cash/debt values are in financial currency
            cash_bucket_raw = self._latest_row_raw(balance_statement, ["Cash, Equivalents & Short Term Investments", "Cash & Short Term Investments", "Cash Cash Equivalents and Short Term Investments"])
            if not cash_bucket_raw:
                cash_bucket_raw = self._latest_row_raw(balance_statement, ["Cash & Cash Equivalents", "Cash and Cash Equivalents"]) + self._latest_row_raw(balance_statement, ["Other Short Term Investments", "Short Term Investments"])
            if not cash_bucket_raw:
                cash_bucket_raw = self._df_raw_value(annual_balance, ["Cash Cash Equivalents And Short Term Investments", "CashCashEquivalentsAndShortTermInvestments", "Cash And Cash Equivalents", "CashAndCashEquivalents"]) * financial_fx_rate
            total_debt_raw = self._df_raw_value(annual_balance, ["Total Debt", "TotalDebt"]) * financial_fx_rate
            if not total_debt_raw:
                total_debt_raw = (self._df_raw_value(annual_balance, ["Current Debt", "CurrentDebt"]) + self._df_raw_value(annual_balance, ["Long Term Debt", "LongTermDebt"])) * financial_fx_rate
            net_cash_raw = cash_bucket_raw - total_debt_raw if cash_bucket_raw or total_debt_raw else 0
            derived_enterprise_value_raw = market_cap_raw - net_cash_raw if market_cap_raw else 0

            valuation_raw = derived_enterprise_value_raw or market_cap_raw
            valuation_basis = "derivedEV"
            valuation_prefix = "EV"
            valuation_numerator_label = "Derived Enterprise Value"

            cy_adj_inc_raw = cy_revenue_raw * adj_margin_ratio if cy_revenue_raw and adj_margin_ratio else 0
            ny_adj_inc_raw = ny_revenue_raw * adj_margin_ratio if ny_revenue_raw and adj_margin_ratio else 0
            nwc_raw = receivables_raw + inventory_raw - accounts_payable_raw
            roc_denominator_raw = nwc_raw + net_fixed_assets_raw

            current_price_raw = (info.get("currentPrice", 0) or info.get("regularMarketPrice", 0) or 0) * quote_fx_rate
            target_mean_raw = (info.get("targetMeanPrice", 0) or 0) * quote_fx_rate
            target_low_raw = (info.get("targetLowPrice", 0) or 0) * quote_fx_rate
            target_high_raw = (info.get("targetHighPrice", 0) or 0) * quote_fx_rate
            target_move_raw = ((target_mean_raw - current_price_raw) / current_price_raw) if target_mean_raw and current_price_raw else None

            recommendation_mean = info.get("recommendationMean", 0) or 0
            recommendation_key = info.get("recommendationKey", "--") or "--"

            company_name = info.get("longName") or info.get("shortName") or ticker

            def safe_ratio(num, denom):
                return (num / denom) if num and denom else None

            def safe_display(value, formatter):
                return formatter(value) if value is not None else "--"

            short_float_raw = info.get("shortPercentOfFloat") if info.get("shortPercentOfFloat") else None
            structured_metrics = self._structured_metrics([
                ("income", operating_income_raw, self._format_money(operating_income_raw), "money"),
                ("margin", adj_margin_ratio or None, self._format_percent(adj_margin_ratio) if adj_margin_ratio else "--", "percent"),
                ("grossMargin", gross_margin_ratio, self._format_percent(gross_margin_ratio) if gross_margin_ratio is not None else "--", "percent"),
                ("ev_cy_ebit", safe_ratio(valuation_raw, cy_adj_inc_raw), safe_display(safe_ratio(valuation_raw, cy_adj_inc_raw), self._format_3sig), "ratio"),
                ("ev_ny_ebit", safe_ratio(valuation_raw, ny_adj_inc_raw), safe_display(safe_ratio(valuation_raw, ny_adj_inc_raw), self._format_3sig), "ratio"),
                ("adj_income", adj_income_raw, self._format_money(adj_income_raw), "money"),
                ("capex", capex_raw, self._format_money(capex_raw), "money"),
                ("da", da_raw, self._format_money(da_raw), "money"),
                ("ev", valuation_raw, self._format_money(valuation_raw), "money"),
                ("ev_adj_ebit", safe_ratio(valuation_raw, adj_income_raw), safe_display(safe_ratio(valuation_raw, adj_income_raw), self._format_3sig), "ratio"),
                ("cy_growth", cy_growth_raw, self._format_percent(cy_growth_raw) if cy_growth_raw is not None else "--", "percent"),
                ("ny_growth", ny_growth_raw, self._format_percent(ny_growth_raw) if ny_growth_raw is not None else "--", "percent"),
                ("gp_3y_growth", gp_3y_growth_raw, self._format_percent(gp_3y_growth_raw) if gp_3y_growth_raw is not None else "--", "percent"),
                ("gp_3y_start", gp_3y_start_raw or None, self._format_money(gp_3y_start_raw) if gp_3y_start_raw else "--", "money"),
                ("gp_3y_end", gp_3y_end_raw or None, self._format_money(gp_3y_end_raw) if gp_3y_end_raw else "--", "money"),
                ("rndAdjIncome", safe_ratio(rnd_raw, adj_income_raw), safe_display(safe_ratio(rnd_raw, adj_income_raw), self._format_percent), "percent"),
                ("cy_adj_inc", cy_adj_inc_raw or None, self._format_money(cy_adj_inc_raw) if cy_adj_inc_raw else "--", "money"),
                ("ny_adj_inc", ny_adj_inc_raw or None, self._format_money(ny_adj_inc_raw) if ny_adj_inc_raw else "--", "money"),
                ("marketCap", market_cap_raw, self._format_money(market_cap_raw), "money"),
                ("netCash", net_cash_raw, self._format_money(net_cash_raw), "money"),
                ("derivedEnterpriseValue", derived_enterprise_value_raw, self._format_money(derived_enterprise_value_raw), "money"),
                ("revenue", revenue_raw, self._format_money(revenue_raw), "money"),
                ("operating_margin", operating_margin_ratio or None, self._format_percent(operating_margin_ratio) if operating_margin_ratio else "--", "percent"),
                ("da_minus_capex", da_minus_capex_raw, self._format_money(da_minus_capex_raw) if da_minus_capex_raw else "0", "money"),
                ("cy_revenue", cy_revenue_raw or None, self._format_money(cy_revenue_raw) if cy_revenue_raw else "--", "money"),
                ("ny_revenue", ny_revenue_raw or None, self._format_money(ny_revenue_raw) if ny_revenue_raw else "--", "money"),
                ("grossPpe", gross_ppe_raw, self._format_money(gross_ppe_raw), "money"),
                ("adjEbitGrossPpe", safe_ratio(adj_income_raw, gross_ppe_raw), safe_display(safe_ratio(adj_income_raw, gross_ppe_raw), self._format_percent), "percent"),
                ("capexAdjIncome", safe_ratio(investment_capex_raw, adj_income_raw), safe_display(safe_ratio(investment_capex_raw, adj_income_raw), self._format_percent), "percent"),
                ("investmentCapex", investment_capex_raw, self._format_money(investment_capex_raw) if investment_capex_raw else "0", "money"),
                ("roc", safe_ratio(adj_income_raw, roc_denominator_raw), safe_display(safe_ratio(adj_income_raw, roc_denominator_raw), self._format_percent), "percent"),
                ("netWorkingCapital", nwc_raw, self._format_money(nwc_raw), "money"),
                ("netFixedAssets", net_fixed_assets_raw, self._format_money(net_fixed_assets_raw), "money"),
                ("receivables", receivables_raw, self._format_money(receivables_raw), "money"),
                ("inventory", inventory_raw, self._format_money(inventory_raw), "money"),
                ("accountsPayable", accounts_payable_raw, self._format_money(accounts_payable_raw), "money"),
                ("shortFloat", short_float_raw, self._format_percent(short_float_raw) if short_float_raw else "--", "percent"),
                ("currentPrice", current_price_raw, self._format_3sig(current_price_raw), "money"),
                ("targetMeanPrice", target_mean_raw, self._format_3sig(target_mean_raw), "money"),
                ("targetLowPrice", target_low_raw, self._format_3sig(target_low_raw), "money"),
                ("targetHighPrice", target_high_raw, self._format_3sig(target_high_raw), "money"),
                ("targetMove", target_move_raw, self._format_percent(target_move_raw) if target_move_raw is not None else "--", "percent"),
                ("currentYearEps", cy_eps_raw, self._format_3sig(cy_eps_raw), "number"),
                ("nextYearEps", ny_eps_raw, self._format_3sig(ny_eps_raw), "number"),
                ("yearAgoEps", year_ago_eps_raw, self._format_3sig(year_ago_eps_raw), "number"),
                ("currentYearEpsGrowth", cy_eps_growth_raw, self._format_percent(cy_eps_growth_raw) if cy_eps_growth_raw is not None else "--", "percent"),
                ("nextYearEpsGrowth", ny_eps_growth_raw, self._format_percent(ny_eps_growth_raw) if ny_eps_growth_raw is not None else "--", "percent"),
                ("priceCurrentEps", safe_ratio(current_price_raw, year_ago_eps_raw), safe_display(safe_ratio(current_price_raw, year_ago_eps_raw), self._format_3sig), "ratio"),
                ("priceCyEps", safe_ratio(current_price_raw, cy_eps_raw), safe_display(safe_ratio(current_price_raw, cy_eps_raw), self._format_3sig), "ratio"),
                ("priceNyEps", safe_ratio(current_price_raw, ny_eps_raw), safe_display(safe_ratio(current_price_raw, ny_eps_raw), self._format_3sig), "ratio"),
            ], currency="USD")

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
                "usd_fx_rate": quote_fx_rate,
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
                "short_float": self._format_percent(info.get("shortPercentOfFloat")) if info.get("shortPercentOfFloat") else "--",
                "structured_metrics": structured_metrics,
            }
            record_stage("build_payload", "Build metrics and statements", build_started)
            total_seconds = round(time.perf_counter() - fetch_started, 3)
            self._fetch_timing = {
                "source": "fresh",
                "totalSeconds": total_seconds,
                "stages": sorted(timing_stages, key=lambda stage: stage.get("seconds", 0), reverse=True),
            }
            return tuple(values[key] for key in FETCH_RESULT_FIELDS)
        except Exception as e:
            self._fetch_timing = {
                "source": "error",
                "totalSeconds": round(time.perf_counter() - fetch_started, 3),
                "stages": sorted(timing_stages, key=lambda stage: stage.get("seconds", 0), reverse=True),
            }
            print(f"yfinance fetch error for {ticker}: {e}")
            raise

    def fetch_yahoo_finance_data(self, ticker, finviz_ev_raw=0, finviz_market_cap_raw=0, finviz_metrics=None):
        if not HAS_YFINANCE:
            print(f"[yfinance] unavailable for {ticker}")
            return self._empty_fetch_tuple(ticker)

        try:
            result = self.fetch_yfinance_data(ticker, finviz_ev_raw=0, finviz_market_cap_raw=0, finviz_metrics={})
            print(f"[yfinance] Successfully fetched data for {ticker}")
            return result
        except Exception as e:
            print(f"[yfinance] Failed for {ticker}: {e}")
            return self._empty_fetch_tuple(ticker)

    def _prune_latest(self, payload):
        if not isinstance(payload, dict): return payload
        for key in ["incomeStatement", "balanceStatement", "cashFlowStatement"]:
            stmt = payload.get(key)
            if not isinstance(stmt, dict): continue
            q = stmt.get("quarterly")
            if not isinstance(q, dict): continue
            periods = q.get("periods")
            if not periods: continue
            
            prune_idx = -1
            if str(periods[0]).upper() == "LATEST":
                prune_idx = 0
            elif str(periods[-1]).upper() == "LATEST":
                prune_idx = len(periods) - 1
                
            if prune_idx != -1:
                q["periods"] = [p for i, p in enumerate(periods) if i != prune_idx]
                for row in q.get("rows", []):
                    vals = row.get("values")
                    if vals:
                        row["values"] = [v for i, v in enumerate(vals) if i != prune_idx]
        return payload

    def handle_api_request(self, ticker, refresh=False):
        if not ticker:
            self._send_response(400, {"error": "Ticker is required"})
            return

        self._request_fetch_count = 0
        self._fetch_timing = {
            "source": "cache",
            "totalSeconds": 0,
            "stages": [],
        }
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
            flat = self._unwrap_annual(statement)
            periods = flat.get("periods") or []
            if not periods or periods[0] != "TTM":
                return False
            labels = {"Total Revenue", "Gross Profit", "Operating Income"}
            for row in flat.get("rows", []):
                if row.get("label") in labels:
                    values = row.get("values") or []
                    if values and values[0] in (None, "", "--"):
                        return True
            return False

        def cache_has_missing_adjusted_operating_income(payload):
            statement = payload.get("incomeStatement") or {}
            flat = self._unwrap_annual(statement)
            return not any(
                row.get("label") == "Adjusted Operating Income"
                for row in flat.get("rows", [])
            )

        def cache_is_usable(payload):
            return (
                isinstance(payload, dict)
                and payload.get("payloadVersion") == PAYLOAD_VERSION
                and payload.get("marketCap") not in (None, "", "--")
                and payload.get("incomeStatement")
                and payload.get("balanceStatement")
                and payload.get("cashFlowStatement")
                and not cache_has_missing_ttm_anchor(payload)
                and not cache_has_missing_adjusted_operating_income(payload)
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
            payload["fetchTiming"] = {
                "source": "cache" if fetch_count == 0 else "stale-cache",
                "totalSeconds": 0,
                "stages": [],
            }
            return payload

        if not refresh and ticker in cache and cache[ticker].get('date') == today:
            cached_payload = cache[ticker].get('data', {})
            if cache_is_usable(cached_payload):
                self._send_response(200, self._prune_latest(enrich_cached_payload(cached_payload, cache[ticker], fetch_count=0)))
                return

        finviz_market_cap_raw = 0
        finviz_enterprise_value_raw = 0
        result = dict(zip(FETCH_RESULT_FIELDS, self.fetch_yahoo_finance_data(
            ticker,
            finviz_ev_raw=0,
            finviz_market_cap_raw=0,
            finviz_metrics={},
        )))

        if result.get("company_name") == ticker and result.get("valuation_basis") == "unavailable":
            can_preserve_previous = (
                refresh
                and isinstance(previous_payload, dict)
                and previous_payload.get("marketCap") not in (None, "", "--")
            ) or cache_is_usable(previous_payload)
            if can_preserve_previous:
                self._send_response(
                    200,
                    self._prune_latest(enrich_cached_payload(
                        previous_payload,
                        previous_cache_entry,
                        fetch_count=getattr(self, "_request_fetch_count", 0),
                        refresh_error=refresh,
                    )),
                )
                return


        payload = {
            "ticker": ticker,
            "shortFloat": result.get("short_float") or "--",
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
            "metrics": result.get("structured_metrics") or {},
            "evSource": "derived" if result["valuation_basis"] == "derivedEV" else "finviz" if finviz_enterprise_value_raw and result["valuation_basis"] == "enterpriseValue" else "unavailable",
            "marketCapSource": "yahoo",
            "dataDate": today,
            "pulledAt": pulled_at,
            "fetchCount": getattr(self, "_request_fetch_count", 0),
            "fetchTiming": getattr(self, "_fetch_timing", {"source": "fresh", "totalSeconds": None, "stages": []}),
        }

        cache[ticker] = {'date': today, 'pulledAt': pulled_at, 'data': payload}
        save_cache(cache)
        self._send_response(200, self._prune_latest(payload))

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
