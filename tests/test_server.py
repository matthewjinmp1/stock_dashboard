import json
import threading
import unittest
from unittest import mock
from urllib.error import HTTPError

import server


FETCH_RESULT_FIELDS = [
    "income",
    "margin",
    "gross_margin",
    "ev_cy_ebit",
    "ev_ny_ebit",
    "adj_income",
    "capex",
    "da",
    "ev",
    "ev_adj_ebit",
    "cy_growth",
    "ny_growth",
    "gp_3y_growth",
    "gp_3y_start",
    "gp_3y_end",
    "gp_3y_label",
    "rnd_adj_income",
    "cy_adj_inc",
    "ny_adj_inc",
    "market_cap",
    "net_cash",
    "derived_enterprise_value",
    "revenue",
    "operating_margin",
    "da_minus_capex",
    "cy_revenue",
    "ny_revenue",
    "gross_ppe",
    "adj_ebit_gross_ppe",
    "capex_adj_income",
    "investment_capex",
    "roc",
    "net_working_capital",
    "net_fixed_assets",
    "receivables",
    "inventory",
    "accounts_payable",
    "financial_currency",
    "usd_fx_rate",
    "company_name",
    "income_statement",
    "balance_statement",
    "cash_flow_statement",
    "current_price",
    "target_mean_price",
    "target_low_price",
    "target_high_price",
    "target_move",
    "recommendation_mean",
    "recommendation_key",
    "analyst_recommendations",
    "valuation_basis",
    "valuation_prefix",
    "valuation_numerator_label",
    "current_year_eps",
    "next_year_eps",
    "year_ago_eps",
    "current_year_eps_growth",
    "next_year_eps_growth",
    "price_current_eps",
    "price_cy_eps",
    "price_ny_eps",
]


class DummyOpener:
    def __init__(self):
        self.addheaders = []


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def read(self):
        if isinstance(self.payload, bytes):
            return self.payload
        return self.payload.encode("utf-8")


class FakePageOpener:
    def __init__(self, html):
        self.html = html

    def open(self, _url, timeout=10):
        return FakeResponse(self.html)


def make_handler():
    handler = server.Handler.__new__(server.Handler)
    handler._request_fetch_count = 0
    handler._fetch_count_lock = threading.Lock()
    return handler


def make_quote_summary_payload():
    return {
        "quoteSummary": {
            "result": [
                {
                    "financialData": {
                        "financialCurrency": "USD",
                        "operatingMargins": {"raw": 0.2},
                        "grossMargins": {"raw": 0.5},
                        "currentPrice": {"raw": 10},
                        "targetMeanPrice": {"raw": 12},
                        "targetLowPrice": {"raw": 8},
                        "targetHighPrice": {"raw": 15},
                        "recommendationMean": {"raw": 2.0},
                        "recommendationKey": "buy",
                    },
                    "earningsTrend": {
                        "trend": [
                            {
                                "period": "0y",
                                "revenueEstimate": {
                                    "avg": {"raw": 120},
                                    "growth": {"raw": 0.2},
                                },
                                "earningsEstimate": {
                                    "avg": {"raw": 12},
                                    "growth": {"raw": 0.2},
                                    "yearAgoEps": {"raw": 10},
                                },
                            },
                            {
                                "period": "+1y",
                                "revenueEstimate": {
                                    "avg": {"raw": 150},
                                    "growth": {"raw": 0.25},
                                },
                                "earningsEstimate": {
                                    "avg": {"raw": 15},
                                    "growth": {"raw": 0.25},
                                },
                            },
                        ]
                    },
                    "price": {
                        "longName": "Acme Corp.",
                        "regularMarketPrice": {"raw": 10},
                        "marketCap": {"raw": 180},
                        "currency": "USD",
                    },
                    "recommendationTrend": {
                        "trend": [
                            {
                                "period": "0m",
                                "strongBuy": 1,
                                "buy": 2,
                                "hold": 3,
                                "sell": 0,
                                "strongSell": 0,
                            }
                        ]
                    },
                    "incomeStatementHistory": {"incomeStatementHistory": []},
                    "balanceSheetHistory": {"balanceSheetStatements": []},
                    "cashflowStatementHistory": {"cashflowStatements": []},
                }
            ]
        }
    }


def make_timeseries_payload():
    def quarterly_series(key, values):
        return {
            "meta": {"type": [key]},
            key: [{"reportedValue": {"raw": value}} for value in values],
        }

    def annual_series(key, value):
        return {
            "meta": {"type": [key]},
            key: [{"reportedValue": {"raw": value}}],
        }

    return {
        "timeseries": {
            "result": [
                quarterly_series("quarterlyTotalRevenue", [25, 25, 25, 25]),
                quarterly_series("quarterlyOperatingIncome", [5, 5, 5, 5]),
                quarterly_series("quarterlyCapitalExpenditure", [-1, -1, -1, -1]),
                quarterly_series("quarterlyDepreciationAndAmortization", [2, 2, 2, 2]),
                annual_series("annualGrossPPE", 40),
                annual_series("annualNetPPE", 30),
                annual_series("annualAccountsReceivable", 12),
                annual_series("annualInventory", 3),
                annual_series("annualAccountsPayable", 5),
            ]
        }
    }


def fake_statement(label):
    return {"periods": ["TTM"], "rows": [{"label": label, "values": ["1"]}]}


def fake_balance_statement():
    return {
        "periods": ["MRQ"],
        "rows": [
            {"label": "Current Debt", "values": ["10"]},
            {"label": "Long Term Debt", "values": ["40"]},
            {"label": "Cash And Cash Equivalents", "values": ["15"]},
            {"label": "Other Short Term Investments", "values": ["5"]},
        ],
    }


def fake_income_statement_with_eps(ttm_value, annual_value):
    return {
        "periods": ["TTM", "2025-12-31"],
        "rows": [
            {"label": "Diluted EPS", "values": [ttm_value, annual_value]},
        ],
    }


class FetchYahooFinanceDataTests(unittest.TestCase):
    def setUp(self):
        self.handler = make_handler()

    def _run_fetch(self, *, finviz_ev_raw, finviz_market_cap_raw):
        quote_summary_payload = make_quote_summary_payload()
        timeseries_payload = make_timeseries_payload()

        def counted_open(_opener, url, timeout=3):
            if "quoteSummary" in url:
                return FakeResponse(json.dumps(quote_summary_payload))
            if "fundamentals-timeseries" in url:
                return FakeResponse(json.dumps(timeseries_payload))
            raise AssertionError(f"Unexpected URL: {url}")

        with mock.patch("server.urllib.request.build_opener", return_value=DummyOpener()), \
             mock.patch("server.urllib.request.install_opener"), \
             mock.patch.object(self.handler, "get_yahoo_crumb", return_value="crumb"), \
             mock.patch.object(self.handler, "get_usd_fx_rate", return_value=1.0), \
             mock.patch.object(self.handler, "_counted_open", side_effect=counted_open), \
             mock.patch.object(self.handler, "build_income_statement_from_page", return_value=fake_statement("Income")), \
             mock.patch.object(self.handler, "build_balance_sheet_from_page", return_value=fake_balance_statement()), \
             mock.patch.object(self.handler, "build_cash_flow_statement_from_page", return_value=fake_statement("Cash")):
            return dict(zip(FETCH_RESULT_FIELDS, self.handler.fetch_yahoo_finance_data(
                "ACME",
                finviz_ev_raw=finviz_ev_raw,
                finviz_market_cap_raw=finviz_market_cap_raw,
            )))

    def test_uses_enterprise_value_when_available(self):
        result = self._run_fetch(finviz_ev_raw=240, finviz_market_cap_raw=180)

        self.assertEqual(result["valuation_basis"], "enterpriseValue")
        self.assertEqual(result["valuation_prefix"], "EV")
        self.assertEqual(result["valuation_numerator_label"], "Current Enterprise Value")
        self.assertEqual(result["ev"], "240")
        self.assertEqual(result["market_cap"], "180")
        self.assertEqual(result["net_cash"], "-30")
        self.assertEqual(result["derived_enterprise_value"], "210")
        self.assertEqual(result["ev_adj_ebit"], "10")
        self.assertEqual(result["ev_cy_ebit"], "8.33")
        self.assertEqual(result["ev_ny_ebit"], "6.67")
        self.assertEqual(result["year_ago_eps"], "10")
        self.assertEqual(result["current_year_eps"], "12")
        self.assertEqual(result["next_year_eps"], "15")
        self.assertEqual(result["current_year_eps_growth"], "20%")
        self.assertEqual(result["next_year_eps_growth"], "25%")
        self.assertEqual(result["price_current_eps"], "1")
        self.assertEqual(result["price_cy_eps"], "0.833")
        self.assertEqual(result["price_ny_eps"], "0.667")
        self.assertEqual(result["investment_capex"], "0")

    def test_falls_back_to_market_cap_when_ev_missing(self):
        result = self._run_fetch(finviz_ev_raw=0, finviz_market_cap_raw=180)

        self.assertEqual(result["valuation_basis"], "marketCap")
        self.assertEqual(result["valuation_prefix"], "Mkt Cap")
        self.assertEqual(result["valuation_numerator_label"], "Current Market Cap")
        self.assertEqual(result["ev"], "180")
        self.assertEqual(result["market_cap"], "180")
        self.assertEqual(result["net_cash"], "-30")
        self.assertEqual(result["derived_enterprise_value"], "210")
        self.assertEqual(result["ev_adj_ebit"], "7.5")
        self.assertEqual(result["ev_cy_ebit"], "6.25")
        self.assertEqual(result["ev_ny_ebit"], "5")
        self.assertEqual(result["price_current_eps"], "1")
        self.assertEqual(result["price_cy_eps"], "0.833")
        self.assertEqual(result["price_ny_eps"], "0.667")

    def test_net_cash_prefers_combined_cash_bucket_without_double_counting(self):
        quote_summary_payload = make_quote_summary_payload()
        quote_summary_payload["quoteSummary"]["result"][0]["price"]["marketCap"] = {"raw": 180000000000}
        timeseries_payload = make_timeseries_payload()

        balance_statement = {
            "periods": ["MRQ", "2025-12-31"],
            "rows": [
                {"label": "Cash & Cash Equivalents", "values": ["30.2B", "30.2B"]},
                {"label": "Other Short Term Investments", "values": ["64.3B", "64.3B"]},
                {"label": "Cash, Equivalents & Short Term Investments", "values": ["94.6B", "94.6B"]},
                {"label": "Current Debt", "values": ["3B", "3B"]},
                {"label": "Long Term Debt", "values": ["40.2B", "40.2B"]},
            ],
        }

        def counted_open(_opener, url, timeout=3):
            if "quoteSummary" in url:
                return FakeResponse(json.dumps(quote_summary_payload))
            if "fundamentals-timeseries" in url:
                return FakeResponse(json.dumps(timeseries_payload))
            raise AssertionError(f"Unexpected URL: {url}")

        with mock.patch("server.urllib.request.build_opener", return_value=DummyOpener()), \
             mock.patch("server.urllib.request.install_opener"), \
             mock.patch.object(self.handler, "get_yahoo_crumb", return_value="crumb"), \
             mock.patch.object(self.handler, "get_usd_fx_rate", return_value=1.0), \
             mock.patch.object(self.handler, "_counted_open", side_effect=counted_open), \
             mock.patch.object(self.handler, "build_income_statement_from_page", return_value=fake_statement("Income")), \
             mock.patch.object(self.handler, "build_balance_sheet_from_page", return_value=balance_statement), \
             mock.patch.object(self.handler, "build_cash_flow_statement_from_page", return_value=fake_statement("Cash")):
            result = dict(zip(FETCH_RESULT_FIELDS, self.handler.fetch_yahoo_finance_data(
                "ACME",
                finviz_ev_raw=240000000000,
                finviz_market_cap_raw=180000000000,
            )))

        self.assertEqual(result["net_cash"], "51.4B")
        self.assertEqual(result["derived_enterprise_value"], "129B")

    def test_investment_capex_is_capex_less_depreciation_floored_at_zero(self):
        result = self._run_fetch(finviz_ev_raw=240, finviz_market_cap_raw=180)
        self.assertEqual(result["capex"], "4")
        self.assertEqual(result["da"], "8")
        self.assertEqual(result["investment_capex"], "0")
        self.assertEqual(result["capex_adj_income"], "0%")

    def test_restores_three_year_growth_and_rnd_spending_metrics(self):
        quote_summary_payload = make_quote_summary_payload()
        timeseries_payload = make_timeseries_payload()
        income_statement = {
            "periods": ["TTM", "2025-12-31", "2024-12-31", "2023-12-31", "2022-12-31"],
            "rows": [
                {"label": "Total Revenue", "values": ["100", "100", "90", "80", "70"]},
                {"label": "Gross Profit", "values": ["56", "56", "48", "44", "40"]},
                {"label": "Research & Development", "values": ["6", "6", "5", "4", "3"]},
            ],
        }

        def counted_open(_opener, url, timeout=3):
            if "quoteSummary" in url:
                return FakeResponse(json.dumps(quote_summary_payload))
            if "fundamentals-timeseries" in url:
                return FakeResponse(json.dumps(timeseries_payload))
            raise AssertionError(f"Unexpected URL: {url}")

        with mock.patch("server.urllib.request.build_opener", return_value=DummyOpener()), \
             mock.patch("server.urllib.request.install_opener"), \
             mock.patch.object(self.handler, "get_yahoo_crumb", return_value="crumb"), \
             mock.patch.object(self.handler, "get_usd_fx_rate", return_value=1.0), \
             mock.patch.object(self.handler, "_counted_open", side_effect=counted_open), \
             mock.patch.object(self.handler, "build_income_statement_from_page", return_value=income_statement), \
             mock.patch.object(self.handler, "build_balance_sheet_from_page", return_value=fake_balance_statement()), \
             mock.patch.object(self.handler, "build_cash_flow_statement_from_page", return_value=fake_statement("Cash")):
            result = dict(zip(FETCH_RESULT_FIELDS, self.handler.fetch_yahoo_finance_data(
                "ACME",
                finviz_ev_raw=240,
                finviz_market_cap_raw=180,
            )))

        self.assertEqual(result["gp_3y_label"], "3Y GP Growth")
        self.assertEqual(result["gp_3y_start"], "40")
        self.assertEqual(result["gp_3y_end"], "56")
        self.assertEqual(result["gp_3y_growth"], "40%")
        self.assertEqual(result["rnd_adj_income"], "25%")

    def test_falls_back_to_actual_annual_eps_when_year_ago_matches_current_year(self):
        quote_summary_payload = make_quote_summary_payload()
        quote_summary_payload["quoteSummary"]["result"][0]["earningsTrend"]["trend"][0]["earningsEstimate"] = {
            "avg": {"raw": 12},
            "yearAgoEps": {"raw": 12},
        }
        timeseries_payload = make_timeseries_payload()

        def counted_open(_opener, url, timeout=3):
            if "quoteSummary" in url:
                return FakeResponse(json.dumps(quote_summary_payload))
            if "fundamentals-timeseries" in url:
                return FakeResponse(json.dumps(timeseries_payload))
            raise AssertionError(f"Unexpected URL: {url}")

        with mock.patch("server.urllib.request.build_opener", return_value=DummyOpener()), \
             mock.patch("server.urllib.request.install_opener"), \
             mock.patch.object(self.handler, "get_yahoo_crumb", return_value="crumb"), \
             mock.patch.object(self.handler, "get_usd_fx_rate", return_value=1.0), \
             mock.patch.object(self.handler, "_counted_open", side_effect=counted_open), \
             mock.patch.object(self.handler, "build_income_statement_from_page", return_value=fake_income_statement_with_eps("12", "10")), \
             mock.patch.object(self.handler, "build_balance_sheet_from_page", return_value=fake_statement("Balance")), \
             mock.patch.object(self.handler, "build_cash_flow_statement_from_page", return_value=fake_statement("Cash")):
            result = dict(zip(FETCH_RESULT_FIELDS, self.handler.fetch_yahoo_finance_data(
                "ACME",
                finviz_ev_raw=240,
                finviz_market_cap_raw=180,
            )))

        self.assertEqual(result["year_ago_eps"], "10")
        self.assertEqual(result["price_current_eps"], "1")

    def test_converts_adr_eps_estimates_into_quote_currency_for_pe(self):
        quote_summary_payload = make_quote_summary_payload()
        result_payload = quote_summary_payload["quoteSummary"]["result"][0]
        result_payload["financialData"]["financialCurrency"] = "CNY"
        result_payload["financialData"]["currentPrice"] = {"raw": 100}
        result_payload["price"]["currency"] = "USD"
        result_payload["price"]["regularMarketPrice"] = {"raw": 100}
        result_payload["earningsTrend"]["trend"][0]["earningsEstimate"] = {
            "avg": {"raw": 500, "fmt": "50.00"},
            "growth": {"raw": 0.25},
            "yearAgoEps": {"raw": 400, "fmt": "40.00"},
        }
        result_payload["earningsTrend"]["trend"][1]["earningsEstimate"] = {
            "avg": {"raw": 800, "fmt": "80.00"},
            "growth": {"raw": 0.6},
        }
        timeseries_payload = make_timeseries_payload()

        def counted_open(_opener, url, timeout=3):
            if "quoteSummary" in url:
                return FakeResponse(json.dumps(quote_summary_payload))
            if "fundamentals-timeseries" in url:
                return FakeResponse(json.dumps(timeseries_payload))
            raise AssertionError(f"Unexpected URL: {url}")

        def fake_fx_rate(currency, opener=None):
            return 0.1 if currency == "CNY" else 1.0

        with mock.patch("server.urllib.request.build_opener", return_value=DummyOpener()), \
             mock.patch("server.urllib.request.install_opener"), \
             mock.patch.object(self.handler, "get_yahoo_crumb", return_value="crumb"), \
             mock.patch.object(self.handler, "get_usd_fx_rate", side_effect=fake_fx_rate), \
             mock.patch.object(self.handler, "_counted_open", side_effect=counted_open), \
             mock.patch.object(self.handler, "build_income_statement_from_page", return_value=fake_statement("Income")), \
             mock.patch.object(self.handler, "build_balance_sheet_from_page", return_value=fake_balance_statement()), \
             mock.patch.object(self.handler, "build_cash_flow_statement_from_page", return_value=fake_statement("Cash")):
            result = dict(zip(FETCH_RESULT_FIELDS, self.handler.fetch_yahoo_finance_data(
                "BABA",
                finviz_ev_raw=240,
                finviz_market_cap_raw=180,
            )))

        self.assertEqual(result["financial_currency"], "CNY")
        self.assertEqual(result["year_ago_eps"], "4")
        self.assertEqual(result["current_year_eps"], "5")
        self.assertEqual(result["next_year_eps"], "8")
        self.assertEqual(result["price_current_eps"], "25")
        self.assertEqual(result["price_cy_eps"], "20")
        self.assertEqual(result["price_ny_eps"], "12.5")

    def test_exception_fallback_keeps_full_tuple_shape(self):
        err = HTTPError(
            url="https://query1.finance.yahoo.com",
            code=401,
            msg="Unauthorized",
            hdrs=None,
            fp=None,
        )

        with mock.patch("server.urllib.request.build_opener", return_value=DummyOpener()), \
             mock.patch("server.urllib.request.install_opener"), \
             mock.patch.object(self.handler, "get_yahoo_crumb", side_effect=["crumb-a", "crumb-b"]), \
             mock.patch.object(self.handler, "_counted_open", side_effect=err), \
             mock.patch("builtins.print"):
            result = self.handler.fetch_yahoo_finance_data("FAIL", 0, 0)

        self.assertEqual(len(result), len(FETCH_RESULT_FIELDS))
        mapped = dict(zip(FETCH_RESULT_FIELDS, result))
        self.assertEqual(mapped["valuation_basis"], "unavailable")
        self.assertEqual(mapped["valuation_prefix"], "EV")
        self.assertEqual(mapped["company_name"], "FAIL")


class HandleApiRequestContractTests(unittest.TestCase):
    def test_test_ticker_returns_complete_fixture_without_external_fetches(self):
        handler = make_handler()
        captured = {}

        def fake_send_response(status, payload):
            captured["status"] = status
            captured["payload"] = payload

        with mock.patch.object(handler, "fetch_finviz_snapshot_metrics") as mock_finviz, \
             mock.patch.object(handler, "fetch_yahoo_finance_data") as mock_yahoo, \
             mock.patch.object(handler, "_send_response", side_effect=fake_send_response):
            handler.handle_api_request("TEST", refresh=True)

        self.assertEqual(captured["status"], 200)
        payload = captured["payload"]
        self.assertEqual(payload["ticker"], "TEST")
        self.assertEqual(payload["companyName"], "Test Fixture Corporation")
        self.assertEqual(payload["marketCap"], "500B")
        self.assertEqual(payload["netCash"], "20B")
        self.assertEqual(payload["derivedEnterpriseValue"], "480B")
        self.assertEqual(payload["grossMargin"], "60%")
        self.assertEqual(payload["capexAdjIncome"], "6.67%")
        self.assertEqual(payload["priceCyEps"], "10")
        self.assertEqual(payload["incomeStatement"]["rows"][0]["label"], "Total Revenue")
        self.assertEqual(payload["balanceStatement"]["rows"][2]["label"], "Cash, Equivalents & Short Term Investments")
        self.assertEqual(payload["cashFlowStatement"]["rows"][1]["label"], "Capital Expenditures")
        mock_finviz.assert_not_called()
        mock_yahoo.assert_not_called()

    def test_same_day_cache_is_reused_even_if_pulled_at_is_old(self):
        handler = make_handler()
        captured = {}
        today = server.datetime.date.today().isoformat()
        cached_payload = {
            "ticker": "META",
            "companyName": "Meta Platforms, Inc.",
            "shortFloat": "1.11%",
            "income": "83.3B",
            "adj_income": "83.3B",
            "capex": "69.7B",
            "da": "18.6B",
            "grossPpe": "254B",
            "adjEbitGrossPpe": "32.8%",
            "capexAdjIncome": "83.7%",
            "investmentCapex": "51.1B",
            "margin": "41.4%",
            "grossMargin": "82%",
            "ev_cy_ebit": "14",
            "ev_ny_ebit": "11.9",
            "ev": "1.46T",
            "ev_adj_ebit": "17.5",
            "roc": "40.1%",
            "cy_growth": "25%",
            "ny_growth": "17.9%",
            "marketCap": "1.45T",
            "netCash": "-2.65B",
            "derivedEnterpriseValue": "1.45T",
            "financialCurrency": "USD",
            "usdFxRate": 1.0,
            "valuationBasis": "enterpriseValue",
            "valuationPrefix": "EV",
            "valuationNumeratorLabel": "Current Enterprise Value",
            "currentYearEps": "29.6",
            "nextYearEps": "34.38",
            "yearAgoEps": "23.49",
            "currentYearEpsGrowth": "26%",
            "nextYearEpsGrowth": "16.1%",
            "priceCurrentEps": "24.5",
            "priceCyEps": "19.4",
            "priceNyEps": "16.7",
            "payloadVersion": server.PAYLOAD_VERSION,
            "evSource": "finviz",
            "marketCapSource": "yahoo",
            "incomeStatement": {
                "periods": ["TTM", "2025-12-31"],
                "rows": [
                    {"label": "Total Revenue", "values": ["201B", "180B"]},
                    {"label": "Gross Profit", "values": ["160B", "140B"]},
                    {"label": "Operating Income", "values": ["83.3B", "75B"]},
                ],
            },
            "balanceStatement": {
                "periods": ["MRQ", "2025-12-31"],
                "rows": [{"label": "Total Assets", "values": ["520B", "500B"]}],
            },
            "cashFlowStatement": {
                "periods": ["TTM", "2025-12-31"],
                "rows": [{"label": "Operating Cash Flow", "values": ["110B", "100B"]}],
            },
        }

        def fake_send_response(status, payload):
            captured["status"] = status
            captured["payload"] = payload

        with mock.patch("server.load_cache", return_value={
            "META": {
                "date": today,
                "pulledAt": "2026-04-10T00:00:00",
                "data": cached_payload,
            }
        }), \
             mock.patch.object(handler, "fetch_finviz_snapshot_metrics") as mock_finviz, \
             mock.patch.object(handler, "fetch_yahoo_finance_data") as mock_yahoo, \
             mock.patch.object(handler, "_send_response", side_effect=fake_send_response):
            handler.handle_api_request("META", refresh=False)

        self.assertEqual(captured["status"], 200)
        self.assertEqual(captured["payload"]["ticker"], "META")
        self.assertEqual(captured["payload"]["fetchCount"], 0)
        mock_finviz.assert_not_called()
        mock_yahoo.assert_not_called()

    def test_same_day_cache_with_missing_ttm_anchor_is_refetched(self):
        handler = make_handler()
        captured = {}
        today = server.datetime.date.today().isoformat()
        cached_payload = {
            "ticker": "MSFT",
            "companyName": "Microsoft Corporation",
            "shortFloat": "1.11%",
            "income": "143B",
            "adj_income": "143B",
            "capex": "83.1B",
            "da": "42.2B",
            "grossPpe": "323B",
            "adjEbitGrossPpe": "44.1%",
            "capexAdjIncome": "58.3%",
            "investmentCapex": "40.9B",
            "margin": "46.7%",
            "grossMargin": "68.5%",
            "ev_cy_ebit": "18.9",
            "ev_ny_ebit": "16.4",
            "ev": "2.89T",
            "ev_adj_ebit": "20.3",
            "roc": "52.2%",
            "cy_growth": "16.4%",
            "ny_growth": "15.4%",
            "marketCap": "2.86T",
            "netCash": "-31.2B",
            "derivedEnterpriseValue": "2.89T",
            "financialCurrency": "USD",
            "usdFxRate": 1.0,
            "valuationBasis": "enterpriseValue",
            "valuationPrefix": "EV",
            "valuationNumeratorLabel": "Current Enterprise Value",
            "currentYearEps": "16.7",
            "nextYearEps": "18.8",
            "yearAgoEps": "13.6",
            "currentYearEpsGrowth": "22.5%",
            "nextYearEpsGrowth": "12.7%",
            "priceCurrentEps": "28.3",
            "priceCyEps": "23",
            "priceNyEps": "20.4",
            "payloadVersion": server.PAYLOAD_VERSION,
            "evSource": "finviz",
            "marketCapSource": "yahoo",
            "incomeStatement": {
                "periods": ["TTM", "2025-06-30", "2024-06-30", "2023-06-30", "2022-06-30"],
                "rows": [
                    {"label": "Total Revenue", "values": ["--", "282B", "245B", "212B", "198B"]},
                    {"label": "Gross Profit", "values": ["--", "194B", "171B", "146B", "136B"]},
                    {"label": "Operating Income", "values": ["--", "129B", "109B", "88.5B", "83.4B"]},
                ],
            },
            "balanceStatement": {
                "periods": ["MRQ", "2025-06-30"],
                "rows": [{"label": "Total Assets", "values": ["619B", "619B"]}],
            },
            "cashFlowStatement": {
                "periods": ["TTM", "2025-06-30"],
                "rows": [{"label": "Operating Cash Flow", "values": ["136B", "119B"]}],
            },
        }

        fetch_payload = (
            "143B", "46.7%", "68.5%", "18.9", "16.4", "143B", "83.1B", "42.2B", "2.89T", "20.3",
            "16.4%", "15.4%", "43.4%", "146B", "209B", "3Y GP Growth", "40.1%", "153B", "177B", "2.86T", "31.2B", "2.89T", "305B", "46.7%", "0", "328B",
            "378B", "323B", "44.1%", "58.3%", "40.9B", "52.2%", "43.1B", "230B", "69.9B", "938M", "27.7B", "USD",
            1.0, "Microsoft Corporation",
            {
                "periods": ["TTM", "2025-06-30", "2024-06-30", "2023-06-30", "2022-06-30"],
                "rows": [
                    {"label": "Total Revenue", "values": ["305B", "282B", "245B", "212B", "198B"]},
                    {"label": "Gross Profit", "values": ["209B", "194B", "171B", "146B", "136B"]},
                    {"label": "Operating Income", "values": ["143B", "129B", "109B", "88.5B", "83.4B"]},
                ],
            },
            {"periods": ["MRQ", "2025-06-30"], "rows": [{"label": "Total Assets", "values": ["619B", "619B"]}]},
            {"periods": ["TTM", "2025-06-30"], "rows": [{"label": "Operating Cash Flow", "values": ["136B", "119B"]}]},
            "384", "585", "392", "730", "52.3%", "1.28", "strong_buy",
            {"period": "0m", "strongBuy": 10, "buy": 45, "hold": 3, "sell": 0, "strongSell": 0},
            "enterpriseValue", "EV", "Current Enterprise Value", "16.7", "18.8", "13.6", "22.5%", "12.7%", "28.3", "23", "20.4",
        )

        def fake_send_response(status, payload):
            captured["status"] = status
            captured["payload"] = payload

        with mock.patch("server.load_cache", return_value={
            "MSFT": {
                "date": today,
                "pulledAt": "2026-04-13T12:00:00",
                "data": cached_payload,
            }
        }), \
             mock.patch("server.save_cache"), \
             mock.patch.object(handler, "fetch_finviz_snapshot_metrics", return_value={
                 "short_float": "1.11%",
                 "market_cap": "2.86T",
                 "enterprise_value": "2.89T",
             }) as mock_finviz, \
             mock.patch.object(handler, "fetch_yahoo_finance_data", return_value=fetch_payload) as mock_yahoo, \
             mock.patch.object(handler, "_send_response", side_effect=fake_send_response):
            handler.handle_api_request("MSFT", refresh=False)

        self.assertEqual(captured["status"], 200)
        self.assertEqual(captured["payload"]["incomeStatement"]["rows"][0]["values"][0], "305B")
        mock_finviz.assert_called_once()
        mock_yahoo.assert_called_once()

    def test_payload_exposes_net_cash_and_derived_enterprise_value(self):
        helper = FetchYahooFinanceDataTests()
        helper.setUp()
        result = helper._run_fetch(finviz_ev_raw=240, finviz_market_cap_raw=180)

        self.assertEqual(result["market_cap"], "180")
        self.assertEqual(result["net_cash"], "-30")
        self.assertEqual(result["derived_enterprise_value"], "210")

    def test_payload_exposes_valuation_metadata(self):
        handler = make_handler()
        captured = {}

        def fake_send_response(status, payload):
            captured["status"] = status
            captured["payload"] = payload

        fetch_payload = (
            "83.3B", "41.4%", "82%", "14", "11.9", "83.3B", "69.7B", "18.6B", "1.46T", "17.5",
            "+25%", "+17.9%", "23.1%", "134B", "165B", "3Y GP Growth", "68.9%", "104B", "123B", "1.45T", "2.65B", "1.45T", "201B", "41.4%", "0", "251B",
            "296B", "254B", "32.8%", "83.7%", "51.1B", "40.1%", "10.9B", "197B", "19.8B", "--", "8.89B", "USD",
            1.0, "Meta Platforms, Inc.", fake_statement("Income"), fake_statement("Balance"), fake_statement("Cash"),
            "574", "860", "614", "1144", "+49.7%", "1.34", "strong_buy",
            {"period": "0m", "strongBuy": 11, "buy": 50, "hold": 6, "sell": 0, "strongSell": 0},
            "enterpriseValue", "EV", "Current Enterprise Value", "29.6", "34.38", "23.49", "26%", "16.1%", "24.5", "19.4", "16.7",
        )

        with mock.patch("server.load_cache", return_value={}), \
             mock.patch("server.save_cache"), \
             mock.patch.object(handler, "fetch_finviz_snapshot_metrics", return_value={
                 "short_float": "1.11%",
                 "market_cap": "1.45T",
                 "enterprise_value": "1.46T",
             }), \
             mock.patch.object(handler, "fetch_yahoo_finance_data", return_value=fetch_payload), \
             mock.patch.object(handler, "_send_response", side_effect=fake_send_response):
            handler.handle_api_request("META", refresh=True)

        self.assertEqual(captured["status"], 200)
        payload = captured["payload"]
        self.assertEqual(payload["valuationBasis"], "enterpriseValue")
        self.assertEqual(payload["valuationPrefix"], "EV")
        self.assertEqual(payload["valuationNumeratorLabel"], "Current Enterprise Value")
        self.assertEqual(payload["ev_adj_ebit"], "17.5")
        self.assertEqual(payload["marketCap"], "1.45T")
        self.assertEqual(payload["netCash"], "2.65B")
        self.assertEqual(payload["derivedEnterpriseValue"], "1.45T")
        self.assertEqual(payload["currentYearEps"], "29.6")
        self.assertEqual(payload["nextYearEps"], "34.38")
        self.assertEqual(payload["yearAgoEps"], "23.49")
        self.assertEqual(payload["currentYearEpsGrowth"], "26%")
        self.assertEqual(payload["nextYearEpsGrowth"], "16.1%")
        self.assertEqual(payload["priceCyEps"], "19.4")

    def test_finviz_404_still_returns_yahoo_payload(self):
        handler = make_handler()
        captured = {}

        def fake_send_response(status, payload):
            captured["status"] = status
            captured["payload"] = payload

        fetch_payload = (
            "83.3B", "41.4%", "82%", "14", "11.9", "83.3B", "69.7B", "18.6B", "--", "17.5",
            "25%", "17.9%", "23.1%", "134B", "165B", "3Y GP Growth", "68.9%", "104B", "123B", "1.45T", "2.65B", "1.45T", "201B", "41.4%", "0", "251B",
            "296B", "254B", "32.8%", "83.7%", "51.1B", "40.1%", "10.9B", "197B", "19.8B", "--", "8.89B", "USD",
            1.0, "Meta Platforms, Inc.", fake_statement("Income"), fake_statement("Balance"), fake_statement("Cash"),
            "574", "860", "614", "1144", "49.7%", "1.34", "strong_buy",
            {"period": "0m", "strongBuy": 11, "buy": 50, "hold": 6, "sell": 0, "strongSell": 0},
            "marketCap", "Mkt Cap", "Current Market Cap", "29.6", "34.38", "23.49", "26%", "16.1%", "24.5", "19.4", "16.7",
        )

        with mock.patch("server.load_cache", return_value={}), \
             mock.patch("server.save_cache"), \
             mock.patch.object(handler, "fetch_finviz_snapshot_metrics", side_effect=HTTPError(
                 "https://finviz.com/quote.ashx?t=META&p=d", 404, "Not Found", hdrs=None, fp=None
             )), \
             mock.patch.object(handler, "fetch_yahoo_finance_data", return_value=fetch_payload), \
             mock.patch.object(handler, "_send_response", side_effect=fake_send_response):
            handler.handle_api_request("META", refresh=True)

        self.assertEqual(captured["status"], 200)
        payload = captured["payload"]
        self.assertEqual(payload["shortFloat"], "--")
        self.assertEqual(payload["marketCapSource"], "yahoo")
        self.assertEqual(payload["evSource"], "unavailable")
        self.assertEqual(payload["valuationBasis"], "marketCap")
        self.assertEqual(payload["companyName"], "Meta Platforms, Inc.")


class StatementPageBuilderTests(unittest.TestCase):
    def setUp(self):
        self.handler = make_handler()

    def test_merge_statement_rows_prefers_primary_values_and_keeps_secondary_extras(self):
        primary = {
            "periods": ["TTM", "2025-06-30"],
            "rows": [
                {"label": "Total Revenue", "values": ["305B", "282B"]},
                {"label": "Operating Income", "values": ["143B", "129B"]},
            ],
        }
        secondary = {
            "periods": ["TTM", "2025-06-30"],
            "rows": [
                {"label": "Total Revenue", "values": ["282B", "282B"]},
                {"label": "Gross Profit", "values": ["194B", "194B"]},
                {"label": "Operating Income", "values": ["129B", "129B"]},
            ],
        }

        merged = self.handler._merge_statement_rows(primary, secondary)

        self.assertEqual(merged["periods"], ["TTM", "2025-06-30"])
        self.assertEqual(
            [row["label"] for row in merged["rows"]],
            ["Total Revenue", "Gross Profit", "Operating Income"],
        )
        self.assertEqual(merged["rows"][0]["values"], ["305B", "282B"])
        self.assertEqual(merged["rows"][1]["values"], ["194B", "194B"])
        self.assertEqual(merged["rows"][2]["values"], ["143B", "129B"])

    def test_merge_statement_rows_preserves_union_of_periods(self):
        primary = {
            "periods": ["TTM", "2025-06-30", "2024-06-30", "2023-06-30"],
            "rows": [
                {"label": "Total Revenue", "values": ["305B", "282B", "245B", "212B"]},
            ],
        }
        secondary = {
            "periods": ["TTM", "2025-06-30", "2024-06-30", "2023-06-30", "2022-06-30"],
            "rows": [
                {"label": "EBITDA", "values": ["160B", "133B", "105B", "100B", "95B"]},
            ],
        }

        merged = self.handler._merge_statement_rows(primary, secondary)
        ebitda_row = next(row for row in merged["rows"] if row["label"] == "EBITDA")

        self.assertEqual(merged["periods"], ["TTM", "2025-06-30", "2024-06-30", "2023-06-30", "2022-06-30"])
        self.assertEqual(ebitda_row["values"], ["160B", "133B", "105B", "100B", "95B"])

    def test_merge_statement_rows_backfills_missing_oldest_period_from_secondary(self):
        primary = {
            "periods": ["TTM", "2025-06-30", "2024-06-30", "2023-06-30"],
            "rows": [
                {"label": "Total Revenue", "values": ["305B", "282B", "245B", "212B"]},
                {"label": "Operating Income", "values": ["143B", "129B", "109B", "88.5B"]},
            ],
        }
        secondary = {
            "periods": ["TTM", "2025-06-30", "2024-06-30", "2023-06-30", "2022-06-30"],
            "rows": [
                {"label": "Total Revenue", "values": ["305B", "282B", "245B", "212B", "198B"]},
                {"label": "Operating Income", "values": ["143B", "129B", "109B", "88.5B", "83.4B"]},
            ],
        }

        merged = self.handler._merge_statement_rows(primary, secondary)
        revenue_row = next(row for row in merged["rows"] if row["label"] == "Total Revenue")
        income_row = next(row for row in merged["rows"] if row["label"] == "Operating Income")

        self.assertEqual(merged["periods"], ["TTM", "2025-06-30", "2024-06-30", "2023-06-30", "2022-06-30"])
        self.assertEqual(revenue_row["values"], ["305B", "282B", "245B", "212B", "198B"])
        self.assertEqual(income_row["values"], ["143B", "129B", "109B", "88.5B", "83.4B"])

    def test_income_statement_page_merges_quarterly_results_from_separate_scripts(self):
        annual_body = {
            "timeseries": {
                "result": [
                    {
                        "meta": {"type": ["annualTotalRevenue"]},
                        "annualTotalRevenue": [
                            {"asOfDate": "2025-06-30", "reportedValue": {"raw": 282000000000}},
                            {"asOfDate": "2024-06-30", "reportedValue": {"raw": 245000000000}},
                        ],
                    },
                    {
                        "meta": {"type": ["annualOperatingIncome"]},
                        "annualOperatingIncome": [
                            {"asOfDate": "2025-06-30", "reportedValue": {"raw": 129000000000}},
                            {"asOfDate": "2024-06-30", "reportedValue": {"raw": 109000000000}},
                        ],
                    },
                ]
            }
        }
        quarterly_body = {
            "timeseries": {
                "result": [
                    {
                        "meta": {"type": ["quarterlyOperatingIncome"]},
                        "quarterlyOperatingIncome": [
                            {"asOfDate": "2026-03-31", "reportedValue": {"raw": 36000000000}},
                            {"asOfDate": "2025-12-31", "reportedValue": {"raw": 34000000000}},
                            {"asOfDate": "2025-09-30", "reportedValue": {"raw": 36000000000}},
                            {"asOfDate": "2025-06-30", "reportedValue": {"raw": 36559000000}},
                        ],
                    }
                ]
            }
        }

        def script_tag(data_url, body):
            outer = {"body": json.dumps(body)}
            return (
                f'<script type="application/json" data-sveltekit-fetched '
                f'data-url="{data_url}">{json.dumps(outer)}</script>'
            )

        html = "".join([
            script_tag(
                "https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/MSFT?type=annualTotalRevenue,annualOperatingIncome",
                annual_body,
            ),
            script_tag(
                "https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/MSFT?type=quarterlyOperatingIncome",
                quarterly_body,
            ),
        ])

        statement = self.handler.build_income_statement_from_page(
            "MSFT",
            FakePageOpener(html),
            lambda value: value,
            lambda value: str(int(value)),
        )

        self.assertEqual(statement["periods"][0], "TTM")
        operating_income_row = next(row for row in statement["rows"] if row["label"] == "Operating Income")
        self.assertEqual(operating_income_row["values"][0], "142559000000")
        self.assertEqual(operating_income_row["values"][1], "129000000000")

    def test_income_statement_ttm_falls_back_to_annual_when_quarters_are_partial(self):
        selected_results = [
            {
                "meta": {"type": ["annualTotalRevenue"]},
                "annualTotalRevenue": [
                    {"asOfDate": "2025-12-31", "reportedValue": {"raw": 751766000000}},
                    {"asOfDate": "2024-12-31", "reportedValue": {"raw": 660257000000}},
                ],
            },
            {
                "meta": {"type": ["quarterlyTotalRevenue"]},
                "quarterlyTotalRevenue": [
                    {"asOfDate": "2025-12-31", "reportedValue": {"raw": 195400000000}},
                ],
            },
        ]

        statement = self.handler.build_income_statement_from_timeseries_results(
            selected_results,
            lambda value: value,
            lambda value: str(int(value)),
        )

        revenue_row = next(row for row in statement["rows"] if row["label"] == "Total Revenue")
        self.assertEqual(statement["periods"][:2], ["TTM", "2025-12-31"])
        self.assertEqual(revenue_row["values"][:2], ["751766000000", "751766000000"])

    def test_income_statement_timeseries_ignores_balance_sheet_and_cash_flow_rows(self):
        selected_results = [
            {
                "meta": {"type": ["annualTotalRevenue"]},
                "annualTotalRevenue": [
                    {"asOfDate": "2025-12-31", "reportedValue": {"raw": 100}},
                ],
            },
            {
                "meta": {"type": ["annualAccountsPayable"]},
                "annualAccountsPayable": [
                    {"asOfDate": "2025-12-31", "reportedValue": {"raw": 30}},
                ],
            },
            {
                "meta": {"type": ["annualGrossPPE"]},
                "annualGrossPPE": [
                    {"asOfDate": "2025-12-31", "reportedValue": {"raw": 40}},
                ],
            },
            {
                "meta": {"type": ["annualCapitalExpenditure"]},
                "annualCapitalExpenditure": [
                    {"asOfDate": "2025-12-31", "reportedValue": {"raw": -10}},
                ],
            },
        ]

        statement = self.handler.build_income_statement_from_timeseries_results(
            selected_results,
            lambda value: value,
            lambda value: str(int(value)),
        )

        labels = [row["label"] for row in statement["rows"]]
        self.assertEqual(labels, ["Total Revenue"])
        self.assertNotIn("Accounts Payable", labels)
        self.assertNotIn("Gross PP&E", labels)
        self.assertNotIn("Capital Expenditures", labels)


if __name__ == "__main__":
    unittest.main()
