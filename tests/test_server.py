import json
import os
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

import server
import datetime


FETCH_RESULT_FIELDS = server.FETCH_RESULT_FIELDS
server.HAS_YFINANCE = False


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
    s = {"periods": ["TTM"], "rows": [{"label": label, "values": ["1"]}]}
    return {"annual": s, "quarterly": s}


def fake_balance_statement():
    s = {
        "periods": ["MRQ"],
        "rows": [
            {"label": "Current Debt", "values": ["10"]},
            {"label": "Long Term Debt", "values": ["40"]},
            {"label": "Cash And Cash Equivalents", "values": ["15"]},
            {"label": "Other Short Term Investments", "values": ["5"]},
        ],
    }
    return {"annual": s, "quarterly": s}


def make_fetch_result(**overrides):
    values = {
        "income": "143B",
        "margin": "46.7%",
        "gross_margin": "68.6%",
        "ev_cy_ebit": "20.4",
        "ev_ny_ebit": "17.7",
        "adj_income": "143B",
        "capex": "83.1B",
        "da": "39.1B",
        "ev": "3.12T",
        "ev_adj_ebit": "21.9",
        "cy_growth": "16.4%",
        "ny_growth": "15.6%",
        "gp_3y_growth": "12.7%",
        "gp_3y_start": "136B",
        "gp_3y_end": "194B",
        "gp_3y_label": "3Y Annual GP Growth",
        "rnd_adj_income": "23.6%",
        "cy_adj_inc": "153B",
        "ny_adj_inc": "177B",
        "market_cap": "3.16T",
        "net_cash": "34B",
        "derived_enterprise_value": "3.12T",
        "revenue": "305B",
        "operating_margin": "46.7%",
        "da_minus_capex": "0",
        "cy_revenue": "328B",
        "ny_revenue": "379B",
        "gross_ppe": "323B",
        "adj_ebit_gross_ppe": "44.1%",
        "capex_adj_income": "30.8%",
        "investment_capex": "44B",
        "roc": "52.2%",
        "net_working_capital": "43.1B",
        "net_fixed_assets": "230B",
        "receivables": "69.9B",
        "inventory": "938M",
        "accounts_payable": "27.7B",
        "financial_currency": "USD",
        "usd_fx_rate": 1.0,
        "company_name": "Microsoft Corporation",
        "income_statement": fake_statement("Income"),
        "balance_statement": fake_statement("Balance"),
        "cash_flow_statement": fake_statement("Cash"),
        "current_price": "425",
        "target_mean_price": "573",
        "target_low_price": "392",
        "target_high_price": "730",
        "target_move": "34.8%",
        "recommendation_mean": "1.29",
        "recommendation_key": "strong_buy",
        "analyst_recommendations": {"strongBuy": 12, "buy": 45, "hold": 1, "sell": 0, "strongSell": 0},
        "valuation_basis": "derivedEV",
        "valuation_prefix": "EV",
        "valuation_numerator_label": "Derived Enterprise Value",
        "current_year_eps": "16.7",
        "next_year_eps": "19",
        "year_ago_eps": "13.6",
        "current_year_eps_growth": "22.3%",
        "next_year_eps_growth": "13.7%",
        "price_current_eps": "31.1",
        "price_cy_eps": "25.5",
        "price_ny_eps": "22.4",
        "short_float": "1.12%",
    }
    values.update(overrides)
    return tuple(values[key] for key in FETCH_RESULT_FIELDS)


class CacheDatabaseTests(unittest.TestCase):
    def test_cache_round_trips_through_sqlite(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "cache.db")
            legacy_path = os.path.join(tmpdir, "cache.json")
            cache_payload = {
                "MSFT": {
                    "date": "2026-04-20",
                    "pulledAt": "2026-04-20T13:00:00",
                    "data": {
                        "ticker": "MSFT",
                        "companyName": "Microsoft Corporation",
                        "payloadVersion": server.PAYLOAD_VERSION,
                    },
                }
            }

            with mock.patch.object(server, "CACHE_DB_FILE", db_path), \
                 mock.patch.object(server, "LEGACY_CACHE_FILE", legacy_path):
                server.save_cache(cache_payload)

                self.assertTrue(os.path.exists(db_path))
                self.assertEqual(server.load_cache(), cache_payload)

                with sqlite3.connect(db_path) as conn:
                    row = conn.execute(
                        """
                        SELECT ticker, data_date, pulled_at, payload_version, payload_json
                        FROM ticker_cache
                        """
                    ).fetchone()

            self.assertEqual(row[0], "MSFT")
            self.assertEqual(row[1], "2026-04-20")
            self.assertEqual(row[2], "2026-04-20T13:00:00")
            self.assertEqual(row[3], server.PAYLOAD_VERSION)
            self.assertEqual(json.loads(row[4])["companyName"], "Microsoft Corporation")

    def test_legacy_json_cache_is_imported_when_database_is_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "cache.db")
            legacy_path = os.path.join(tmpdir, "cache.json")
            legacy_payload = {
                "TEST": {
                    "date": "2026-04-20",
                    "pulledAt": "2026-04-20T14:00:00",
                    "data": {"ticker": "TEST", "payloadVersion": server.PAYLOAD_VERSION},
                }
            }
            with open(legacy_path, "w") as f:
                json.dump(legacy_payload, f)

            with mock.patch.object(server, "CACHE_DB_FILE", db_path), \
                 mock.patch.object(server, "LEGACY_CACHE_FILE", legacy_path):
                self.assertEqual(server.load_cache(), legacy_payload)

                with sqlite3.connect(db_path) as conn:
                    count = conn.execute("SELECT COUNT(*) FROM ticker_cache").fetchone()[0]

            self.assertEqual(count, 1)


def fake_income_statement_with_eps(ttm_value, annual_value):
    s = {
        "periods": ["TTM", "2025-12-31"],
        "rows": [
            {"label": "Diluted EPS", "values": [ttm_value, annual_value]},
        ],
    }
    return {"annual": s, "quarterly": s}


class FetchYahooFinanceDataTests(unittest.TestCase):
    def setUp(self):
        self.handler = make_handler()

    def test_market_cap_prefers_yfinance_market_cap_over_share_class_math(self):
        info = {
            "marketCap": 4238063173632,
            "sharesOutstanding": 5822000000,
            "impliedSharesOutstanding": 12097000000,
            "currentPrice": 350.34,
        }

        self.assertEqual(self.handler._market_cap_from_info(info), 4238063173632)

    def test_market_cap_fallback_uses_implied_shares_when_api_market_cap_missing(self):
        info = {
            "sharesOutstanding": 5822000000,
            "impliedSharesOutstanding": 12097000000,
            "currentPrice": 350.34,
        }

        self.assertEqual(round(self.handler._market_cap_from_info(info)), 4238062980000)

    def test_delegates_to_yfinance_without_manual_fetches(self):
        expected = tuple(f"value-{idx}" for idx, _field in enumerate(FETCH_RESULT_FIELDS))

        with mock.patch.object(server, "HAS_YFINANCE", True), \
             mock.patch.object(self.handler, "fetch_yfinance_data", return_value=expected) as mock_yfinance:
            result = self.handler.fetch_yahoo_finance_data(
                "ACME",
                finviz_ev_raw=240,
                finviz_market_cap_raw=180,
                finviz_metrics={"eps_this_y": "99%"},
            )

        self.assertEqual(result, expected)
        mock_yfinance.assert_called_once_with(
            "ACME",
            finviz_ev_raw=0,
            finviz_market_cap_raw=0,
            finviz_metrics={},
        )
        self.assertFalse(hasattr(self.handler, "_counted_open"))

    def test_yfinance_failure_keeps_full_tuple_shape(self):
        with mock.patch.object(server, "HAS_YFINANCE", True), \
             mock.patch.object(self.handler, "fetch_yfinance_data", side_effect=RuntimeError("boom")), \
             mock.patch("builtins.print"):
            result = self.handler.fetch_yahoo_finance_data("FAIL", 0, 0)

        self.assertEqual(len(result), len(FETCH_RESULT_FIELDS))
        mapped = dict(zip(FETCH_RESULT_FIELDS, result))
        self.assertEqual(mapped["valuation_basis"], "unavailable")
        self.assertEqual(mapped["valuation_prefix"], "EV")
        self.assertEqual(mapped["company_name"], "FAIL")

    def test_missing_yfinance_keeps_full_tuple_shape(self):
        with mock.patch.object(server, "HAS_YFINANCE", False), \
             mock.patch("builtins.print"):
            result = self.handler.fetch_yahoo_finance_data("FAIL", 0, 0)

        self.assertEqual(len(result), len(FETCH_RESULT_FIELDS))
        mapped = dict(zip(FETCH_RESULT_FIELDS, result))
        self.assertEqual(mapped["valuation_basis"], "unavailable")
        self.assertEqual(mapped["valuation_prefix"], "EV")
        self.assertEqual(mapped["company_name"], "FAIL")

class HandleApiRequestContractTests(unittest.TestCase):
    REQUIRED_PUBLIC_PAYLOAD_KEYS = {
        "ticker",
        "shortFloat",
        "income",
        "margin",
        "grossMargin",
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
        "rndAdjIncome",
        "cy_adj_inc",
        "ny_adj_inc",
        "marketCap",
        "netCash",
        "derivedEnterpriseValue",
        "revenue",
        "operating_margin",
        "da_minus_capex",
        "cy_revenue",
        "ny_revenue",
        "grossPpe",
        "adjEbitGrossPpe",
        "capexAdjIncome",
        "investmentCapex",
        "roc",
        "netWorkingCapital",
        "netFixedAssets",
        "receivables",
        "inventory",
        "accountsPayable",
        "financialCurrency",
        "usdFxRate",
        "companyName",
        "incomeStatement",
        "balanceStatement",
        "cashFlowStatement",
        "currentPrice",
        "targetMeanPrice",
        "targetLowPrice",
        "targetHighPrice",
        "targetMove",
        "recommendationMean",
        "recommendationKey",
        "analystRecommendations",
        "valuationBasis",
        "valuationPrefix",
        "valuationNumeratorLabel",
        "currentYearEps",
        "nextYearEps",
        "yearAgoEps",
        "currentYearEpsGrowth",
        "nextYearEpsGrowth",
        "priceCurrentEps",
        "priceCyEps",
        "priceNyEps",
        "payloadVersion",
        "evSource",
        "marketCapSource",
        "dataDate",
        "pulledAt",
        "fetchCount",
    }

    def test_test_ticker_returns_complete_fixture_without_external_fetches(self):
        handler = make_handler()
        captured = {}

        def fake_send_response(status, payload):
            captured["status"] = status
            captured["payload"] = payload

        with mock.patch.object(handler, "fetch_yahoo_finance_data") as mock_yahoo, \
             mock.patch.object(handler, "_send_response", side_effect=fake_send_response):
            handler.handle_api_request("TEST", refresh=True)

        self.assertEqual(captured["status"], 200)
        payload = captured["payload"]
        self.assertEqual(payload["ticker"], "TEST")
        self.assertEqual(payload["companyName"], "Test Fixture Corporation")
        self.assertEqual(payload["marketCap"], "500B")
        self.assertEqual(payload["netCash"], "20B")
        self.assertEqual(payload["derivedEnterpriseValue"], "480B")
        self.assertEqual(payload["financialCurrency"], "CNY")
        self.assertEqual(payload["usdFxRate"], 0.138)
        self.assertEqual(payload["grossMargin"], "60%")
        self.assertEqual(payload["income"], "30B")
        self.assertEqual(payload["da"], "5B")
        self.assertEqual(payload["capex"], "7B")
        self.assertEqual(payload["da_minus_capex"], "0")
        self.assertEqual(payload["adj_income"], "30B")
        self.assertEqual(payload["margin"], "30%")
        self.assertEqual(payload["investmentCapex"], "2B")
        self.assertEqual(payload["capexAdjIncome"], "6.67%")
        self.assertEqual(payload["ev_adj_ebit"], "16")
        self.assertEqual(payload["ev_cy_ebit"], "14.5")
        self.assertEqual(payload["ev_ny_ebit"], "13")
        self.assertEqual(payload["priceCyEps"], "10")
        self.assertEqual(payload["incomeStatement"]["annual"]["rows"][0]["label"], "Total Revenue")
        self.assertEqual(payload["balanceStatement"]["annual"]["rows"][2]["label"], "Cash, Equivalents & Short Term Investments")
        self.assertEqual(payload["cashFlowStatement"]["annual"]["rows"][1]["label"], "Capital Expenditures")
        mock_yahoo.assert_not_called()

    def test_test_ticker_fixture_has_frontend_contract_keys(self):
        handler = make_handler()
        captured = {}

        def fake_send_response(status, payload):
            captured["status"] = status
            captured["payload"] = payload

        with mock.patch.object(handler, "_send_response", side_effect=fake_send_response):
            handler.handle_api_request("TEST", refresh=True)

        self.assertEqual(captured["status"], 200)
        self.assertTrue(self.REQUIRED_PUBLIC_PAYLOAD_KEYS.issubset(captured["payload"].keys()))

    def test_fetch_result_maps_to_public_payload_and_saved_cache(self):
        handler = make_handler()
        captured = {}
        saved = {}

        def fake_send_response(status, payload):
            captured["status"] = status
            captured["payload"] = payload

        def fake_save_cache(cache):
            saved.update(cache)

        with mock.patch("server.load_cache", return_value={}), \
             mock.patch("server.save_cache", side_effect=fake_save_cache), \
             mock.patch.object(handler, "fetch_yahoo_finance_data", return_value=make_fetch_result()), \
             mock.patch.object(handler, "_send_response", side_effect=fake_send_response):
            handler.handle_api_request("MSFT", refresh=True)

        payload = captured["payload"]
        self.assertEqual(captured["status"], 200)
        self.assertTrue(self.REQUIRED_PUBLIC_PAYLOAD_KEYS.issubset(payload.keys()))
        self.assertEqual(payload["ticker"], "MSFT")
        self.assertEqual(payload["grossMargin"], "68.6%")
        self.assertEqual(payload["rndAdjIncome"], "23.6%")
        self.assertEqual(payload["marketCap"], "3.16T")
        self.assertEqual(payload["netCash"], "34B")
        self.assertEqual(payload["derivedEnterpriseValue"], "3.12T")
        self.assertEqual(payload["grossPpe"], "323B")
        self.assertEqual(payload["netWorkingCapital"], "43.1B")
        self.assertEqual(payload["companyName"], "Microsoft Corporation")
        self.assertEqual(payload["valuationBasis"], "derivedEV")
        self.assertEqual(payload["valuationPrefix"], "EV")
        self.assertEqual(payload["valuationNumeratorLabel"], "Derived Enterprise Value")
        self.assertEqual(payload["evSource"], "derived")
        self.assertEqual(payload["marketCapSource"], "yahoo")
        self.assertEqual(payload["payloadVersion"], server.PAYLOAD_VERSION)
        self.assertEqual(saved["MSFT"]["data"], payload)

    def test_same_day_cache_with_old_payload_version_is_refetched(self):
        handler = make_handler()
        captured = {}
        today = server.datetime.date.today().isoformat()
        cached_payload = {
            "ticker": "MSFT",
            "companyName": "Old Microsoft",
            "marketCap": "1T",
            "payloadVersion": server.PAYLOAD_VERSION - 1,
            "incomeStatement": fake_statement("Income"),
            "balanceStatement": fake_statement("Balance"),
            "cashFlowStatement": fake_statement("Cash"),
        }

        def fake_send_response(status, payload):
            captured["status"] = status
            captured["payload"] = payload

        with mock.patch("server.load_cache", return_value={
            "MSFT": {"date": today, "pulledAt": "2026-04-20T10:00:00", "data": cached_payload}
        }), \
             mock.patch("server.save_cache"), \
             mock.patch.object(handler, "fetch_yahoo_finance_data", return_value=make_fetch_result()) as mock_fetch, \
             mock.patch.object(handler, "_send_response", side_effect=fake_send_response):
            handler.handle_api_request("MSFT", refresh=False)

        self.assertEqual(captured["status"], 200)
        self.assertEqual(captured["payload"]["companyName"], "Microsoft Corporation")
        self.assertEqual(captured["payload"]["payloadVersion"], server.PAYLOAD_VERSION)
        mock_fetch.assert_called_once()

    def test_refresh_failure_preserves_existing_cached_payload(self):
        handler = make_handler()
        captured = {}
        cached_payload = {
            "ticker": "MSFT",
            "companyName": "Microsoft Corporation",
            "marketCap": "3.14T",
            "payloadVersion": server.PAYLOAD_VERSION,
            "incomeStatement": {
                "periods": ["TTM"],
                "rows": [
                    {"label": "Total Revenue", "values": ["305B"]},
                    {"label": "Gross Profit", "values": ["209B"]},
                    {"label": "Operating Income", "values": ["143B"]},
                ],
            },
            "balanceStatement": fake_statement("Balance"),
            "cashFlowStatement": fake_statement("Cash"),
        }
        cached_entry = {
            "date": "2026-04-20",
            "pulledAt": "2026-04-20T10:00:00",
            "data": cached_payload,
        }

        def fake_send_response(status, payload):
            captured["status"] = status
            captured["payload"] = payload

        with mock.patch("server.load_cache", return_value={"MSFT": cached_entry}), \
             mock.patch("server.save_cache") as mock_save, \
             mock.patch.object(handler, "fetch_yahoo_finance_data", return_value=handler._empty_fetch_tuple("MSFT")), \
             mock.patch.object(handler, "_send_response", side_effect=fake_send_response):
            handler.handle_api_request("MSFT", refresh=True)

        self.assertEqual(captured["status"], 200)
        self.assertEqual(captured["payload"]["ticker"], "MSFT")
        self.assertEqual(captured["payload"]["marketCap"], "3.14T")
        self.assertEqual(captured["payload"]["dataDate"], "2026-04-20")
        self.assertEqual(captured["payload"]["pulledAt"], "2026-04-20T10:00:00")
        self.assertTrue(captured["payload"]["staleDueToRefreshError"])
        self.assertIn("refreshError", captured["payload"])
        mock_save.assert_not_called()

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
             mock.patch.object(handler, "fetch_yahoo_finance_data") as mock_yahoo, \
             mock.patch.object(handler, "_send_response", side_effect=fake_send_response):
            handler.handle_api_request("META", refresh=False)

        self.assertEqual(captured["status"], 200)
        self.assertEqual(captured["payload"]["ticker"], "META")
        self.assertEqual(captured["payload"]["fetchCount"], 0)
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
                "annual": {
                    "periods": ["TTM", "2025-06-30", "2024-06-30", "2023-06-30", "2022-06-30"],
                    "rows": [
                        {"label": "Total Revenue", "values": ["--", "282B", "245B", "212B", "198B"]},
                        {"label": "Gross Profit", "values": ["--", "194B", "171B", "146B", "136B"]},
                        {"label": "Operating Income", "values": ["--", "129B", "109B", "88.5B", "83.4B"]},
                    ],
                },
                "quarterly": {"periods": [], "rows": []}
            },
            "balanceStatement": {
                "annual": {
                    "periods": ["MRQ", "2025-06-30"],
                    "rows": [{"label": "Total Assets", "values": ["619B", "619B"]}],
                },
                "quarterly": {"periods": [], "rows": []}
            },
            "cashFlowStatement": {
                "annual": {
                    "periods": ["TTM", "2025-06-30"],
                    "rows": [{"label": "Operating Cash Flow", "values": ["136B", "119B"]}],
                },
                "quarterly": {"periods": [], "rows": []}
            },
        }

        fetch_payload = (
            "143B", "46.7%", "68.5%", "18.9", "16.4", "143B", "83.1B", "42.2B", "2.89T", "20.3",
            "16.4%", "15.4%", "43.4%", "146B", "209B", "3Y Annual GP Growth", "40.1%", "153B", "177B", "2.86T", "31.2B", "2.89T", "305B", "46.7%", "0", "328B",
            "378B", "323B", "44.1%", "58.3%", "40.9B", "52.2%", "43.1B", "230B", "69.9B", "938M", "27.7B", "USD",
            1.0, "Microsoft Corporation",
            {
                "annual": {
                    "periods": ["TTM", "2025-06-30", "2024-06-30", "2023-06-30", "2022-06-30"],
                    "rows": [
                        {"label": "Total Revenue", "values": ["305B", "282B", "245B", "212B", "198B"]},
                        {"label": "Gross Profit", "values": ["209B", "194B", "171B", "146B", "136B"]},
                        {"label": "Operating Income", "values": ["143B", "129B", "109B", "88.5B", "83.4B"]},
                    ],
                },
                "quarterly": {"periods": [], "rows": []}
            },
            {"annual": {"periods": ["MRQ", "2025-06-30"], "rows": [{"label": "Total Assets", "values": ["619B", "619B"]}]}, "quarterly": {"periods": [], "rows": []}},
            {"annual": {"periods": ["TTM", "2025-06-30"], "rows": [{"label": "Operating Cash Flow", "values": ["136B", "119B"]}]}, "quarterly": {"periods": [], "rows": []}},
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
             mock.patch.object(handler, "fetch_yahoo_finance_data", return_value=fetch_payload) as mock_yahoo, \
             mock.patch.object(handler, "_send_response", side_effect=fake_send_response):
            handler.handle_api_request("MSFT", refresh=False)

        self.assertEqual(captured["status"], 200)
        self.assertEqual(captured["payload"]["incomeStatement"]["annual"]["rows"][0]["values"][0], "305B")
        mock_yahoo.assert_called_once()

    def test_payload_exposes_valuation_metadata(self):
        handler = make_handler()
        captured = {}

        def fake_send_response(status, payload):
            captured["status"] = status
            captured["payload"] = payload

        fetch_payload = (
            "83.3B", "41.4%", "82%", "14", "11.9", "83.3B", "69.7B", "18.6B", "1.46T", "17.5",
            "+25%", "+17.9%", "23.1%", "134B", "165B", "3Y Annual GP Growth", "68.9%", "104B", "123B", "1.45T", "2.65B", "1.45T", "201B", "41.4%", "0", "251B",
            "296B", "254B", "32.8%", "83.7%", "51.1B", "40.1%", "10.9B", "197B", "19.8B", "--", "8.89B", "USD",
            1.0, "Meta Platforms, Inc.", fake_statement("Income"), fake_statement("Balance"), fake_statement("Cash"),
            "574", "860", "614", "1144", "+49.7%", "1.34", "strong_buy",
            {"period": "0m", "strongBuy": 11, "buy": 50, "hold": 6, "sell": 0, "strongSell": 0},
            "enterpriseValue", "EV", "Current Enterprise Value", "29.6", "34.38", "23.49", "26%", "16.1%", "24.5", "19.4", "16.7",
        )

        with mock.patch("server.load_cache", return_value={}), \
             mock.patch("server.save_cache"), \
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

    def test_unavailable_ev_still_returns_yfinance_payload(self):
        handler = make_handler()
        captured = {}

        def fake_send_response(status, payload):
            captured["status"] = status
            captured["payload"] = payload

        fetch_payload = (
            "83.3B", "41.4%", "82%", "14", "11.9", "83.3B", "69.7B", "18.6B", "--", "17.5",
            "25%", "17.9%", "23.1%", "134B", "165B", "3Y Annual GP Growth", "68.9%", "104B", "123B", "1.45T", "2.65B", "1.45T", "201B", "41.4%", "0", "251B",
            "296B", "254B", "32.8%", "83.7%", "51.1B", "40.1%", "10.9B", "197B", "19.8B", "--", "8.89B", "USD",
            1.0, "Meta Platforms, Inc.", fake_statement("Income"), fake_statement("Balance"), fake_statement("Cash"),
            "574", "860", "614", "1144", "49.7%", "1.34", "strong_buy",
            {"period": "0m", "strongBuy": 11, "buy": 50, "hold": 6, "sell": 0, "strongSell": 0},
            "marketCap", "Mkt Cap", "Current Market Cap", "29.6", "34.38", "23.49", "26%", "16.1%", "24.5", "19.4", "16.7",
        )

        with mock.patch("server.load_cache", return_value={}), \
             mock.patch("server.save_cache"), \
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
            ["Total Revenue", "Operating Income", "Gross Profit"],
        )
        self.assertEqual(merged["rows"][0]["values"], ["305B", "282B"])
        self.assertEqual(merged["rows"][1]["values"], ["143B", "129B"])
        self.assertEqual(merged["rows"][2]["values"], ["194B", "194B"])

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

    def test_df_statement_prefers_official_annual_ttm_over_quarter_sum(self):
        import pandas as pd

        annual = pd.DataFrame(
            {
                "TTM": [1100],
                pd.Timestamp("2025-12-31"): [900],
                pd.Timestamp("2024-12-31"): [800],
            },
            index=["Total Revenue"],
        )
        quarterly = pd.DataFrame(
            {
                pd.Timestamp("2025-12-31"): [250],
                pd.Timestamp("2025-09-30"): [250],
                pd.Timestamp("2025-06-30"): [250],
                pd.Timestamp("2025-03-31"): [250],
            },
            index=["Total Revenue"],
        )

        statement = self.handler._df_to_statement(
            annual,
            formatter=lambda value: str(int(value)),
            quarterly_df=quarterly,
        )

        revenue_row = statement["rows"][0]
        self.assertEqual(statement["periods"], ["TTM", "2025-12-31", "2024-12-31"])
        self.assertEqual(revenue_row["values"], ["1100", "900", "800"])

    def test_df_ttm_value_prefers_official_annual_ttm_over_quarter_sum(self):
        import pandas as pd

        annual = pd.DataFrame(
            {"TTM": [1100], pd.Timestamp("2025-12-31"): [900]},
            index=["Total Revenue"],
        )
        quarterly = pd.DataFrame(
            {
                pd.Timestamp("2025-12-31"): [250],
                pd.Timestamp("2025-09-30"): [250],
                pd.Timestamp("2025-06-30"): [250],
                pd.Timestamp("2025-03-31"): [250],
            },
            index=["Total Revenue"],
        )

        value = self.handler._df_ttm_value(quarterly, annual, ["Total Revenue"])

        self.assertEqual(value, 1100)

    def test_df_with_ttm_column_adds_yfinance_ttm_frame_to_annuals(self):
        import pandas as pd

        annual = pd.DataFrame(
            {pd.Timestamp("2025-06-30"): [7462000000]},
            index=["Diluted Average Shares"],
        )
        ttm = pd.DataFrame(
            {pd.Timestamp("2025-12-31"): [7462000000]},
            index=["Diluted Average Shares"],
        )

        merged = self.handler._df_with_ttm_column(annual, ttm)

        self.assertIn("TTM", merged.columns)
        self.assertEqual(merged.loc["Diluted Average Shares", "TTM"], 7462000000)
        self.assertEqual(merged.loc["Diluted Average Shares", pd.Timestamp("2025-06-30")], 7462000000)

    def test_df_history_columns_excludes_ttm_column_before_sorting(self):
        import pandas as pd

        frame = pd.DataFrame(
            {
                pd.Timestamp("2025-06-30"): [100],
                "TTM": [120],
                pd.Timestamp("2024-06-30"): [90],
            },
            index=["Total Revenue"],
        )

        self.assertEqual(
            self.handler._df_history_columns(frame),
            [pd.Timestamp("2025-06-30"), pd.Timestamp("2024-06-30")],
        )

    def test_df_ttm_value_does_not_sum_share_count_rows(self):
        import pandas as pd

        annual = pd.DataFrame(
            {pd.Timestamp("2025-06-30"): [7462000000]},
            index=["Diluted Average Shares"],
        )
        quarterly = pd.DataFrame(
            {
                pd.Timestamp("2025-06-30"): [7462000000],
                pd.Timestamp("2025-03-31"): [7470000000],
                pd.Timestamp("2024-12-31"): [7470000000],
                pd.Timestamp("2024-09-30"): [7470000000],
            },
            index=["Diluted Average Shares"],
        )

        value = self.handler._df_ttm_value(quarterly, annual, ["Diluted Average Shares"])

        self.assertEqual(value, 7462000000)

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

        revenue_row = next(row for row in statement["annual"]["rows"] if row["label"] == "Total Revenue")
        self.assertEqual(statement["annual"]["periods"][:2], ["TTM", "2025-12-31"])
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

        labels = [row["label"] for row in statement["annual"]["rows"]]
        self.assertEqual(labels, ["Total Revenue"])
        self.assertNotIn("Accounts Payable", labels)
        self.assertNotIn("Gross PP&E", labels)
        self.assertNotIn("Capital Expenditures", labels)

    def test_build_statement_separates_annual_and_quarterly_correctly(self):
        selected_results = [
            {
                "meta": {"type": ["annualTotalRevenue"]},
                "annualTotalRevenue": [{"asOfDate": "2025-12-31", "reportedValue": {"raw": 1000}}],
            },
            {
                "meta": {"type": ["quarterlyTotalRevenue"]},
                "quarterlyTotalRevenue": [
                    {"asOfDate": "2025-12-31", "reportedValue": {"raw": 300}},
                    {"asOfDate": "2025-09-30", "reportedValue": {"raw": 250}},
                    {"asOfDate": "2025-06-30", "reportedValue": {"raw": 220}},
                    {"asOfDate": "2025-03-31", "reportedValue": {"raw": 230}},
                    {"asOfDate": "2024-12-31", "reportedValue": {"raw": 280}},
                ],
            },
            # Test deduplication: secondary key for same label
            {
                "meta": {"type": ["quarterlyRevenue"]},
                "quarterlyRevenue": [{"asOfDate": "2025-12-31", "reportedValue": {"raw": 300}}],
            }
        ]

        statement = self.handler.build_income_statement_from_timeseries_results(
            selected_results,
            lambda value: value,
            lambda value: str(int(value)),
        )

        # Check Annual
        self.assertEqual(statement["annual"]["periods"], ["TTM", "2025-12-31"])
        self.assertEqual(statement["annual"]["rows"][0]["values"], ["1000", "1000"])

        # Check Quarterly
        q_stmt = statement["quarterly"]
        self.assertEqual(q_stmt["periods"], ["2025-12-31", "2025-09-30", "2025-06-30", "2025-03-31", "2024-12-31"])
        
        revenue_row = next(row for row in q_stmt["rows"] if row["label"] == "Total Revenue")
        self.assertEqual(revenue_row["values"], ["300", "250", "220", "230", "280"])

        # Verify only one Total Revenue row (deduplication check)
        labels = [row["label"] for row in q_stmt["rows"]]
        self.assertEqual(labels.count("Total Revenue"), 1)

if __name__ == "__main__":
    unittest.main()
