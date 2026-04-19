import http.server
import socketserver
import urllib.request
import re
import json
import os
import datetime
from urllib.parse import urlparse, parse_qs
from urllib.error import URLError, HTTPError

PORT = int(os.environ.get("PORT", "3000"))
CACHE_FILE = 'cache.json'
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "900"))

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

    def fetch_yahoo_finance_data(self, ticker, finviz_ev_raw=0, finviz_market_cap_raw=0):
        try:
            from http.cookiejar import CookieJar
            cj = CookieJar()
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
            opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
            urllib.request.install_opener(opener)
            
            try:
                urllib.request.urlopen('https://fc.yahoo.com/', timeout=3)
            except:
                pass
                
            crumb = urllib.request.urlopen('https://query1.finance.yahoo.com/v1/test/getcrumb', timeout=3).read().decode('utf-8')
            url = f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=financialData,earningsTrend,defaultKeyStatistics,price&crumb={crumb}'
            data = json.loads(urllib.request.urlopen(url, timeout=3).read().decode('utf-8'))
            
            if not data.get('quoteSummary', {}).get('result'):
                return "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "Gross Profit"
            
            res = data['quoteSummary']['result'][0]
            fd = res.get('financialData', {})
            et = res.get('earningsTrend', {}).get('trend', [])
            dks = res.get('defaultKeyStatistics', {})
            price = res.get('price', {})

            # EV + Market Cap now sourced from Finviz, with Yahoo as fallback.
            ev_raw = finviz_ev_raw or dks.get('enterpriseValue', {}).get('raw', 0)
            market_cap_raw = finviz_market_cap_raw or dks.get('marketCap', {}).get('raw', 0) or price.get('marketCap', {}).get('raw', 0)
            
            # Fetch CAPEX, D&A, and historical gross profit/revenue from timeseries.
            import time
            now = int(time.time())
            ts_types = 'annualCapitalExpenditure,annualDepreciationAndAmortization,annualGrossProfit,annualTotalRevenue'
            ts_url = f'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{ticker}?symbol={ticker}&type={ts_types}&period1={now-86400*365*6}&period2={now}&crumb={crumb}'
            ts_data = json.loads(urllib.request.urlopen(ts_url, timeout=3).read().decode('utf-8'))
            
            capex_raw = 0
            da_raw = 0
            annual_gross_profit_values = []
            annual_revenue_values = []
            try:
                ts_res = ts_data.get('timeseries', {}).get('result', [])
                for item in ts_res:
                    meta = item.get('meta', {}).get('type', [''])[0]
                    if meta == 'annualCapitalExpenditure':
                        arr = item.get('annualCapitalExpenditure', [])
                        if arr: capex_raw = abs(arr[-1].get('reportedValue', {}).get('raw', 0))
                    elif meta == 'annualDepreciationAndAmortization':
                        arr = item.get('annualDepreciationAndAmortization', [])
                        if arr: da_raw = arr[-1].get('reportedValue', {}).get('raw', 0)
                    elif meta == 'annualGrossProfit':
                        arr = item.get('annualGrossProfit', [])
                        annual_gross_profit_values = [
                            point.get('reportedValue', {}).get('raw', 0)
                            for point in arr
                            if point.get('reportedValue', {}).get('raw') is not None
                        ]
                    elif meta == 'annualTotalRevenue':
                        arr = item.get('annualTotalRevenue', [])
                        annual_revenue_values = [
                            point.get('reportedValue', {}).get('raw', 0)
                            for point in arr
                            if point.get('reportedValue', {}).get('raw') is not None
                        ]
            except Exception as e:
                print("Timeseries error:", e)

            rev = fd.get('totalRevenue', {}).get('raw', 0)
            margin = fd.get('operatingMargins', {}).get('raw', 0)
            margin_raw = fd.get('operatingMargins', {}).get('raw', 0)
            
            def format_3sig(val):
                if val == 0: return "0"
                abs_val = abs(val)
                if abs_val >= 100: res = f"{val:.0f}"
                elif abs_val >= 10: res = f"{val:.1f}"
                elif abs_val >= 1: res = f"{val:.2f}"
                elif abs_val >= 0.1: res = f"{val:.3f}"
                elif abs_val >= 0.01: res = f"{val:.4f}"
                else: res = f"{val:.3g}"
                if '.' in res:
                    res = res.rstrip('0').rstrip('.')
                return res

            margin_fmt = format_3sig(margin_raw * 100) + "%" if margin_raw != 0 else "--"
            
            def format_money(val):
                if val == 0: return "--"
                if abs(val) >= 1e12: return format_3sig(val / 1e12) + "T"
                if abs(val) >= 1e9: return format_3sig(val / 1e9) + "B"
                if abs(val) >= 1e6: return format_3sig(val / 1e6) + "M"
                return format_3sig(val)

            gp_3y_growth_str = "--"
            gp_3y_start_str = "--"
            gp_3y_end_str = "--"
            gp_3y_basis = "Gross Profit"
            growth_values = [v for v in annual_gross_profit_values if v]
            if len(growth_values) < 2:
                growth_values = [v for v in annual_revenue_values if v]
                gp_3y_basis = "Revenue"
            if len(growth_values) >= 2:
                start_val = growth_values[-4] if len(growth_values) >= 4 else growth_values[0]
                end_val = growth_values[-1]
                gp_3y_start_str = format_money(start_val)
                gp_3y_end_str = format_money(end_val)
                if start_val != 0:
                    gp_3y_growth = ((end_val - start_val) / abs(start_val)) * 100
                    gp_3y_growth_str = format_3sig(gp_3y_growth) + "%"

            adj_income_cy_str = "--"
            adj_income_ny_str = "--"
            cy_growth_str = "--"
            ny_growth_str = "--"
            rev_cy_raw = 0
            rev_ny_raw = 0
            
            for t in et:
                if t.get('period') == '0y':
                    rev_cy_raw = t.get('revenueEstimate', {}).get('avg', {}).get('raw', 0)
                    gwth_raw = t.get('revenueEstimate', {}).get('growth', {}).get('raw', 0)
                    if gwth_raw != 0:
                        cy_growth = gwth_raw * 100
                        res = format_3sig(cy_growth)
                        cy_growth_str = ("+" + res if cy_growth > 0 and not res.startswith('-') else res) + "%"
                elif t.get('period') == '+1y':
                    rev_ny_raw = t.get('revenueEstimate', {}).get('avg', {}).get('raw', 0)
                    gwth_raw = t.get('revenueEstimate', {}).get('growth', {}).get('raw', 0)
                    if gwth_raw != 0:
                        ny_growth = gwth_raw * 100
                        res = format_3sig(ny_growth)
                        ny_growth_str = ("+" + res if ny_growth > 0 and not res.startswith('-') else res) + "%"

            income_str = "--"
            adj_income_str = "--"
            capex_str = "--"
            da_str = "--"
            revenue_str = "--"
            operating_margin_str = "--"
            da_minus_capex_str = "--"
            ev_str = format_money(ev_raw) if ev_raw != 0 else "--"
            market_cap_str = format_money(market_cap_raw) if market_cap_raw != 0 else "--"
            net_debt_str = "--"
            ev_adj_ebit_str = "--"
            cy_adj_inc_str = "--"
            ny_adj_inc_str = "--"
            cy_revenue_str = format_money(rev_cy_raw) if rev_cy_raw != 0 else "--"
            ny_revenue_str = format_money(rev_ny_raw) if rev_ny_raw != 0 else "--"
            
            if rev != 0 and margin != 0:
                revenue_str = format_money(rev)
                operating_margin_str = format_3sig(margin_raw * 100) + "%"
                oper_income = rev * margin
                income_str = format_money(oper_income)
                capex_str = format_money(capex_raw)
                da_str = format_money(da_raw)
                
                # Adj Oper Inc
                da_minus_capex_raw = max(0, da_raw - capex_raw)
                da_minus_capex_str = format_money(da_minus_capex_raw) if da_minus_capex_raw != 0 else "0"
                adj_income = oper_income + da_minus_capex_raw
                adj_income_str = format_money(adj_income)
                
                # Adj Oper Margin
                adj_margin_ratio = adj_income / rev
                adj_margin = adj_margin_ratio * 100
                margin_fmt = format_3sig(adj_margin) + "%"
                
                # EV / CY & NY Adj EBIT
                if ev_raw != 0:
                    if adj_income != 0:
                        ev_adj_ebit_str = format_3sig(ev_raw / adj_income)
                        
                    if rev_cy_raw != 0:
                        cy_adj_inc = rev_cy_raw * adj_margin_ratio
                        if cy_adj_inc != 0: 
                            ev_cy_ebit_str = format_3sig(ev_raw / cy_adj_inc)
                            cy_adj_inc_str = format_money(cy_adj_inc)
                        
                    if rev_ny_raw != 0:
                        ny_adj_inc = rev_ny_raw * adj_margin_ratio
                        if ny_adj_inc != 0: 
                            ev_ny_ebit_str = format_3sig(ev_raw / ny_adj_inc)
                            ny_adj_inc_str = format_money(ny_adj_inc)

            if ev_raw != 0 and market_cap_raw != 0:
                net_debt_str = format_money(ev_raw - market_cap_raw)
                    
            return (
                income_str, margin_fmt, ev_cy_ebit_str, ev_ny_ebit_str, adj_income_str, capex_str, da_str, ev_str,
                ev_adj_ebit_str, cy_growth_str, ny_growth_str, cy_adj_inc_str, ny_adj_inc_str, market_cap_str, net_debt_str,
                revenue_str, operating_margin_str, da_minus_capex_str, cy_revenue_str, ny_revenue_str,
                gp_3y_growth_str, gp_3y_start_str, gp_3y_end_str, gp_3y_basis
            )
                
        except Exception as e:
            print("Yahoo error:", e)
            return "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "Gross Profit"

    def handle_api_request(self, ticker, refresh=False):
        if not ticker:
            self._send_response(400, {"error": "Ticker is required"})
            return

        cache = load_cache()
        today = datetime.date.today().isoformat()
        now_dt = datetime.datetime.now()
        pulled_at = now_dt.isoformat(timespec="seconds")

        if not refresh and ticker in cache and cache[ticker].get('date') == today:
            cached_payload = cache[ticker].get('data', {})
            cached_pulled_at = cache[ticker].get('pulledAt') or cached_payload.get('pulledAt')
            is_fresh = False
            if cached_pulled_at:
                try:
                    cached_dt = datetime.datetime.fromisoformat(cached_pulled_at)
                    is_fresh = (now_dt - cached_dt).total_seconds() <= CACHE_TTL_SECONDS
                except Exception:
                    is_fresh = False
            cache_has_required_fields = (
                isinstance(cached_payload, dict)
                and 'ev_cy_ebit' in cached_payload
                and 'marketCap' in cached_payload
                and 'netDebt' in cached_payload
                and 'gp_3y_growth' in cached_payload
                and 'gp_3y_start' in cached_payload
                and 'gp_3y_end' in cached_payload
                and 'gp_3y_basis' in cached_payload
                and cached_payload.get('marketCap') not in (None, "", "--")
                and cached_payload.get('netDebt') not in (None, "", "--")
            )
            if cache_has_required_fields and is_fresh:
                if 'dataDate' not in cached_payload:
                    cached_payload = {**cached_payload, "dataDate": cache[ticker].get('date', today)}
                if 'pulledAt' not in cached_payload or not cached_payload.get('pulledAt'):
                    cached_payload = {**cached_payload, "pulledAt": cache[ticker].get('pulledAt')}
                self._send_response(200, cached_payload)
                return
        elif refresh and ticker in cache:
            # Force refresh: drop cached entry so a new fetch overwrites it.
            try:
                del cache[ticker]
                save_cache(cache)
            except Exception:
                pass

        url = f"https://finviz.com/quote.ashx?t={ticker}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        
        try:
            with urllib.request.urlopen(req) as response:
                html = response.read().decode('utf-8', errors='ignore')
                
                # Match Short Float
                short_float_match = re.search(r'Short Float.*?</td>.*?<td[^>]*>.*?<b>\s*(.+?)\s*</b>', html, re.IGNORECASE)
                finviz_market_cap = self._extract_finviz_metric(html, "Market Cap")
                finviz_enterprise_value = self._extract_finviz_metric(html, "Enterprise Value")
                finviz_market_cap_raw = self._parse_finviz_abbrev_to_raw(finviz_market_cap)
                finviz_enterprise_value_raw = self._parse_finviz_abbrev_to_raw(finviz_enterprise_value)
                
                # Fetch Data from Yahoo Finance
                income, margin, ev_cy_ebit, ev_ny_ebit, adj_income, capex, da, ev, ev_adj_ebit, cy_growth, ny_growth, cy_adj_inc, ny_adj_inc, market_cap, net_debt, revenue, operating_margin, da_minus_capex, cy_revenue, ny_revenue, gp_3y_growth, gp_3y_start, gp_3y_end, gp_3y_basis = self.fetch_yahoo_finance_data(
                    ticker,
                    finviz_ev_raw=finviz_enterprise_value_raw,
                    finviz_market_cap_raw=finviz_market_cap_raw,
                )
                
                if short_float_match or income != "--":
                    short_float_str = short_float_match.group(1) if short_float_match else ""
                    short_float = "--"
                    if short_float_str and '%' in short_float_str:
                        try:
                            sf_val = float(short_float_str.replace('%', ''))
                            def format_3sig_sf(val):
                                if val == 0: return "0"
                                abs_val = abs(val)
                                if abs_val >= 100: res = f"{val:.0f}"
                                elif abs_val >= 10: res = f"{val:.1f}"
                                elif abs_val >= 1: res = f"{val:.2f}"
                                elif abs_val >= 0.1: res = f"{val:.3f}"
                                elif abs_val >= 0.01: res = f"{val:.4f}"
                                else: res = f"{val:.3g}"
                                if '.' in res: res = res.rstrip('0').rstrip('.')
                                return res
                            short_float = format_3sig_sf(sf_val) + "%"
                        except:
                            short_float = short_float_str

                    payload = {
                        "ticker": ticker, "shortFloat": short_float, "income": income,
                        "adj_income": adj_income, "capex": capex, "da": da, "margin": margin,
                        "ev_cy_ebit": ev_cy_ebit, "ev_ny_ebit": ev_ny_ebit,
                        "ev": ev, "ev_adj_ebit": ev_adj_ebit, "cy_growth": cy_growth, "ny_growth": ny_growth,
                        "marketCap": market_cap, "netDebt": net_debt,
                        "revenue": revenue, "operating_margin": operating_margin, "da_minus_capex": da_minus_capex,
                        "cy_revenue": cy_revenue, "ny_revenue": ny_revenue,
                        "cy_adj_inc": cy_adj_inc, "ny_adj_inc": ny_adj_inc,
                        "gp_3y_growth": gp_3y_growth,
                        "gp_3y_start": gp_3y_start,
                        "gp_3y_end": gp_3y_end,
                        "gp_3y_basis": gp_3y_basis,
                        "dataDate": today,
                        "pulledAt": pulled_at
                    }
                    
                    cache[ticker] = {'date': today, 'pulledAt': pulled_at, 'data': payload}
                    save_cache(cache)
                    
                    self._send_response(200, payload)
                else:
                    self._send_response(404, {"error": "Data not found for this ticker."})
        except HTTPError as e:
            if e.code == 404:
                self._send_response(404, {"error": "Ticker not found on Finviz."})
            else:
                self._send_response(500, {"error": f"Error fetching data: {e.code}"})
        except Exception as e:
            self._send_response(500, {"error": f"Server error: {str(e)}"})

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
    with ReusableTCPServer(("", PORT), Handler) as httpd:
        print("Serving at port", PORT)
        httpd.serve_forever()
