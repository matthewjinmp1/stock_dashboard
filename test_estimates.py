import yfinance as yf

stock = yf.Ticker("MSFT")

print("=== revenue_estimate ===")
try:
    re = stock.revenue_estimate
    if re is not None and not re.empty:
        print(f"columns: {list(re.columns)}")
        print(f"index: {list(re.index)}")
        print(re)
    else:
        print("Empty or None")
except Exception as e:
    print(f"Error: {e}")

print("\n=== earnings_estimate ===")
try:
    ee = stock.earnings_estimate
    if ee is not None and not ee.empty:
        print(f"columns: {list(ee.columns)}")
        print(f"index: {list(ee.index)}")
        print(ee)
    else:
        print("Empty or None")
except Exception as e:
    print(f"Error: {e}")

print("\n=== growth_estimates ===")
try:
    ge = stock.growth_estimates
    if ge is not None and not ge.empty:
        print(f"columns: {list(ge.columns)}")
        print(f"index: {list(ge.index)}")
        print(ge)
    else:
        print("Empty or None")
except Exception as e:
    print(f"Error: {e}")

print("\n=== analyst_price_targets ===")
try:
    apt = stock.analyst_price_targets
    print(apt)
except Exception as e:
    print(f"Error: {e}")

print("\n=== info revenue keys ===")
info = stock.info
for k in sorted(info.keys()):
    if any(x in k.lower() for x in ["revenue", "growth", "eps", "earning", "forward", "trailing"]):
        print(f"  {k}: {info[k]}")
