import yfinance as yf

stock = yf.Ticker("MSFT")

print("=== Income Statement Index Labels ===")
df = stock.financials
if df is not None and not df.empty:
    for i, label in enumerate(df.index):
        normalized = label.replace(" ", "").lower()
        print(f"  {i:2d}. '{label}' -> normalized: '{normalized}'")

print("\n=== Balance Sheet Index Labels ===")
df = stock.balance_sheet
if df is not None and not df.empty:
    for i, label in enumerate(df.index):
        print(f"  {i:2d}. '{label}'")

print("\n=== Cash Flow Index Labels ===")
df = stock.cashflow
if df is not None and not df.empty:
    for i, label in enumerate(df.index):
        print(f"  {i:2d}. '{label}'")
