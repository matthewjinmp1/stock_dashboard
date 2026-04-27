import yfinance as yf
import requests

count = 0
orig_get = requests.Session.get

def mocked_get(self, *args, **kwargs):
    global count
    count += 1
    print(f"Fetch {count}: {args[0]}")
    return orig_get(self, *args, **kwargs)

requests.Session.get = mocked_get

print("Init Ticker...")
stock = yf.Ticker("AAPL")

print("\nGetting Info...")
info = stock.info

print("\nGetting Income Stmt...")
inc = stock.income_stmt

print("\nGetting Q Income Stmt...")
q_inc = stock.quarterly_income_stmt

print("\nGetting Balance Sheet...")
bs = stock.balance_sheet

print("\nGetting Q Balance Sheet...")
q_bs = stock.quarterly_balance_sheet

print("\nGetting Cashflow...")
cf = stock.cashflow

print("\nGetting Q Cashflow...")
q_cf = stock.quarterly_cashflow

print(f"\nTotal Fetches: {count}")
