import yfinance as yf

def test_fetch():
    ticker = "AAPL"
    print(f"Testing yfinance fetch for {ticker}...")
    
    # Create Ticker object
    stock = yf.Ticker(ticker)
    
    # Get basic info
    try:
        info = stock.info
        print("\n--- Basic Info ---")
        print(f"Company: {info.get('longName', 'N/A')}")
        print(f"Current Price: {info.get('currentPrice', 'N/A')}")
        print(f"Market Cap: {info.get('marketCap', 'N/A')}")
    except Exception as e:
        print(f"Error fetching info: {e}")

    # Get financials (Income Statement)
    try:
        print("\n--- Annual Income Statement ---")
        financials = stock.financials
        if not financials.empty:
            print(financials.head())
        else:
            print("No financial data found.")
    except Exception as e:
        print(f"Error fetching financials: {e}")
        
    # Get quarterly financials
    try:
        print("\n--- Quarterly Income Statement ---")
        q_financials = stock.quarterly_financials
        if not q_financials.empty:
            print(q_financials.head())
        else:
            print("No quarterly financial data found.")
    except Exception as e:
        print(f"Error fetching quarterly financials: {e}")

if __name__ == "__main__":
    test_fetch()
