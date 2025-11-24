import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# .envから株・プラスの認証情報を取得
load_dotenv()
KABU_PLUS_USER = os.getenv('KABU_PLUS_USER')
KABU_PLUS_PASSWORD = os.getenv('KABU_PLUS_PASSWORD')

# NOTE: 実際にはここに株・プラスのAPI接続ロジックが入ります。
#       以下の関数は、一旦ダミーのデータ（Pandas DataFrame）を返すように記述しています。

def fetch_data(code: str) -> dict:
    """
    指定された証券コードに基づき、株価、財務、需給データを取得する（ダミー関数）。
    
    Args:
        code: 証券コード (例: '7203')
        
    Returns:
        取得したデータを含む辞書
    """
    
    if not KABU_PLUS_USER or not KABU_PLUS_PASSWORD:
        return {"error": "株plusの認証情報が設定されていません。"}
        
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 株・プラス認証情報確認: OK. 証券コード {code} のデータ取得を開始します。")

    # --- 1. ダミーの株価データ (テクニカル分析用) ---
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    dummy_prices = [1500 + i % 100 + (i // 5) * 5 for i, _ in enumerate(dates)]
    
    stock_data = pd.DataFrame({
        'Date': dates,
        'Open': dummy_prices,
        'High': [p + 10 for p in dummy_prices],
        'Low': [p - 10 for p in dummy_prices],
        'Close': [p + 5 for p in dummy_prices],
        'Volume': [10000 + i * 100 for i, _ in enumerate(dates)]
    }).set_index('Date').dropna()
    
    # --- 2. ダミーの財務データ (ファンダメンタル分析用) ---
    financial_data = {
        'year': [2021, 2022, 2023, 2024, 2025],
        'sales': [5000, 5500, 6200, 6800, 7500],
        'op_profit': [450, 500, 580, 650, 720],
        'net_profit': [300, 350, 400, 480, 550],
        'eps': [120, 140, 160, 190, 220],
        'per': [15.0, 14.5, 16.0, 13.5, 12.0],
        'roe': [10.5, 11.2, 12.0, 12.8, 13.5]
    }
    
    return {
        "stock_data": stock_data,
        "financial_data": pd.DataFrame(financial_data),
        "company_name": f"銘柄コード {code} のダミー企業",
        "company_summary": "この企業は〇〇事業を主軸とし、特に海外展開に強みがあります。",
        "error": None
    }

# データ取得のテスト用関数（直接実行時）
if __name__ == '__main__':
    # .envを読み込まないと認証情報がないため、ここでは読み込みを省略
    data = fetch_data('7203')
    if not data.get("error"):
        print("\n--- 株価データ (一部) ---")
        print(data['stock_data'].tail())
        print("\n--- 財務データ ---")
        print(data['financial_data'])
    else:
        print(data['error'])

