import os
import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv
import io
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.db_manager import get_connection
# ******** 🌟 修正箇所 0: Unionをインポート 🌟 ********
from typing import Union
# ******************************************************

# .envファイルを読み込み
load_dotenv()
KABU_PLUS_USER = os.getenv('KABU_PLUS_USER')
KABU_PLUS_PASSWORD = os.getenv('KABU_PLUS_PASSWORD')

# 株・プラスのベースURL
KABU_PLUS_BASE_URL = 'https://csvex.com/kabu.plus/csv/'
TIMEOUT = 30  # 秒
ENCODING = 'cp932' # 文字化け対策

# --- 接続設定 ---
def make_session_with_retries():
    """リトライ機能付きのrequestsセッションを作成"""
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "StockAnalysisBot/1.0"
    })
    return s

# ******** 🌟 修正箇所 1: 型ヒントを Union[int, None] に変更 🌟 ********
def download_csv(url: str, session: requests.Session, skiprows: Union[int, None] = None):
    """CSVをダウンロードし、Pandas DataFrameとして返す"""
    auth_tuple = (KABU_PLUS_USER, KABU_PLUS_PASSWORD)
    
    try:
        response = session.get(url, auth=auth_tuple, timeout=TIMEOUT)
        response.raise_for_status()
        
        # skiprows引数を使用して読み込み
        df = pd.read_csv(io.BytesIO(response.content), encoding=ENCODING, skiprows=skiprows)
        return df

    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            print(f"  -> スキップ: {url.split('/')[-1]} のデータはまだ存在しません (404 Not Found)。")
        elif response.status_code == 401:
            print(f"  -> エラー: 401 Unauthorized。認証情報 KABU_PLUS_USER/PW を確認してください。")
        else:
            print(f"  -> エラー: HTTPエラーが発生しました: {e}")
    except requests.exceptions.RequestException as e:
        print(f"  -> エラー: リクエストエラーが発生しました: {e}")
    except Exception as e:
        print(f"  -> エラー: 処理中に予期せぬエラーが発生しました: {e}")
    return None
# ******************************************************


# --- 1. 日足株価の処理 ---
def insert_daily_prices(date_str: str, conn: sqlite3.Connection, session: requests.Session):
    """日足株価データをダウンロードし、daily_pricesテーブルに挿入する"""
    filename = f"japan-all-stock-prices-2_{date_str}.csv"
    url = f"{KABU_PLUS_BASE_URL}japan-all-stock-prices-2/daily/{filename}"
    
    # skiprows=None (ヘッダー行をスキップしない)
    df = download_csv(url, session, skiprows=None)
    if df is None: return

    try:
        required_cols = {
            'SC': 'code',
            '日付': 'date',
            '始値': 'open',
            '高値': 'high',
            '安値': 'low',
            '株価': 'close', # 株価は終値として扱う
            '出来高': 'volume'
        }
        
        df.rename(columns=required_cols, inplace=True)
        db_columns = ['code', 'date', 'open', 'high', 'low', 'close', 'volume']
        
        # 'code'がリネーム後に存在するかチェック
        if 'code' not in df.columns:
            raise KeyError("'code'")
            
        df['date'] = date_str
        df['code'] = df['code'].astype(str)
        df = df[db_columns]
        
        records = [tuple(row) for row in df.replace({float('nan'): None}).itertuples(index=False)]

        conn.executemany("""
            INSERT OR REPLACE INTO daily_prices (code, date, open, high, low, close, volume) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, records)
        print(f"  -> 日足株価成功: {len(records)}件のデータをキューに追加。")
        
    except KeyError as e:
        print(f"  -> エラー: 株価CSVヘッダーエラー {e}。マッピングを確認してください。")


# --- 2. 財務指標の処理 ---
def insert_daily_financials(date_str: str, conn: sqlite3.Connection, session: requests.Session):
    """財務指標CSVをダウンロードし、daily_financialsテーブルに挿入する"""
    filename = f"japan-all-stock-data_{date_str}.csv"
    url = f"{KABU_PLUS_BASE_URL}japan-all-stock-data/daily/{filename}"
    
    # skiprows=None (ヘッダー行をスキップしない)
    df = download_csv(url, session, skiprows=None)
    if df is None: return
    
    try:
        required_cols = {
            'SC': 'code',
            '配当利回り（予想）': 'dividend_yield',
            'PER（予想）': 'per_forecast',
            'PBR（実績）': 'pbr_actual',
            'EPS（予想）': 'eps_forecast',
            'BPS（実績）': 'bps_actual',
            '時価総額（百万円）': 'market_cap'
        }
        
        df.rename(columns=required_cols, inplace=True)
        db_columns = ['code', 'date', 'market_cap', 'per_forecast', 'pbr_actual', 'eps_forecast', 'bps_actual', 'dividend_yield']
        
        # 'code'がリネーム後に存在するかチェック
        if 'code' not in df.columns:
            raise KeyError("'code'")

        df = df[[col for col in df.columns if col in required_cols.values()]]

        df['date'] = date_str
        df['code'] = df['code'].astype(str)
        df = df[db_columns]

        records = [tuple(row) for row in df.replace({float('nan'): None}).itertuples(index=False)]

        conn.executemany("""
            INSERT OR REPLACE INTO daily_financials (
                code, date, market_cap, per_forecast, pbr_actual, eps_forecast, bps_actual, dividend_yield
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, records)
        print(f"  -> 財務指標成功: {len(records)}件のデータをキューに追加。")
        
    except KeyError as e:
        print(f"  -> エラー: 財務指標CSVヘッダーエラー {e}。マッピングを確認してください。")


# --- 3. 信用残の処理 ---
def insert_weekly_margin(date_str: str, conn: sqlite3.Connection, session: requests.Session):
    """信用残週次データをダウンロードし、weekly_marginテーブルに挿入する"""
    filename = f"tosho-stock-margin-transactions-2_{date_str}.csv"
    url = f"{KABU_PLUS_BASE_URL}tosho-stock-margin-transactions-2/weekly/{filename}"
    
    print(f"  -> 信用残ダウンロード開始: {date_str} ({url})")
    
    # skiprows=1 (ヘッダー行を1行スキップ)
    df = download_csv(url, session, skiprows=1)
    if df is None: return

    try:
        # ご提供いただいた文字化けデータから逆算した正しいヘッダー名
        required_cols = {
            'SC': 'code',
            '公表日': 'date', 
            '信用売残': 'sell_balance',
            '信用買残': 'buy_balance',
            '貸借倍率': 'ratio', 
        }
        
        # 信用残のCSVは以下のカラム順（文字化け前）と特定しました。
        original_cols = ["SC","公表日","信用取引区分","信用売残","信用売残 前週比","信用買残","信用買残 前週比","貸借倍率", "制度信用売残", "制度信用売残 前週比", "制度信用買残", "制度信用買残 前週比", "一般信用売残", "一般信用売残 前週比", "一般信用買残", "一般信用買残 前週比"]
        
        # カラム数が一致する場合のみ、元の日本語カラム名に置き換え
        if len(df.columns) == len(original_cols):
            df.columns = original_cols
        else:
            print(f"  -> 警告: カラム数が合いません ({len(df.columns)} vs {len(original_cols)})。ヘッダー名のマッピングをスキップします。")
            raise KeyError("カラム数不一致")

        # 必要なカラムのみ抽出してリネーム
        df.rename(columns=required_cols, inplace=True)
        db_columns = ['code', 'date', 'sell_balance', 'buy_balance', 'ratio']
        
        # 'code'がリネーム後に存在するかチェック
        if 'code' not in df.columns:
            raise KeyError("'code'")
            
        df = df[[col for col in required_cols.values() if col in df.columns]]
        
        df['code'] = df['code'].astype(str)
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
        
        df = df[db_columns]

        records = [tuple(row) for row in df.replace({float('nan'): None}).itertuples(index=False)]

        conn.executemany("""
            INSERT OR REPLACE INTO weekly_margin (code, date, sell_balance, buy_balance, ratio) 
            VALUES (?, ?, ?, ?, ?)
        """, records)
        print(f"  -> 信用残成功: {len(records)}件のデータをキューに追加。")
        
    except KeyError as e:
        print(f"  -> エラー: 信用残CSVヘッダーエラー {e}。マッピングを確認してください。")
    except Exception as e:
        print(f"  -> 予期せぬエラー (信用残): {e}")


# --- メイン実行関数 (変更なし) ---
def run_daily_batch(start_date_str: str, end_date_str: str):
    """
    指定期間の日次/週次データをダウンロードし、データベースに格納するバッチ処理を実行
    """
    if not all([KABU_PLUS_USER, KABU_PLUS_PASSWORD]):
        print("❌ エラー: .envファイルにKABU_PLUS_USERまたはKABU_PLUS_PASSWORDが設定されていません。")
        return

    session = make_session_with_retries()
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')
    
    print(f"--- データバッチ処理開始: {start_date_str} から {end_date_str} まで ---")
    
    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    
    with get_connection() as conn:
        for date in dates:
            date_str = date.strftime('%Y%m%d')
            
            # 土日はスキップ (市場休業日)
            if date.weekday() >= 5: 
                continue
                
            print(f"\n--- 処理日: {date_str} ---")
            
            # 1. 日足株価 (Daily Prices)
            insert_daily_prices(date_str, conn, session)
            
            # 2. 財務指標 (Daily Financials)
            insert_daily_financials(date_str, conn, session)
            
            # 3. 信用残 (Weekly Margin)
            insert_weekly_margin(date_str, conn, session) 
            
            time.sleep(1.5)
        
        conn.commit()
        print("\n✅ 全処理完了: データベースにコミットしました。")


if __name__ == '__main__':
    # 実行例: 過去14日間のデータを取得し、初期DBを埋める
    end_date = datetime.now()
    start_date = end_date - timedelta(days=14)
    
    # 実行日をYYYYMMDD形式に整形（例: 20251124）
    run_daily_batch(start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d'))
