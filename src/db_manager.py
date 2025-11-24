import sqlite3
import os
from contextlib import contextmanager

# データベースファイルのパス
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'stock_data.db')

@contextmanager
def get_connection():
    """データベース接続を管理するコンテキストマネージャ"""
    # dataディレクトリがない場合は作成
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    """データベースとテーブルの初期化"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # 1. 銘柄マスタテーブル (企業情報)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            code TEXT PRIMARY KEY,
            name TEXT,
            market TEXT,
            industry TEXT
        )
        ''')

        # 2. 日足株価テーブル (Daily Prices)
        # 週足チャートはここからプログラム側で集計して作成します
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_prices (
            code TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            PRIMARY KEY (code, date)
        )
        ''')
        
        # 3. 財務・指標テーブル (Financials)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_financials (
            code TEXT,
            date TEXT,
            market_cap REAL,      -- 時価総額
            per_forecast REAL,    -- 予想PER
            pbr_actual REAL,      -- 実績PBR
            eps_forecast REAL,    -- 予想EPS
            bps_actual REAL,      -- 実績BPS
            dividend_yield REAL,  -- 配当利回り
            PRIMARY KEY (code, date)
        )
        ''')

        # 4. 信用残テーブル (Margin Trading)
        # 株・プラス: tosho-stock-margin-transactions-2
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS weekly_margin (
            code TEXT,
            date TEXT,
            sell_balance INTEGER, -- 売り残
            buy_balance INTEGER,  -- 買い残
            ratio REAL,           -- 信用倍率
            PRIMARY KEY (code, date)
        )
        ''')
        
        # 5. 価格帯別出来高テーブル - Volume Profile Cache
        # 特定期間（例：過去1年）における出来高の分布をキャッシュ
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS volume_profile (
            code TEXT,
            analysis_date TEXT,     -- 計算実行日
            price_band REAL,        -- 価格帯
            volume_sum INTEGER,     -- その価格帯で取引された総出来高
            PRIMARY KEY (code, analysis_date, price_band)
        )
        ''')
        
        conn.commit()
        print(f"✅ Database initialized at: {DB_PATH}")

if __name__ == '__main__':
    # 直接実行された場合はDBを初期化
    init_db()
