import os
import io
import mplfinance as mpf
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

def generate_charts(data: pd.DataFrame, code: str) -> dict:
    """
    株価データからローソク足チャートとRSIチャートを生成し、
    Discordに送信可能な形式 (BytesIO) で返す（ダミー機能）。

    Args:
        data: 株価データ (DataFrame, インデックスは日付)
        code: 証券コード

    Returns:
        生成されたチャート画像のバイナリデータとファイル名を含む辞書
    """
    
    # --- 1. RSIの計算 (ダミーデータに基づく) ---
    # 注: pandas-taはまだインストールしていないため、ここでは手動でダミーRSIを計算
    
    # 差分を計算
    delta = data['Close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # 14日間の移動平均（Wilder's smoothing）
    avg_gain = gain.ewm(com=13, adjust=False).mean()
    avg_loss = loss.ewm(com=13, adjust=False).mean()

    rs = avg_gain / avg_loss
    data['RSI'] = 100 - (100 / (1 + rs))

    # --- 2. チャート生成 ---
    
    # データを最新の約3ヶ月分に絞る (プロットを見やすくするため)
    plot_data = data.iloc[-90:]

    # RSIサブプロットを作成
    apd = mpf.make_addplot(plot_data['RSI'], panel=2, color='blue', ylabel='RSI (14)')

    # チャートのスタイル設定
    mc = mpf.make_marketcolors(up='r', down='b', inherit=True)
    s = mpf.make_mpf_style(base_mpf_style='default', marketcolors=mc, gridcolor='gray')

    # ファイル名を決定
    filename_candle = f"chart_{code}_{datetime.now().strftime('%Y%m%d')}.png"

    # mplfinanceで描画
    fig, axes = mpf.plot(
        plot_data, 
        type='candle', 
        style=s, 
        title=f'ローソク足 & RSI (コード: {code})',
        ylabel='Price',
        volume=True,
        addplot=apd,
        returnfig=True  # 図オブジェクトを返す
    )

    # 画像をメモリに保存（Discord送信形式）
    buffer = io.BytesIO()
    fig.savefig(buffer, format='png')
    buffer.seek(0)
    plt.close(fig) # メモリリーク防止

    return {
        "file": buffer, 
        "filename": filename_candle
    }

# if __name__でのテストコードは省略
