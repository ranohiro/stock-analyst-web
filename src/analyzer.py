import os
from dotenv import load_dotenv
from google import genai
from google.genai import types
import pandas as pd
import io

# .envファイルを読み込み、環境変数として設定
load_dotenv()
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

# Geminiクライアントの初期化
if GEMINI_API_KEY:
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
    except Exception as e:
        print(f"Gemini Client Initialization Error: {e}")
        client = None
else:
    client = None

def generate_analysis(
    company_name: str, 
    code: str, 
    summary: str, 
    stock_data: pd.DataFrame, 
    financial_data: pd.DataFrame, 
    chart_buffer: io.BytesIO
) -> dict:
    """
    株価・財務データとチャート画像に基づき、Gemini AIによる分析レポートを生成する。
    """
    
    if not client:
        return {"error": "Gemini APIキーが設定されていないか、クライアントの初期化に失敗しています。"}

    # --- 1. テキストプロンプトの準備 ---
    
    # 財務データを整形（最新5年分）
    financial_text = financial_data.to_markdown(index=False)
    
    # 株価データから主要な指標を抽出（最新の終値と過去の平均など）
    latest_close = stock_data['Close'].iloc[-1]
    last_90_days_avg = stock_data['Close'].iloc[-90:].mean()
    
    # Geminiに渡すためのシステムプロンプト（AIへの役割設定）
    system_prompt = (
        "あなたは日本の株式市場の専門家であり、優秀なアナリストです。提供されたデータ（株価チャート画像、財務データ、企業概要）に基づき、"
        "以下の構造で日本語の分析レポートを作成してください。トーンは客観的でプロフェッショナルなものにしてください。"
    )

    # ユーザーからAIへの入力指示
    user_prompt = f"""
    ### 銘柄分析レポート作成依頼

    **銘柄名:** {company_name} ({code})
    **企業概要:** {summary}
    **現在の株価:** {latest_close:.2f} 円
    **過去90日間の平均株価:** {last_90_days_avg:.2f} 円

    **【財務データ (過去5年間の主要指標)】**
    {financial_text}

    **【分析依頼事項】**
    1.  **株価動向の評価 (テクニカル):** 提供されたチャート画像（ローソク足とRSI）を見て、現在の株価トレンド（上昇/下降/レンジ）と、短期的な売買シグナル（RSIなど）を評価してください。
    2.  **財務健全性の評価 (ファンダメンタルズ):** 財務データ（売上、利益、EPS、PER、ROE）の推移を見て、企業の成長性、収益性、割安感を評価してください。
    3.  **総合的な見解:** 上記を踏まえ、この銘柄に対する総合的な投資見解（強気/中立/弱気）と、その理由を簡潔にまとめてください。
    """
    
    # --- 2. コンテンツの構築 (画像とテキストの結合) ---
    
    # チャート画像をPIL Image形式でメモリから読み込みます
    # Note: Gemini APIはBytesIOを直接受け付けるため、画像形式を指定します
    
    # Geminiへの入力コンテンツリスト
    contents = [
        types.Part.from_bytes(
            data=chart_buffer.getvalue(),
            mime_type='image/png'
        ),
        user_prompt
    ]

    # --- 3. Gemini APIの呼び出し ---
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',  # 高速かつマルチモーダル対応のモデル
            contents=contents,
            config=types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.2  # 客観的な分析のため、低めの温度を設定
            )
        )
        return {"report": response.text, "error": None}
        
    except Exception as e:
        return {"error": f"Gemini API実行中にエラーが発生しました: {e}"}

# テスト用コードは省略
