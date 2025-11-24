import os
import discord
from dotenv import load_dotenv
from src.data_loader import fetch_data
from src.chart_generator import generate_charts
from src.analyzer import generate_analysis


# .envファイルを読み込み、環境変数として設定します
load_dotenv()
TOKEN = os.getenv('DISCORD_BOT_TOKEN')

# Discord Botの設定
intents = discord.Intents.default()
# コマンドを読み込むためにMESSAGE CONTENT INTENTを有効化
intents.message_content = True 
client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f'✅ Bot Login Successful: {client.user} としてログインしました。')
    print("--- 動作確認用Discordで /analyze 証券コード を試してください ---")

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    # /analyze コマンドの処理
    if message.content.startswith('/analyze'):
        # すべての処理をこのブロックで囲むことで、実行中はDiscordに「入力中...」を表示し続ける
        async with message.channel.typing():
            try:
                parts = message.content.split(' ')
                code = parts[1]
                
                # --- 1. データ取得フェーズ ---
                await message.channel.send(f'**{code}** のデータ取得を開始します。お待ちください...')

                analysis_data = fetch_data(code) 
                
                if analysis_data.get("error"):
                    # 認証エラーやデータ取得エラーの場合
                    await message.channel.send(f'データ取得エラー: {analysis_data["error"]}')
                    return
                    
                company = analysis_data["company_name"]
                
                # --- 2. グラフ生成・送信フェーズ ---
                await message.channel.send(f"### ✅ データ取得成功: {company} ({code})\n\n📈 グラフを生成しています。お待ちください...")
                    
                chart_info = generate_charts(analysis_data['stock_data'], code)
                    
                await message.channel.send(
                    content=f"**[{code}] ローソク足＆RSIチャート** (直近3ヶ月)",
                    file=discord.File(chart_info['file'], filename=chart_info['filename'])
                )

                # --- 3. AI分析フェーズ ---
                await message.channel.send("🧠 **Gemini AIによる詳細分析を開始します...**")
                
                analysis_result = generate_analysis(
                    company_name=company,
                    code=code,
                    summary=analysis_data['company_summary'],
                    stock_data=analysis_data['stock_data'],
                    financial_data=analysis_data['financial_data'],
                    chart_buffer=chart_info['file']
                )

                if analysis_result.get("error"):
                    await message.channel.send(f"AI分析エラー: {analysis_result['error']}")
                    return

                # AIレポートをDiscordに送信
                await message.channel.send(analysis_result['report'])
                                        
            except IndexError:
                await message.channel.send('エラー: 証券コードを入力してください。例: `/analyze 7203`')
            except Exception as e:
                # その他の予期せぬエラー
                await message.channel.send(f'予期せぬエラーが発生しました: {e}')

if TOKEN:
    client.run(TOKEN)
else:
    print("❌ Error: .envファイルにDISCORD_BOT_TOKENが設定されていません。")
