import os
import discord
from dotenv import load_dotenv

# .envファイルを読み込み、環境変数として設定します
# (venv)hiranotakahiro@...% pip install python-dotenv が完了している前提
load_dotenv()
TOKEN = os.getenv('DISCORD_BOT_TOKEN')

# Discord Botの設定
# メッセージ内容を読み取る権限を設定（intents）
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

@client.event
async def on_ready():
    # ログイン成功時にターミナルに表示されます
    print(f'✅ Bot Login Successful: {client.user} としてログインしました。')
    print("--- 動作確認用Discordで /analyze 証券コード を試してください ---")

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    # /analyze コマンドのテスト
    if message.content.startswith('/analyze'):
        try:
            # メッセージを分割し、2番目の要素（証券コード）を取得
            parts = message.content.split(' ')
            code = parts[1]
            
            # 最小限の応答
            await message.channel.send(f'証券コード **{code}** の分析準備を開始します...（データ取得機能はまだ未実装です）')
        except IndexError:
            # 証券コードが入力されていない場合
            await message.channel.send('エラー: 証券コードを入力してください。例: `/analyze 7203`')

# トークンが設定されていれば実行
if TOKEN:
    client.run(TOKEN)
else:
    print("❌ Error: .envファイルにDISCORD_BOT_TOKENが設定されていません。")
