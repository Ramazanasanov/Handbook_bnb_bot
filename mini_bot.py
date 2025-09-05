import os
from dotenv import load_dotenv
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram import Update

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN не найден или некорректен в .env")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Мини-бот работает!")

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    print("🤖 Мини-бот запущен. Жду сообщений в Telegram...")
    app.run_polling()

if __name__ == "__main__":
    main()
