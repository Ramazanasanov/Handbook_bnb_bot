import os, sys, traceback, logging, asyncio
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

logging.basicConfig(level=logging.DEBUG)  # максимум логов

print(">>> bot_diag.py стартует", flush=True)

try:
    load_dotenv(".env")
    TOKEN = os.getenv("BOT_TOKEN")
    print("TOKEN_PREFIX:", (TOKEN or "")[:15], flush=True)
    if not TOKEN or ":" not in TOKEN:
        raise RuntimeError("❌ Некорректный BOT_TOKEN (пустой или обрезан)")
except Exception as e:
    print("Ошибка загрузки .env:", e, flush=True)
    sys.exit(1)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Диагностический бот работает. /start ок")

def run():
    print(">>> Создаю Application...", flush=True)
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    print("🤖 Запускаю run_polling() ...", flush=True)
    try:
        app.run_polling()
        print("<<< run_polling() вернулась (бот остановлен)", flush=True)
    except Exception as e:
        print("❌ Исключение в run_polling:", repr(e), flush=True)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run()
