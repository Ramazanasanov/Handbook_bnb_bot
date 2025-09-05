import os, json, logging, re, pathlib, numpy as np
from dotenv import load_dotenv
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, ContextTypes, filters
from telegram import Update
import faiss
from fastembed import TextEmbedding

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

ROOT = pathlib.Path(__file__).parent
STORAGE = ROOT/"storage"
CHUNKS_PATH = STORAGE/"chunks.jsonl"
EMB_PATH = STORAGE/"embeddings.npy"
INDEX_PATH = STORAGE/"index.faiss"

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN","")
# MODEL_NAME from .env
MODEL_NAME = os.getenv("EMBED_MODEL","sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN не найден/некорректен")

# Префиксы нужны только для E5
def is_e5(model: str) -> bool:
    return "e5" in (model or "").lower()

# Загрузка корпуса
chunks = [json.loads(l) for l in CHUNKS_PATH.read_text("utf-8").splitlines()]
X = np.load(EMB_PATH)
index = faiss.read_index(str(INDEX_PATH))
if index.ntotal != X.shape[0] or len(chunks) != X.shape[0]:
    raise RuntimeError("❌ Размеры индекса/эмбеддингов/текстов не совпадают")

embedder = TextEmbedding(model_name=MODEL_NAME)

def embed_query(q: str):
    q = q.strip()
    if is_e5(MODEL_NAME):
        q = "query: " + q
    v = np.asarray(list(embedder.embed([q]))[0], dtype="float32")
    n = np.linalg.norm(v) + 1e-12
    return (v / n).astype("float32").reshape(1, -1)

def keyword_score(text: str, q_tokens: list[str]) -> float:
    t = text.lower()
    return sum(t.count(tok) for tok in q_tokens)

def best_hit(q: str):
    q_tokens = [w for w in re.findall(r"\w+", q.lower()) if len(w) >= 3]
    v = embed_query(q)
    D, I = index.search(v, 15)  # расширим кандидатов
    if I.size == 0 or I[0,0] < 0:
        return None
    candidates = []
    for rank in range(I.shape[1]):
        idx = int(I[0,rank])
        if idx < 0: 
            continue
        h = chunks[idx].copy()
        sim = float(D[0,rank])
        kw = keyword_score(h.get("text",""), q_tokens) + 0.5*keyword_score(h.get("title",""), q_tokens)
        # комбинированный скор: вектор + ключевые слова
        score = 0.82*sim + 0.18*(1.0 if kw>0 else 0.0) + min(kw,5)*0.01
        candidates.append((score, sim, kw, h))
    candidates.sort(key=lambda x: x[0], reverse=True)
    # отсечём явно слабые совпадения
    for score, sim, kw, h in candidates:
        if sim >= 0.18 or kw > 0:
            return h
    return candidates[0][3] if candidates else None

def make_snippet(text: str, q: str, max_len=500) -> str:
    t = re.sub(r"\s+", " ", text).strip()
    if not t:
        return ""
    # постараемся вырезать вокруг первого совпадения любого слова из запроса
    q_tokens = [w for w in re.findall(r"\w+", q.lower()) if len(w)>=3]
    pos = -1
    low = t.lower()
    for tok in q_tokens:
        pos = low.find(tok)
        if pos != -1:
            break
    if pos == -1:
        return t[:max_len]
    start = max(0, pos - max_len//2)
    end = min(len(t), start + max_len)
    return t[start:end]

def format_reply(hit: dict, q: str) -> str:
    snippet = make_snippet(hit.get("text",""), q)
    title = hit.get("title","Документ")
    url = hit.get("url","")
    if url:
        return f"{snippet}\n\nПодробнее: {title}\n{url}"
    return snippet

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Бот готов. Спросите: «Чек-лист открытия», «Дресс-код бариста», «График уборки» …")

async def handle_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = (update.message.text or "").strip()
    if not q:
        return
    try:
        hit = best_hit(q)
        if not hit:
            await update.message.reply_text("Пока не нашёл ответ. Уточните запрос.")
            return
        await update.message.reply_text(format_reply(hit, q))
    except Exception as e:
        log.exception("Ошибка:", exc_info=e)
        await update.message.reply_text("Произошла ошибка. Попробуйте ещё раз.")

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_question))
    print("🤖 Бот запущен. Жду сообщения в Telegram...")
    app.run_polling()

if __name__ == "__main__":
    main()
