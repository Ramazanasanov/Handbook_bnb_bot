import os, json, pathlib, pickle, logging
from typing import List, Dict
import numpy as np
from sentence_transformers import SentenceTransformer
import faiss
from dotenv import load_dotenv
from text_utils import split_into_chunks

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("index")

ROOT = pathlib.Path(__file__).parent
STORAGE = ROOT/"storage"
RAW = STORAGE/"raw_docs"

MODEL_NAME = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-base")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE_CHARS", "1200"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP_CHARS", "200"))

def load_meta() -> Dict:
    path = STORAGE/"meta.json"
    if not path.exists():
        raise RuntimeError("Нет storage/meta.json — сначала выполните ingest скрипт")
    return json.loads(path.read_text("utf-8"))

def build_chunks():
    meta = load_meta()
    records: List[Dict] = []
    for fid, info in meta.items():
        text = pathlib.Path(info["path"]).read_text("utf-8")
        chunks = split_into_chunks(text, CHUNK_SIZE, CHUNK_OVERLAP)
        for i, ch in enumerate(chunks):
            records.append({
                "doc_id": fid,
                "chunk_id": f"{fid}:{i}",
                "text": ch,
                "title": info["title"],
                "url": info["url"],
            })
    (STORAGE/"chunks.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in records), "utf-8"
    )
    log.info("Сформировано фрагментов: %d", len(records))

def build_embeddings():
    model = SentenceTransformer(MODEL_NAME)
    recs = [json.loads(l) for l in (STORAGE/"chunks.jsonl").read_text("utf-8").splitlines()]
    texts = ["query: "+r["text"] for r in recs]
    X = model.encode(texts, batch_size=32, normalize_embeddings=True, show_progress_bar=True)
    np.save(STORAGE/"embeddings.npy", X)
    index = faiss.IndexFlatIP(X.shape[1])
    index.add(X.astype("float32"))
    faiss.write_index(index, str(STORAGE/"index.faiss"))
    # простой резерв — сохраним тексты
    with open(STORAGE/"bm25.pkl", "wb") as f:
        pickle.dump([r["text"] for r in recs], f)
    log.info("Готово: эмбеддинги и индекс")

if __name__ == "__main__":
    build_chunks()
    build_embeddings()
