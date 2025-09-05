import os, json, pathlib, pickle, numpy as np
from dotenv import load_dotenv
import faiss
from fastembed import TextEmbedding

load_dotenv()
ROOT = pathlib.Path(__file__).parent
STORAGE = ROOT/"storage"
CHUNKS = STORAGE/"chunks.jsonl"

MODEL_NAME = os.getenv("EMBED_MODEL","sentence-transformers/paraphrase-multilingual-mpnet-base-v2")

def l2_normalize(X: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(X, axis=1, keepdims=True) + 1e-12
    return (X / n).astype("float32")

def main():
    if not CHUNKS.exists():
        raise SystemExit("Нет storage/chunks.jsonl — сначала запустите make_chunks.py")

    recs = [json.loads(l) for l in CHUNKS.read_text("utf-8").splitlines()]
    texts = [" ".join(r["text"].split()) for r in recs]

    print(f"Фрагментов для кодирования: {len(texts)}")
    embedder = TextEmbedding(model_name=MODEL_NAME)
    vecs = list(embedder.embed(texts, batch_size=64))
    X = l2_normalize(np.vstack(vecs))

    index = faiss.IndexFlatIP(X.shape[1])
    index.add(X)

    np.save(STORAGE/"embeddings.npy", X)
    faiss.write_index(index, str(STORAGE/"index.faiss"))

    with open(STORAGE/"bm25.pkl", "wb") as f:
        pickle.dump(texts, f)

    print("✅ Индекс готов: embeddings.npy, index.faiss, bm25.pkl сохранены в storage/")

if __name__ == "__main__":
    main()
