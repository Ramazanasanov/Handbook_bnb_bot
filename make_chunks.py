import json, re, textwrap
from pathlib import Path

ROOT = Path(__file__).parent
RAW = ROOT/"storage/raw_docs.jsonl"      
OUT = ROOT/"storage/chunks.jsonl"        


MAX_CHARS = 1200
MIN_CHARS = 300

def clean(t: str) -> str:
    t = t.replace("\u200b","").replace("\xa0"," ")
    t = re.sub(r"[ \t]{2,}", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def split_into_chunks(text: str):
    paras = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    buf, cur = [], 0
    for p in paras:
        if not buf:
            buf = [p]; cur = len(p)
            continue
        if cur + 1 + len(p) <= MAX_CHARS:
            buf.append(p); cur += 1 + len(p)
        else:
            chunk = clean("\n\n".join(buf))
            if len(chunk) >= MIN_CHARS:
                yield chunk
            else:
                
                buf.append(p); cur += 1 + len(p)
                chunk = clean("\n\n".join(buf))
                yield chunk
            buf, cur = [], 0
    if buf:
        yield clean("\n\n".join(buf))

def main():
    if not RAW.exists():
        raise SystemExit("Нет storage/raw_docs.jsonl — сначала запустите краулер ingest_gdrive.py")
    lines = RAW.read_text("utf-8").splitlines()
    out = []
    for i, line in enumerate(lines, 1):
        try:
            doc = json.loads(line)
        except Exception:
            continue
        text = doc.get("text") or ""
        url  = doc.get("url") or doc.get("source") or ""
        title= doc.get("title") or "Документ"
        if not text.strip():
            continue
        for ch in split_into_chunks(text):
            out.append({"text": ch, "url": url, "title": title})
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in out), "utf-8")
    print(f"Готово: чанков {len(out)} → {OUT}")

if __name__ == "__main__":
    main()
