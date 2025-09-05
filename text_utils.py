import re

def split_into_chunks(text: str, size: int = 1200, overlap: int = 200):
    paras = re.split(r"\n\s*\n", text)
    chunks, buf = [], ""
    for p in paras:
        if len(buf) + len(p) + 2 <= size:
            buf = (buf + "\n\n" + p).strip()
        else:
            if buf:
                chunks.append(buf)
                tail = buf[-overlap:]
                buf = (tail + "\n\n" + p).strip()
            else:
                chunks.append(p[:size])
                buf = p[size-overlap:]
    if buf:
        chunks.append(buf)
    return [c.strip() for c in chunks if c.strip()]

def best_snippet(text: str, query: str, window: int = 400) -> str:
    words = [w for w in re.findall(r"\w+", query.lower()) if len(w) > 2]
    t = text
    lo = min((t.lower().find(w) for w in words if w in t.lower()), default=-1)
    if lo == -1:
        return (t[:window] + "…") if len(t) > window else t
    start = max(0, lo - window//3)
    end = min(len(t), start + window)
    return (t[start:end] + ("…" if end < len(t) else "")).strip()
