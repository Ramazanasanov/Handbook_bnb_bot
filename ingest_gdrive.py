import os, re, json, time, pathlib, logging
from typing import Dict, List, Set
from bs4 import BeautifulSoup
import html2text

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ingest")

ROOT = pathlib.Path(__file__).parent
STORAGE = ROOT / "storage"
RAW = STORAGE / "raw_docs.jsonl"
STORAGE.mkdir(exist_ok=True)

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
DOC_MIME = "application/vnd.google-apps.document"
DOC_LINK_RE = re.compile(r"https://docs\.google\.com/document/d/([\w-]+)/")

def build_drive():
    info = {
        "type": "service_account",
        "project_id": os.getenv("GOOGLE_PROJECT_ID"),
        "private_key_id": "dummy",
        "private_key": (os.getenv("GOOGLE_PRIVATE_KEY") or "").replace("\\n", "\n"),
        "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
        "client_id": "dummy",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account"
    }
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

def list_all_docs_recursive(drive, folder_id: str) -> List[Dict]:
    docs = []
    def walk(fid: str):
        page = None
        while True:
            resp = drive.files().list(
                q=f"'{fid}' in parents and trashed=false",
                fields="nextPageToken, files(id,name,mimeType)",
                pageToken=page,
            ).execute()
            for f in resp.get("files", []):
                if f["mimeType"] == "application/vnd.google-apps.folder":
                    walk(f["id"])
                elif f["mimeType"] == DOC_MIME:
                    docs.append({"id": f["id"], "name": f["name"]})
            page = resp.get("nextPageToken")
            if not page:
                break
    walk(folder_id)
    return docs

def export_doc_html(drive, file_id: str) -> str:
    data = drive.files().export(fileId=file_id, mimeType='text/html').execute()
    return data.decode('utf-8') if isinstance(data, (bytes, bytearray)) else data

def file_meta(drive, file_id: str) -> Dict:
    return drive.files().get(fileId=file_id, fields="name, webViewLink").execute()

def extract_linked_ids(html: str) -> Set[str]:
    return set(m.group(1) for m in DOC_LINK_RE.finditer(html))

def html_to_md(html: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.ignore_images = True
    h.body_width = 0
    md = h.handle(html)
    return re.sub(r"\n{3,}", "\n\n", md).strip()

def title_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    if soup.title and soup.title.text.strip():
        return soup.title.text.strip()
    h1 = soup.find(["h1","h2"])
    return h1.get_text(strip=True) if h1 else ""

def crawl():
    allowed = os.getenv("ALLOWED_FOLDER_IDS","").strip()
    if not allowed:
        raise RuntimeError("В .env должен быть ALLOWED_FOLDER_IDS=cid1,cid2")
    allowed_folders = [x.strip() for x in allowed.split(",") if x.strip()]

    drive = build_drive()
    seed_docs: List[Dict] = []
    for fid in allowed_folders:
        seed_docs += list_all_docs_recursive(drive, fid)

    allowed_ids = {d["id"] for d in seed_docs}
    seen: Set[str] = set()

    with RAW.open("w", encoding="utf-8") as out:
        for d in seed_docs:
            fid, gname = d["id"], d["name"]
            if fid in seen:
                continue
            seen.add(fid)

            try:
                html = export_doc_html(drive, fid)
            except HttpError as e:
                log.error("Ошибка экспорта %s: %s", fid, e)
                continue

            meta = file_meta(drive, fid)
            web = meta.get("webViewLink", "")
            title_html = title_from_html(html)
            title = title_html if title_html.strip() else gname

            rec = {"id": fid, "title": title, "gdoc_name": gname, "url": web, "text": html_to_md(html)}
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")

            for lid in extract_linked_ids(html):
                if lid in seen or lid not in allowed_ids:
                    continue
                try:
                    html2 = export_doc_html(drive, lid)
                    meta2 = file_meta(drive, lid)
                    t2_html = title_from_html(html2)
                    t2 = t2_html if t2_html.strip() else meta2.get("name","Без названия")
                    rec2 = {"id": lid, "title": t2, "gdoc_name": meta2.get("name",""), "url": meta2.get("webViewLink",""), "text": html_to_md(html2)}
                    out.write(json.dumps(rec2, ensure_ascii=False) + "\n")
                    seen.add(lid)
                except HttpError:
                    continue
            time.sleep(0.1)

    log.info("Готово. Документов выгружено: %d", len(seen))

if __name__ == "__main__":
    crawl()
