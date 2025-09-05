import os, re, json, time, pathlib, logging
from typing import Dict, List, Optional, Set
from bs4 import BeautifulSoup
import html2text
from dotenv import load_dotenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ingest_doc")
ROOT = pathlib.Path(__file__).parent
STORAGE = ROOT / "storage"
RAW = STORAGE / "raw_docs"
STORAGE.mkdir(exist_ok=True)
RAW.mkdir(exist_ok=True)

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
DOC_LINK_RE = re.compile(r"https://docs\.google\.com/document/d/([\w-]+)/")

def build_drive():
    info = {
        "type": "service_account",
        "project_id": os.getenv("GOOGLE_PROJECT_ID"),
        "private_key_id": "dummy",
        "private_key": os.getenv("GOOGLE_PRIVATE_KEY").replace("\\n", "\n"),
        "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
        "client_id": "dummy",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/"
    }
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

def export_doc_html(drive, file_id: str) -> str:
    data = drive.files().export(fileId=file_id, mimeType='text/html').execute()
    return data.decode('utf-8') if isinstance(data, (bytes, bytearray)) else data

def file_webview_link(drive, file_id: str) -> str:
    return drive.files().get(fileId=file_id, fields="webViewLink, name").execute()["webViewLink"]

def extract_linked_doc_ids(html: str) -> Set[str]:
    return set(m.group(1) for m in DOC_LINK_RE.finditer(html))

def html_to_md(html: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.ignore_images = True
    h.body_width = 0
    md = h.handle(html)
    return re.sub(r"\n{3,}", "\n\n", md).strip()

def get_title_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    if soup.title and soup.title.text.strip():
        return soup.title.text.strip()
    h1 = soup.find(["h1","h2"])
    return h1.get_text(strip=True) if h1 else "Без названия"

def save_doc(file_id: str, title: str, url: str, md: str):
    path = RAW / f"{file_id}.md"
    path.write_text(md, encoding="utf-8")
    meta_path = STORAGE / "meta.json"
    meta = json.loads(meta_path.read_text("utf-8")) if meta_path.exists() else {}
    meta[file_id] = {"title": title, "url": url, "path": str(path)}
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), "utf-8")

def crawl_from_root_doc():
    root_doc_id = os.getenv("ROOT_DOC_ID")
    if not root_doc_id:
        raise RuntimeError("В .env должен быть ROOT_DOC_ID (ID главного документа-оглавления)")

    drive = build_drive()
    to_visit: List[str] = [root_doc_id]
    visited: Set[str] = set()

    while to_visit:
        doc_id = to_visit.pop(0)
        if doc_id in visited:
            continue
        visited.add(doc_id)

        log.info("Выгружаю документ %s", doc_id)
        try:
            html = export_doc_html(drive, doc_id)
        except HttpError as e:
            log.error("Нет доступа к %s: %s", doc_id, e)
            continue

        title = get_title_from_html(html)
        url = file_webview_link(drive, doc_id)
        md = html_to_md(html)
        save_doc(doc_id, title, url, md)

        # добавляем все связанные документы из ссылок
        for lid in extract_linked_doc_ids(html):
            if lid not in visited:
                to_visit.append(lid)

        time.sleep(0.2)

    log.info("Готово. Документов выгружено: %d", len(visited))

if __name__ == "__main__":
    crawl_from_root_doc()
