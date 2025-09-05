import os, io, re, json, time, pathlib, logging
from typing import Dict, List, Set
from bs4 import BeautifulSoup
import html2text

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

# --- подготовка ---
load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ingest_any")

ROOT = pathlib.Path(__file__).parent
STORAGE = ROOT / "storage"
RAW = STORAGE / "raw_docs"
STORAGE.mkdir(exist_ok=True)
RAW.mkdir(exist_ok=True)

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
DOC_LINK_RE = re.compile(r"https://docs\.google\.com/document/d/([\w-]+)/")

def drive_client():
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
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def html_to_md(html: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.ignore_images = True
    h.body_width = 0
    md = h.handle(html)
    return re.sub(r"\n{3,}", "\n\n", md).strip()

def save_md(file_id: str, title: str, url: str, md: str):
    path = RAW / f"{file_id}.md"
    path.write_text(md, encoding="utf-8")
    meta_path = STORAGE / "meta.json"
    meta = json.loads(meta_path.read_text("utf-8")) if meta_path.exists() else {}
    meta[file_id] = {"title": title, "url": url, "path": str(path)}
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), "utf-8")

def export_google_doc_as_md(drive, file):
    # Google Документ -> HTML -> MD
    data = drive.files().export(fileId=file["id"], mimeType="text/html").execute()
    html = data.decode("utf-8") if isinstance(data, (bytes, bytearray)) else data
    title = file["name"]
    url = drive.files().get(fileId=file["id"], fields="webViewLink").execute()["webViewLink"]
    md = html_to_md(html)
    save_md(file["id"], title, url, md)
    # добираем документы по внутренним ссылкам
    for lid in set(m.group(1) for m in DOC_LINK_RE.finditer(html)):
        try:
            sub = drive.files().get(fileId=lid, fields="id,name,mimeType,webViewLink").execute()
            export_google_doc_as_md(drive, sub)
        except HttpError:
            pass

def download_file(drive, file_id: str) -> bytes:
    req = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    buf.seek(0)
    return buf.read()

def parse_docx(content: bytes) -> str:
    import docx
    from tempfile import NamedTemporaryFile
    with NamedTemporaryFile(suffix=".docx") as tmp:
        tmp.write(content); tmp.flush()
        d = docx.Document(tmp.name)
    paras = []
    for p in d.paragraphs:
        if p.text.strip():
            paras.append(p.text.strip())
    return "\n\n".join(paras)

def parse_pdf(content: bytes) -> str:
    from tempfile import NamedTemporaryFile
    from pdfminer.high_level import extract_text
    with NamedTemporaryFile(suffix=".pdf") as tmp:
        tmp.write(content); tmp.flush()
        return extract_text(tmp.name)

def walk_folder(drive, folder_id: str, visited_folders: Set[str]):
    if folder_id in visited_folders:
        return
    visited_folders.add(folder_id)

    page_token = None
    while True:
        resp = drive.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id,name,mimeType,shortcutDetails,webViewLink)",
            pageToken=page_token
        ).execute()
        files = resp.get("files", [])
        for f in files:
            mime = f.get("mimeType", "")
            name = f.get("name", "")
            log.info("Найдено: %s (%s) [%s]", name, f["id"], mime)

            try:
                if mime == "application/vnd.google-apps.folder":
                    # рекурсивно в подпапку
                    walk_folder(drive, f["id"], visited_folders)

                elif mime == "application/vnd.google-apps.shortcut":
                    # переходим по ярлыку
                    target_id = f["shortcutDetails"]["targetId"]
                    target_mime = f["shortcutDetails"]["targetMimeType"]
                    log.info("  Ярлык -> %s (%s)", target_id, target_mime)
                    if target_mime == "application/vnd.google-apps.folder":
                        walk_folder(drive, target_id, visited_folders)
                    else:
                        handle_file(drive, {"id": target_id, "name": name, "mimeType": target_mime})

                else:
                    handle_file(drive, f)

            except HttpError as e:
                log.error("Ошибка доступа к %s: %s", f["id"], e)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

def handle_file(drive, f: Dict):
    mime = f["mimeType"]
    name = f["name"]
    fid  = f["id"]
    url  = drive.files().get(fileId=fid, fields="webViewLink").execute().get("webViewLink","")

    if mime == "application/vnd.google-apps.document":
        export_google_doc_as_md(drive, f)
        return

    if mime in ("application/pdf", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"):
        log.info("  Скачиваю %s (%s)", name, mime)
        content = download_file(drive, fid)
        text = parse_pdf(content) if mime == "application/pdf" else parse_docx(content)
        if not text.strip():
            log.warning("  Пустой текст у %s", name)
            return
        md = f"# {name}\n\n{text}"
        save_md(fid, name, url, md)
        return

    # Остальные типы пока пропустим (XLSX, SLIDES и т.п.)
    log.info("  Пропуск типа %s", mime)

def main():
    folder_id = os.getenv("ROOT_FOLDER_ID")
    if not folder_id:
        raise RuntimeError("В .env добавьте ROOT_FOLDER_ID (ID корневой папки)")

    drive = drive_client()
    walk_folder(drive, folder_id, set())
    log.info("Готово. Проверьте storage/raw_docs и обновите индекс.")

if __name__ == "__main__":
    main()
