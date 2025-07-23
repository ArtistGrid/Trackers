import csv
import os
import re
import requests
import time
import traceback
from io import StringIO
from datetime import datetime
import hashlib
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import urllib.parse
import zipfile
import random
from waybackpy import WaybackMachineSaveAPI

REMOTE_CSV_URL = "https://sheets.artistgrid.cx/artists.csv"
CACHE_FILE = "last_artists.csv"
EXPORT_DIR = "downloads"
SLEEP_INTERVAL_SECONDS = 3600  # 1 hour
HOST = "0.0.0.0"
PORT = 8000

def normalize_artist_name(name):
    name = name.lower()
    name = name.replace("$", "s")        # Replace $ with s first
    return re.sub(r'[^a-z0-9]', '', name)


def sanitize_filename(filename):
    # Replace $ with s first
    filename = filename.replace("$", "s")
    # Remove spaces
    filename = filename.replace(" ", "")
    # Remove all characters except letters, numbers, underscore, dot, dash
    filename = re.sub(r'[^a-zA-Z0-9_.-]', '', filename)
    return filename


def log_down_host(url):
    os.makedirs("host", exist_ok=True)
    with open("host/down.txt", "a", encoding="utf-8") as f:
        f.write(f"{url}\n")

def clean_url(url):
    match = re.search(r"https://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9-_]{44})", url)
    return f"https://docs.google.com/spreadsheets/d/{match.group(1)}/" if match else None

def extract_sheet_id(url):
    match = re.search(r"/d/([a-zA-Z0-9-_]{44})/", url)
    return match.group(1) if match else None

def sha256_of_file(path):
    hash_sha256 = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    except:
        return None

def get_metadata_path(file_path):
    return file_path + ".meta"

def load_metadata(file_path):
    meta_path = get_metadata_path(file_path)
    if not os.path.exists(meta_path):
        return {}
    with open(meta_path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
        return dict(line.split(":", 1) for line in lines if ":" in line)

def save_metadata(file_path, metadata):
    meta_path = get_metadata_path(file_path)
    with open(meta_path, "w", encoding="utf-8") as f:
        for key, value in metadata.items():
            f.write(f"{key}:{value}\n")

def should_archive_today(lastarchive):
    try:
        last_time = datetime.strptime(lastarchive, "%Y-%m-%d")
        return datetime.now().date() > last_time.date()
    except:
        return True

def archive_file(file_path, public_url):
    metadata = load_metadata(file_path)
    sha = sha256_of_file(file_path)
    lastarchive = metadata.get("lastarchive")

    if not should_archive_today(lastarchive):
        print(f"[{datetime.now()}] ⏩ Skipping archive (already done today): {file_path}")
        return

    delay = random.randint(7, 13) * 60
    print(f"[{datetime.now()}] ⏱ Waiting {delay//60} min before archiving: {file_path}")
    time.sleep(delay)

    try:
        print(f"[{datetime.now()}] 🌍 Archiving {public_url}")
        save_api = WaybackMachineSaveAPI(public_url, user_agent="Mozilla/5.0 (Wayback Tracker)")
        archive_url = save_api.save()
        print(f"[{datetime.now()}] ✅ Archived: {archive_url}")

        metadata["sha256"] = sha
        metadata["lastarchive"] = datetime.now().strftime("%Y-%m-%d")
        save_metadata(file_path, metadata)
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Archiving failed for {public_url}: {e}")

def download_exports(sheet_id, artist_dir):
    os.makedirs(artist_dir, exist_ok=True)
    print(f"[{datetime.now()}] 📁 Starting download for sheet ID: {sheet_id} into '{artist_dir}'")

    # XLSX
    xlsx_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    xlsx_path = os.path.join(artist_dir, "spreadsheet.xlsx")
    try:
        print(f"[{datetime.now()}] ⬇️ Attempting XLSX download from: {xlsx_url}")
        r = requests.get(xlsx_url)
        r.raise_for_status()
        with open(xlsx_path, "wb") as f:
            f.write(r.content)
        print(f"[{datetime.now()}] ✓ XLSX downloaded: {xlsx_path} ({len(r.content)} bytes)")
    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ XLSX download failed for {xlsx_path}: {e}")
        print(traceback.format_exc())
        if isinstance(e, requests.exceptions.HTTPError) and r.status_code == 401:
            log_down_host(xlsx_url)

    # ZIP
    zip_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=zip"
    zip_path = os.path.join(artist_dir, "spreadsheet.zip")
    try:
        print(f"[{datetime.now()}] ⬇️ Attempting ZIP download from: {zip_url}")
        r = requests.get(zip_url)
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(r.content)
        print(f"[{datetime.now()}] ✓ ZIP downloaded: {zip_path} ({len(r.content)} bytes)")

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            print(f"[{datetime.now()}] 📦 ZIP opened. Extracting files...")
            for member in zip_ref.namelist():
                original_name = os.path.basename(member)
                if not original_name:
                    continue
                sanitized_name = sanitize_filename(original_name)
                source = zip_ref.open(member)
                target_path = os.path.join(artist_dir, sanitized_name)
                with open(target_path, "wb") as target:
                    with source:
                        data = source.read()
                        target.write(data)
                        print(f"[{datetime.now()}] → Extracted: {target_path} ({len(data)} bytes)")
        print(f"[{datetime.now()}] ✓ ZIP extraction complete for {artist_dir}")

    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ ZIP download or extraction failed for {zip_path}: {e}")
        print(traceback.format_exc())
        if isinstance(e, requests.exceptions.HTTPError) and r.status_code == 401:
            log_down_host(zip_url)

def parse_csv(text):
    reader = csv.DictReader(StringIO(text))
    result = {}
    for row in reader:
        if row.get("Best", "").strip().lower() != "yes":
            continue
        artist = normalize_artist_name(row["Artist Name"])
        url = clean_url(row["URL"])
        if artist and url:
            result[artist] = url
    return result

def save_csv(data, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["artist", "url"])
        for artist, url in data.items():
            writer.writerow([artist, url])

def load_cached_csv(path):
    if not os.path.exists(path):
        return {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["artist"]: row["url"] for row in reader}

def format_timestamp(ts):
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def run_once():
    print(f"[{datetime.now()}] 🔍 Checking for updates...")
    try:
        response = requests.get(REMOTE_CSV_URL)
        response.raise_for_status()
        remote_data = parse_csv(response.text)
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Failed to fetch remote CSV: {e}")
        print(traceback.format_exc())
        return

    cached_data = load_cached_csv(CACHE_FILE)

    to_update = {
        artist: url for artist, url in remote_data.items()
        if artist not in cached_data or cached_data[artist] != url
    }

    if not to_update:
        print(f"[{datetime.now()}] ✅ No updates found.")
    else:
        print(f"[{datetime.now()}] 🔄 {len(to_update)} update(s) found.")

        # Collect all files to archive after all downloads finish
        files_to_archive = []

        for artist, url in to_update.items():
            print(f"[{datetime.now()}] 🎯 Updating: {artist} | URL: {url}")
            sheet_id = extract_sheet_id(url)
            if sheet_id:
                artist_dir = os.path.join(EXPORT_DIR, artist)
                download_exports(sheet_id, artist_dir)

                # Collect files for archiving after all downloads
                for filename in os.listdir(artist_dir):
                    file_path = os.path.join(artist_dir, filename)
                    if os.path.isfile(file_path):
                        public_url = f"https://trackers.artistgrid.cx/downloads/{urllib.parse.quote(artist)}/{urllib.parse.quote(filename)}"
                        files_to_archive.append((file_path, public_url))
            else:
                print(f"[{datetime.now()}] ⚠️ Invalid URL for {artist}: {url}")

        print(f"[{datetime.now()}] ✅ All downloads complete. Starting archiving of {len(files_to_archive)} files.")

        for file_path, public_url in files_to_archive:
            # Start archive threads with ~10 minute delay inside archive_file()
            threading.Thread(target=archive_file, args=(file_path, public_url), daemon=True).start()

    save_csv(remote_data, CACHE_FILE)
    print(f"[{datetime.now()}] 💾 Cache updated.\n")


def fetch_loop():
    print(f"[{datetime.now()}] 🟢 Tracker started. Will fetch every hour.")
    while True:
        run_once()
        print(f"[{datetime.now()}] 💤 Sleeping for {SLEEP_INTERVAL_SECONDS // 60} minutes...\n")
        time.sleep(SLEEP_INTERVAL_SECONDS)

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        raw_path = parsed_path.path.strip("/")
        path = urllib.parse.unquote(raw_path)

        if path == "" or path in ["index", "index.html"]:
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            html = self.build_artist_list_page()
            self.wfile.write(html.encode("utf-8"))
            return

        if path == "down":
            down_file = os.path.join("host", "down.txt")
            if os.path.isfile(down_file):
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(os.path.getsize(down_file)))
                self.end_headers()
                with open(down_file, "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"No 401 errors logged.\n")
            return

        if path and "/" not in path:
            artist = path
            artist_dir = os.path.join(EXPORT_DIR, artist)
            if os.path.isdir(artist_dir):
                self.send_response(200)
                self.send_header("Content-type", "text/html; charset=utf-8")
                self.end_headers()
                html = self.build_artist_files_page(artist, artist_dir)
                self.wfile.write(html.encode("utf-8"))
                return
            else:
                self.send_error(404, "Artist not found")
                return

        if path.startswith("downloads/"):
            decoded_path = os.path.join(".", urllib.parse.unquote(path))
            if os.path.isfile(decoded_path):
                self.send_response(200)
                if decoded_path.endswith(".xlsx"):
                    self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                elif decoded_path.endswith(".html") or decoded_path.endswith(".htm"):
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                else:
                    self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(os.path.getsize(decoded_path)))
                self.end_headers()
                with open(decoded_path, "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.send_error(404, "File not found")
            return

        self.send_error(404, "Not found")

    def build_artist_list_page(self):
        artists = sorted(os.listdir(EXPORT_DIR)) if os.path.exists(EXPORT_DIR) else []
        html = [
            "<!DOCTYPE html><html><head><meta charset='utf-8'><title>Artists</title>",
            "<style>body { font-family: monospace; background:#111; color:#eee; padding:20px; }",
            "a { color: #6cf; text-decoration:none; } a:hover { text-decoration: underline; }</style>",
            "</head><body><h1>Artists</h1>"
        ]
        if not artists:
            html.append("<p>No artists found.</p>")
        else:
            html.append("<ul>")
            for artist in artists:
                if os.path.isdir(os.path.join(EXPORT_DIR, artist)):
                    html.append(f"<li><a href='/{urllib.parse.quote(artist)}/'>{artist}</a></li>")
            html.append("</ul>")
        html.append("</body></html>")
        return "\n".join(html)

    def build_artist_files_page(self, artist, artist_dir):
        files = sorted(os.listdir(artist_dir))
        html = [
            f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{artist} Files</title>",
            "<style>body { font-family: monospace; background:#111; color:#eee; padding:20px; }",
            "a { color: #6cf; text-decoration:none; } a:hover { text-decoration: underline; }</style>",
            "</head><body>",
            f"<h1>Downloads for {artist}</h1><p><a href='/'>← Back to Artists</a></p><ul>"
        ]
        for filename in files:
            if filename.endswith(".meta"):
                continue
            full_path = os.path.join(artist_dir, filename)
            if os.path.isfile(full_path):
                mtime = os.path.getmtime(full_path)
                mtime_str = format_timestamp(mtime)
                filehash = sha256_of_file(full_path) or "N/A"
                file_url = f"/downloads/{urllib.parse.quote(artist)}/{urllib.parse.quote(filename)}"
                html.append(f"<li><a href='{file_url}'>{filename}</a> (Modified: {mtime_str}) SHA256: {filehash}</li>")
        html.append("</ul></body></html>")
        return "\n".join(html)

def start_http_server():
    server = HTTPServer((HOST, PORT), SimpleHTTPRequestHandler)
    print(f"[{datetime.now()}] 🌐 HTTP server started on http://{HOST}:{PORT}")
    server.serve_forever()

def main():
    threading.Thread(target=fetch_loop, daemon=True).start()
    start_http_server()

if __name__ == "__main__":
    main()
