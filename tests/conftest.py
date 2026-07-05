"""Test harness: stages gallery_app + templates into a temp dir that mirrors
the server layout (templates/ subfolder), with all data paths pointed at
fixture files. Nothing under /home/ubuntu is read or written."""
import glob
import json
import os
import shutil
import sys
import tempfile

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGE = tempfile.mkdtemp(prefix="archivedb-tests-")
META = os.path.join(STAGE, "meta")
OUTPUT = os.path.join(STAGE, "output")
os.makedirs(META)
os.makedirs(OUTPUT)
os.makedirs(os.path.join(STAGE, "templates"))

shutil.copy(os.path.join(REPO, "gallery_app.py"), STAGE)
for html in glob.glob(os.path.join(REPO, "*.html")):
    shutil.copy(html, os.path.join(STAGE, "templates"))

# --- Fixture library -------------------------------------------------------
# Tag frequencies are deliberate: Fantasy is common (idf low), Regression is
# rare (idf high), so tests can assert that rare shared tags outrank both
# common tags and raw popularity.
FIXTURE_NOVELS = [
    {"id": 1, "title": "가나다라", "author": "AuthA", "tags": ["Regression", "Fantasy"],
     "likes": 50, "age": 0},
    {"id": 2, "title": "나비꿈", "author": "AuthB", "tags": ["Regression", "Fantasy"],
     "likes": 50, "age": 0},
    {"id": 3, "title": "달빛서", "author": "AuthC", "tags": ["Fantasy"], "likes": 10, "age": 0},
    {"id": 4, "title": "라온길", "author": "AuthA", "tags": ["Romance"], "likes": 20, "age": 0},
    {"id": 5, "title": "마루성", "author": "AuthD", "tags": ["Regression", "Fantasy"],
     "likes": 500, "age": 19},
    {"id": 6, "title": "바다별", "author": "AuthA", "tags": [], "likes": 5, "age": 0},
    {"id": 7, "title": "사자후", "author": "AuthE", "tags": ["Fantasy"], "likes": 99999, "age": 0},
    {"id": 8, "title": "아침놀", "author": "AuthF", "tags": ["Fantasy", "Romance"],
     "likes": 30, "age": 0},
]
for n in FIXTURE_NOVELS:
    n.setdefault("cover", "")
    n.setdefault("synopsis", f"Synopsis {n['id']}")
    n.setdefault("views", 100)
    n.setdefault("chapters", 100)
    n.setdefault("complete", 1)

FILENAMES = {n["id"]: f"Novel{n['id']} ({n['title']}).epub" for n in FIXTURE_NOVELS}

with open(os.path.join(META, "novels_full.json"), "w", encoding="utf-8") as fh:
    json.dump(FIXTURE_NOVELS, fh, ensure_ascii=False)

with open(os.path.join(STAGE, "tracker.csv"), "w", encoding="utf-8") as fh:
    fh.write("filename,tg_link,upload_date\n")
    for n in FIXTURE_NOVELS:
        fh.write(f"{FILENAMES[n['id']]},https://t.me/c/1234567/{n['id']},2026-06-0{n['id']}\n")

with open(os.path.join(META, "allowed_gmails.txt"), "w", encoding="utf-8") as fh:
    fh.write("u1@test.local\nfresh@test.local\n")

# u1 has finished novel 1 -> profile {Regression, Fantasy}, novel 1 saved.
with open(os.path.join(META, "user_data.json"), "w", encoding="utf-8") as fh:
    json.dump({"u1@test.local": {"1": {"status": "finished", "progress": 0}}}, fh)

os.environ.update({
    "FLASK_SECRET_KEY": "test-key",
    "ARCHIVEDB_NO_TELEGRAM": "1",
    "ADMIN_EMAILS": "admin@test.local",
    "META_DIR": META,
    "LOCAL_OUTPUT_DIR": OUTPUT,
    "TRANSLATED_CSV_PATH": os.path.join(STAGE, "tracker.csv"),
    "RAW_MASTER_CSV_PATH": os.path.join(STAGE, "raw.csv"),
    "SESSION_PATH": os.path.join(STAGE, "tg_session"),
    "COOKIE_SECURE": "1",
})

sys.path.insert(0, STAGE)
import gallery_app  # noqa: E402  (env must be set before this import)


@pytest.fixture(scope="session")
def g():
    gallery_app.app.config["TESTING"] = True
    return gallery_app


@pytest.fixture(scope="session")
def app(g):
    return g.app


def _client_for(app, email):
    client = app.test_client()
    with client.session_transaction() as sess:
        sess["user_email"] = email
    return client


@pytest.fixture()
def anon_client(app):
    return app.test_client()


@pytest.fixture()
def admin_client(app):
    return _client_for(app, "admin@test.local")


@pytest.fixture()
def user_client(app):
    return _client_for(app, "u1@test.local")


@pytest.fixture()
def fresh_client(app):
    return _client_for(app, "fresh@test.local")
