"""Auth, headers, payload privacy, input strictness, and sanitizer checks."""
from conftest import FILENAMES

PRIVATE_FIELDS = ("tg_link", "raw_tg_link", "local_folder")


def test_security_headers_on_every_response(anon_client):
    r = anon_client.get("/login")
    assert r.status_code == 200
    assert "Content-Security-Policy" in r.headers
    assert r.headers["X-Frame-Options"] == "DENY"
    assert r.headers["X-Content-Type-Options"] == "nosniff"
    assert "Referrer-Policy" in r.headers


def test_api_requires_auth(anon_client):
    for path in ("/api/library", "/api/collections"):
        r = anon_client.post(path, json={})
        assert r.status_code == 401, path
        assert r.is_json
    assert anon_client.get("/api/recommendations").status_code == 401
    assert anon_client.get("/api/novel/1/similar").status_code == 401


def test_gallery_page_renders_with_detail_modal(admin_client):
    r = admin_client.get("/")
    assert r.status_code == 200
    body = r.data.decode("utf-8")
    assert body.count("</html>") == 1
    assert 'id="detailModal"' in body
    assert "/api/novel/" in body          # detail view wired to the new API


def test_auth_pages_render(app):
    from flask import render_template
    auth_pages = ("login.html", "register.html", "verify.html",
                  "forgot_password.html", "reset_password.html")
    with app.test_request_context("/"):
        for name in auth_pages:
            html = render_template(name, error="boom", email="user@test.local")
            assert html.count("</html>") == 1, name
            assert "Archive<span>DB</span>" in html, name   # shared brand mark
            assert "boom" in html, name                     # error box renders


def test_reader_template_renders(app):
    from flask import render_template
    with app.test_request_context("/"):
        html = render_template("reader.html", novel={"id": "1", "title_en": "T", "filename": "f"},
                               toc=["c1.html"], images=[], server_progress=0)
    assert html.count("</html>") == 1
    assert "endrecs" in html            # end-of-book panel present
    assert "Synced progress" in html    # cross-device resume offer present


def test_library_payload_hides_private_fields(admin_client):
    r = admin_client.post("/api/library", json={"page": 1, "limit": 30})
    data = r.get_json()
    assert r.status_code == 200
    assert data["total"] == len(FILENAMES)
    for novel in data["novels"]:
        assert all(field not in novel for field in PRIVATE_FIELDS)
        assert novel["has_download"] is True


def test_collections_reject_non_json_bodies(admin_client):
    r = admin_client.post("/api/collection_create", data='{"name":"smuggled"}',
                          content_type="application/x-www-form-urlencoded")
    assert r.status_code == 400


def test_edit_authorization(user_client, admin_client):
    matched = FILENAMES[8]
    assert admin_client.post("/api/edit", json={"filename": "nope.epub"}).status_code == 404
    r = user_client.post("/api/edit", json={"filename": matched, "title_en": "Hax"})
    assert r.status_code == 403
    r = admin_client.post("/api/edit", json={"filename": matched, "title_en": "Edited8",
                                             "tags": "", "synopsis": ""})
    assert r.status_code == 200


def test_chapter_sanitizer(g):
    dirty = ('<p onclick="x()">hi</p><script>evil()</script><SCRIPT src=a.js>'
             '<iframe src=x></iframe><a href="javascript:alert(1)">l</a>'
             '<img src="pic.jpg" onerror=hack()>')
    clean = g.sanitize_chapter_html(dirty)
    lowered = clean.lower()
    assert "script" not in lowered
    assert "onclick" not in lowered and "onerror" not in lowered
    assert "javascript:" not in lowered
    assert "iframe" not in lowered
    assert 'src="pic.jpg"' in clean  # formatting markup survives


def test_natural_sort_key_type_stable(g):
    names = ["ch1.html", "extra.html", "ch10.html", "2nd.html", "ch2.html"]
    ordered = sorted(names, key=g.natural_sort_key)
    assert ordered.index("ch2.html") < ordered.index("ch10.html")
