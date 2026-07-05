"""Tag-similarity and personalised recommendation behavior.

Fixture shape (see conftest): Fantasy is a common tag, Regression rare.
Novel 1+2 share both; 5 is the adult twin; 6 has no tags; 7 is a
mega-popular Fantasy-only title that must NOT outrank a rare-tag match.
"""

PRIVATE_FIELDS = ("tg_link", "raw_tg_link", "local_folder")


def ids(payload):
    return [n["id"] for n in payload["novels"]]


def test_similar_ranks_rare_shared_tags_first(admin_client):
    r = admin_client.get("/api/novel/1/similar")
    assert r.status_code == 200
    data = r.get_json()
    assert data["basis"] == "tags"
    got = ids(data)
    assert got[0] == 2                     # shares Regression+Fantasy
    assert 1 not in got                    # never recommends the seed itself
    assert 5 not in got                    # adult title, non-adult seed
    assert 4 not in got                    # no shared tags at all
    assert got.index(2) < got.index(7)     # rare tag beats 99999 likes
    scores = [n["score"] for n in data["novels"]]
    assert scores == sorted(scores, reverse=True)
    for novel in data["novels"]:
        assert all(field not in novel for field in PRIVATE_FIELDS)


def test_similar_adult_seed_may_include_non_adult(admin_client):
    data = admin_client.get("/api/novel/5/similar").get_json()
    assert data["basis"] == "tags"
    assert {1, 2} <= set(ids(data))


def test_similar_author_fallback_when_no_tags(admin_client):
    data = admin_client.get("/api/novel/6/similar").get_json()
    assert data["basis"] == "author"
    assert ids(data) == [1, 4]             # AuthA's other novels, by likes


def test_similar_limit_clamped(admin_client):
    assert len(ids(admin_client.get("/api/novel/1/similar?limit=1").get_json())) == 1
    big = admin_client.get("/api/novel/1/similar?limit=9999").get_json()
    assert len(ids(big)) <= 24


def test_similar_unknown_novel_404(admin_client):
    assert admin_client.get("/api/novel/zzz-missing/similar").status_code == 404
    assert admin_client.get("/api/novel/zzz-missing").status_code == 404


def test_novel_detail_includes_user_record(user_client):
    r = user_client.get("/api/novel/1")
    assert r.status_code == 200
    data = r.get_json()
    assert data["novel"]["id"] == 1
    assert all(field not in data["novel"] for field in PRIVATE_FIELDS)
    assert data["user_record"]["status"] == "finished"


def test_recommendations_from_tag_profile(user_client):
    # u1 finished novel 1 (Regression+Fantasy)
    data = user_client.get("/api/recommendations").get_json()
    assert data["basis"] == "tags"
    got = ids(data)
    assert got[0] == 2                     # best profile match
    assert 1 not in got                    # already in the user's list
    assert 5 not in got                    # no adult titles in profile


def test_recommendations_popular_fallback_for_new_user(fresh_client):
    data = fresh_client.get("/api/recommendations").get_json()
    assert data["basis"] == "popular"
    got = ids(data)
    assert got[0] == 7                     # most-liked title
    assert 5 not in got                    # adult excluded for a blank profile
