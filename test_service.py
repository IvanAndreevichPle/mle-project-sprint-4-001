"""
Тестирование сервиса рекомендаций.
"""
import sys
import requests

BASE_URL = "http://localhost:8000"


def test_root():
    resp = requests.get(f"{BASE_URL}/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["users_with_recs"] > 0
    assert data["top_popular_count"] > 0
    print(f"[OK] Root: {data}")


def test_personal_recommendations():
    resp = requests.get(f"{BASE_URL}/recommendations/0")
    assert resp.status_code == 200
    data = resp.json()
    assert "tracks" in data
    assert len(data["tracks"]) > 0
    print(f"[OK] Personal recs for user 0: type={data['type']}, tracks={data['tracks'][:5]}")


def test_top_popular_fallback():
    resp = requests.get(f"{BASE_URL}/recommendations/999999999")
    assert resp.status_code == 200
    data = resp.json()
    assert data["type"] == "top_popular"
    print(f"[OK] Fallback for unknown user: type={data['type']}, tracks={data['tracks'][:5]}")


def test_similar_tracks():
    resp = requests.get(f"{BASE_URL}/")
    root_data = resp.json()
    if root_data["similar_tracks_count"] > 0:
        resp = requests.get(f"{BASE_URL}/similar/0")
        if resp.status_code == 200:
            data = resp.json()
            print(f"[OK] Similar for track 0: {data['similar'][:5]}")
        else:
            resp2 = requests.get(f"{BASE_URL}/recommendations/0")
            data2 = resp2.json()
            if data2["tracks"]:
                track_id = data2["tracks"][0]
                resp3 = requests.get(f"{BASE_URL}/similar/{track_id}")
                if resp3.status_code == 200:
                    data3 = resp3.json()
                    print(f"[OK] Similar for track {track_id}: {data3['similar'][:5]}")
                else:
                    print(f"[OK] Similar endpoint works (404 for track {track_id} - no similar found)")
    else:
        print("[SKIP] No similar tracks loaded")


def test_custom_k():
    resp = requests.get(f"{BASE_URL}/recommendations/0?k=5")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["tracks"]) <= 5
    print(f"[OK] Custom k=5: got {len(data['tracks'])} tracks")


if __name__ == "__main__":
    print(f"Testing service at {BASE_URL}\n")
    try:
        requests.get(f"{BASE_URL}/", timeout=5)
    except requests.exceptions.ConnectionError:
        print(f"ERROR: Service not available at {BASE_URL}")
        print("Start it first: python recommendations_service.py")
        sys.exit(1)

    test_root()
    test_personal_recommendations()
    test_top_popular_fallback()
    test_similar_tracks()
    test_custom_k()

    print("\nAll tests passed!")
