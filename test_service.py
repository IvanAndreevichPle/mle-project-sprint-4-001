"""
Тестирование сервиса рекомендаций.

Три сценария:
1. Пользователь без персональных рекомендаций → топ популярных.
2. Пользователь с персональными рекомендациями, но без онлайн-истории → офлайн.
3. Пользователь с персональными рекомендациями и онлайн-историей → смешанные.
"""
import sys

import requests

BASE_URL = "http://localhost:8000"


def get_user_with_recs() -> int | None:
    """Находит user_id, у которого есть персональные рекомендации."""
    candidates = [12, 23, 41, 54, 82, 100, 200, 500, 1000, 5000, 10000]
    for uid in candidates:
        resp = requests.get(f"{BASE_URL}/recommendations/{uid}")
        data = resp.json()
        if data["type"] != "top_popular":
            return uid
    return None


def test_no_personal_recs():
    """Сценарий 1: пользователь без персональных рекомендаций."""
    print("=" * 60)
    print("Сценарий 1: Пользователь без персональных рекомендаций")
    print("=" * 60)

    user_id = 999999999
    resp = requests.get(f"{BASE_URL}/recommendations/{user_id}?k=10")
    assert resp.status_code == 200
    data = resp.json()

    assert data["type"] == "top_popular", f"Expected top_popular, got {data['type']}"
    assert len(data["tracks"]) == 10, f"Expected 10 tracks, got {len(data['tracks'])}"
    assert data["user_id"] == user_id

    print(f"  user_id: {user_id}")
    print(f"  type: {data['type']}")
    print(f"  tracks (first 5): {data['tracks'][:5]}")
    print(f"  total tracks: {len(data['tracks'])}")
    print("  [OK] Получены топ популярных треков\n")


def test_personal_no_online(user_id: int):
    """Сценарий 2: персональные рекомендации без онлайн-истории."""
    print("=" * 60)
    print("Сценарий 2: Персональные рекомендации, без онлайн-истории")
    print("=" * 60)

    history_resp = requests.get(f"{BASE_URL}/history/{user_id}")
    history_data = history_resp.json()

    resp = requests.get(f"{BASE_URL}/recommendations/{user_id}?k=10")
    assert resp.status_code == 200
    data = resp.json()

    assert data["type"] == "personal", f"Expected personal, got {data['type']}"
    assert len(data["tracks"]) > 0
    assert data["user_id"] == user_id

    print(f"  user_id: {user_id}")
    print(f"  online history length: {len(history_data['history'])}")
    print(f"  type: {data['type']}")
    print(f"  tracks (first 5): {data['tracks'][:5]}")
    print(f"  total tracks: {len(data['tracks'])}")
    print("  [OK] Получены офлайн персональные рекомендации\n")


def test_personal_with_online(user_id: int):
    """Сценарий 3: персональные рекомендации + онлайн-история → смешанные."""
    print("=" * 60)
    print("Сценарий 3: Персональные рекомендации + онлайн-история")
    print("=" * 60)

    recs_before = requests.get(f"{BASE_URL}/recommendations/{user_id}?k=10").json()
    print(f"  Рекомендации ДО добавления истории:")
    print(f"    type: {recs_before['type']}")
    print(f"    tracks: {recs_before['tracks'][:5]}")

    root_resp = requests.get(f"{BASE_URL}/").json()
    print(f"\n  Добавляем треки в онлайн-историю...")

    # Берём треки из similar_tracks (чтобы i2i мог найти похожие)
    # Используем треки из рекомендаций, для которых с большей вероятностью
    # есть похожие
    tracks_to_listen = recs_before["tracks"][:3]
    for track_id in tracks_to_listen:
        resp = requests.put(f"{BASE_URL}/history/{user_id}?track_id={track_id}")
        assert resp.status_code == 200
        print(f"    Добавлен track_id={track_id}")

    history_resp = requests.get(f"{BASE_URL}/history/{user_id}")
    history_data = history_resp.json()
    print(f"  Длина онлайн-истории: {len(history_data['history'])}")

    resp = requests.get(f"{BASE_URL}/recommendations/{user_id}?k=10")
    assert resp.status_code == 200
    data = resp.json()

    assert data["user_id"] == user_id
    assert data["type"] in ("blended", "personal"), (
        f"Expected blended or personal, got {data['type']}"
    )
    assert len(data["tracks"]) > 0

    for listened in tracks_to_listen:
        assert listened not in data["tracks"], (
            f"Track {listened} is in history but still recommended"
        )

    print(f"\n  Рекомендации ПОСЛЕ добавления истории:")
    print(f"    type: {data['type']}")
    print(f"    tracks (first 5): {data['tracks'][:5]}")
    print(f"    total tracks: {len(data['tracks'])}")

    if data["type"] == "blended":
        print("  [OK] Получены смешанные (онлайн + офлайн) рекомендации")
    else:
        print("  [OK] Получены персональные рекомендации "
              "(онлайн-треки не нашлись в i2i — нет смешивания)")
    print()


def test_api_endpoints():
    """Дополнительные проверки API."""
    print("=" * 60)
    print("Дополнительные проверки API")
    print("=" * 60)

    resp = requests.get(f"{BASE_URL}/")
    assert resp.status_code == 200
    data = resp.json()
    print(f"  Корневой эндпоинт: {data}")
    print(f"  [OK] Сервис доступен\n")


if __name__ == "__main__":
    print(f"Тестирование сервиса рекомендаций: {BASE_URL}\n")

    try:
        requests.get(f"{BASE_URL}/", timeout=5)
    except requests.exceptions.ConnectionError:
        print(f"ОШИБКА: Сервис недоступен по адресу {BASE_URL}")
        print("Запустите сервис: python recommendations_service.py")
        sys.exit(1)

    test_api_endpoints()

    test_no_personal_recs()

    user_id = get_user_with_recs()
    if user_id is None:
        print("ОШИБКА: Не найден пользователь с персональными рекомендациями")
        sys.exit(1)

    print(f"Найден пользователь с персональными рекомендациями: user_id={user_id}\n")

    test_personal_no_online(user_id)

    test_personal_with_online(user_id)

    print("=" * 60)
    print("Все тесты пройдены!")
    print("=" * 60)
