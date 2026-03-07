"""
Сервис рекомендаций для Яндекс Музыки.

Стратегия смешивания онлайн- и офлайн-рекомендаций:
- Офлайн: предрассчитанные персональные рекомендации (ALS + ранжирование CatBoost),
  загружаются из S3 при старте сервиса.
- Онлайн: на основе последних прослушанных треков пользователя (онлайн-история)
  подбираются похожие треки (i2i), рассчитанные через ALS.
- Смешивание: чередование (interleaving) — онлайн-рекомендации на нечётных позициях,
  офлайн на чётных. Дубликаты и уже прослушанные треки исключаются.
- Для пользователей без персональных рекомендаций — fallback на топ популярных.
"""
import io
import logging
import os
from collections import defaultdict

import boto3
import pandas as pd
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get('S3_BUCKET_NAME', 's3-student-mle-20251116-8842c990e3-freetrack')
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', 'https://storage.yandexcloud.net')

app = FastAPI(title="Recommendations Service", version="1.0.0")

personal_recs: dict[int, list[int]] = {}
top_popular: list[int] = []
similar_tracks: dict[int, list[int]] = {}

# Онлайн-история: user_id -> список последних прослушанных track_id
user_online_history: dict[int, list[int]] = defaultdict(list)

MAX_ONLINE_HISTORY = 20


def load_parquet_from_s3(key: str) -> pd.DataFrame:
    s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT,
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(resp['Body'].read()))


@app.on_event("startup")
def load_data():
    global personal_recs, top_popular, similar_tracks

    logger.info("Loading recommendations from S3...")

    recs_df = load_parquet_from_s3('recsys/recommendations/recommendations.parquet')
    personal_recs.update(
        recs_df.sort_values(['user_id', 'rank'])
        .groupby('user_id')['track_id']
        .apply(list)
        .to_dict()
    )
    logger.info(f"Personal recs: {len(personal_recs)} users")

    top_df = load_parquet_from_s3('recsys/recommendations/top_popular.parquet')
    top_popular.extend(top_df.sort_values('rank')['track_id'].tolist())
    logger.info(f"Top popular: {len(top_popular)} tracks")

    sim_df = load_parquet_from_s3('recsys/recommendations/similar.parquet')
    similar_tracks.update(
        sim_df.sort_values(['track_id', 'score'], ascending=[True, False])
        .groupby('track_id')['similar_track_id']
        .apply(list)
        .to_dict()
    )
    logger.info(f"Similar tracks: {len(similar_tracks)} source tracks")
    logger.info("All data loaded!")


def get_online_recs(user_id: int, exclude: set[int], k: int) -> list[int]:
    """Онлайн-рекомендации на основе последних прослушанных треков (i2i)."""
    history = user_online_history.get(user_id, [])
    if not history:
        return []

    online = []
    seen = set(exclude)
    for track_id in reversed(history):
        if track_id in similar_tracks:
            for sim_id in similar_tracks[track_id]:
                if sim_id not in seen:
                    online.append(sim_id)
                    seen.add(sim_id)
                    if len(online) >= k:
                        return online
    return online


def blend_recommendations(offline: list[int], online: list[int],
                          exclude: set[int], k: int) -> list[int]:
    """Смешивание офлайн и онлайн рекомендаций чередованием."""
    result = []
    seen = set(exclude)
    i_off, i_on = 0, 0

    while len(result) < k:
        # Нечётные позиции (1, 3, 5...) — онлайн, чётные (0, 2, 4...) — офлайн
        if len(result) % 2 == 1 and i_on < len(online):
            candidate = online[i_on]
            i_on += 1
        elif i_off < len(offline):
            candidate = offline[i_off]
            i_off += 1
        elif i_on < len(online):
            candidate = online[i_on]
            i_on += 1
        else:
            break

        if candidate not in seen:
            result.append(candidate)
            seen.add(candidate)

    return result


@app.get("/")
def root():
    return {
        "service": "Recommendations Service",
        "users_with_personal_recs": len(personal_recs),
        "top_popular_count": len(top_popular),
        "similar_tracks_count": len(similar_tracks),
        "users_with_online_history": len(user_online_history)
    }


@app.put("/history/{user_id}")
def add_to_history(user_id: int, track_id: int):
    """Добавить трек в онлайн-историю пользователя."""
    history = user_online_history[user_id]
    history.append(track_id)
    if len(history) > MAX_ONLINE_HISTORY:
        user_online_history[user_id] = history[-MAX_ONLINE_HISTORY:]
    return {"user_id": user_id, "history_length": len(user_online_history[user_id])}


@app.get("/history/{user_id}")
def get_history(user_id: int):
    """Получить онлайн-историю пользователя."""
    return {"user_id": user_id, "history": user_online_history.get(user_id, [])}


@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: int, k: int = 10):
    """
    Получить рекомендации для пользователя.

    Логика:
    1. Если нет персональных рекомендаций — возвращаем топ популярных.
    2. Если есть персональные, но нет онлайн-истории — возвращаем офлайн (персональные).
    3. Если есть и персональные, и онлайн-история — смешиваем онлайн и офлайн.
    """
    history_set = set(user_online_history.get(user_id, []))

    # Офлайн-рекомендации
    if user_id in personal_recs:
        offline = personal_recs[user_id]
    else:
        return {
            "user_id": user_id,
            "type": "top_popular",
            "tracks": top_popular[:k]
        }

    # Онлайн-рекомендации из i2i
    online = get_online_recs(user_id, exclude=history_set, k=k)

    if not online:
        # Нет онлайн-истории — только офлайн
        tracks = [t for t in offline if t not in history_set][:k]
        return {
            "user_id": user_id,
            "type": "personal",
            "tracks": tracks
        }

    # Смешиваем
    tracks = blend_recommendations(offline, online, exclude=history_set, k=k)
    return {
        "user_id": user_id,
        "type": "blended",
        "tracks": tracks
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
