"""
Сервис рекомендаций для Яндекс Музыки.
Загружает предрассчитанные рекомендации из S3 и отдаёт их по HTTP-запросам.
"""
import io
import logging
import os

import boto3
import pandas as pd
from fastapi import FastAPI, HTTPException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get('S3_BUCKET_NAME', 's3-student-mle-20251116-8842c990e3-freetrack')
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', 'https://storage.yandexcloud.net')

app = FastAPI(title="Recommendations Service", version="1.0.0")

recommendations = {}
top_popular = []
similar_tracks = {}


def load_parquet_from_s3(key: str) -> pd.DataFrame:
    s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT,
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
    buf = io.BytesIO(resp['Body'].read())
    return pd.read_parquet(buf)


@app.on_event("startup")
def load_data():
    global recommendations, top_popular, similar_tracks

    logger.info("Loading recommendations from S3...")

    # Персональные рекомендации (итоговые, после ранжирования)
    recs_df = load_parquet_from_s3('recsys/recommendations/recommendations.parquet')
    recommendations = (
        recs_df.sort_values(['user_id', 'rank'])
        .groupby('user_id')['track_id']
        .apply(list)
        .to_dict()
    )
    logger.info(f"Personal recs: {len(recommendations)} users")

    # Топ популярных (fallback)
    top_df = load_parquet_from_s3('recsys/recommendations/top_popular.parquet')
    top_popular = top_df.sort_values('rank')['track_id'].tolist()
    logger.info(f"Top popular: {len(top_popular)} tracks")

    # Похожие треки
    sim_df = load_parquet_from_s3('recsys/recommendations/similar.parquet')
    similar_tracks = (
        sim_df.sort_values(['track_id', 'score'], ascending=[True, False])
        .groupby('track_id')['similar_track_id']
        .apply(list)
        .to_dict()
    )
    logger.info(f"Similar tracks: {len(similar_tracks)} source tracks")
    logger.info("All data loaded!")


@app.get("/")
def root():
    return {
        "service": "Recommendations Service",
        "users_with_recs": len(recommendations),
        "top_popular_count": len(top_popular),
        "similar_tracks_count": len(similar_tracks)
    }


@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: int, k: int = 10):
    """Персональные рекомендации для пользователя. Fallback на топ популярных."""
    if user_id in recommendations:
        tracks = recommendations[user_id][:k]
        rtype = "personal"
    else:
        tracks = top_popular[:k]
        rtype = "top_popular"

    return {"user_id": user_id, "type": rtype, "tracks": tracks}


@app.get("/similar/{track_id}")
def get_similar(track_id: int, k: int = 10):
    """Похожие треки для данного трека."""
    if track_id not in similar_tracks:
        raise HTTPException(status_code=404, detail=f"No similar tracks for {track_id}")

    return {"track_id": track_id, "similar": similar_tracks[track_id][:k]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
