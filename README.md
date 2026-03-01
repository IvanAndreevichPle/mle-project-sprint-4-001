# Рекомендательная система для Яндекс Музыки

## Описание

Прототип персональной рекомендательной системы для музыкального стримингового сервиса. Система использует данные о прослушиваниях ~1.4M пользователей и 1M треков для генерации рекомендаций трёх типов:

- **Топ популярных** — базовый baseline
- **Персональные ALS** — collaborative filtering (implicit)
- **Итоговые** — с ранжирующей моделью CatBoost (3 признака)

Дополнительно рассчитаны похожие треки (i2i) для онлайн-рекомендаций.

## Подготовка виртуальной машины

### Склонируйте репозиторий

```bash
git clone git@github.com:IvanAndreevichPle/mle-project-sprint-4-001.git
cd mle-project-sprint-4-001
```

### Активируйте виртуальное окружение

```bash
python3 -m venv env_recsys_start
. env_recsys_start/bin/activate
pip install -r requirements.txt
```

### Скачайте файлы с данными

```bash
wget https://storage.yandexcloud.net/mle-data/ym/tracks.parquet
wget https://storage.yandexcloud.net/mle-data/ym/catalog_names.parquet
wget https://storage.yandexcloud.net/mle-data/ym/interactions.parquet
```

## Расчёт рекомендаций

Код для выполнения первой части проекта находится в файле `recommendations.ipynb`. Для запуска:

```bash
jupyter lab --ip=0.0.0.0 --no-browser
```

Откройте ноутбук и выполните все ячейки последовательно. Результаты (parquet-файлы) автоматически сохраняются в S3-бакет.

## Сервис рекомендаций

Код сервиса рекомендаций находится в файле `recommendations_service.py`.

### Запуск сервиса

```bash
python recommendations_service.py
```

Сервис запустится на `http://localhost:8000`.

### API endpoints

- `GET /` — информация о сервисе
- `GET /recommendations/{user_id}?k=10` — персональные рекомендации (fallback на топ популярных)
- `GET /similar/{track_id}?k=10` — похожие треки

### Примеры запросов

```bash
curl http://localhost:8000/
curl http://localhost:8000/recommendations/42
curl http://localhost:8000/similar/53404
```

## Инструкции для тестирования сервиса

Код для тестирования сервиса находится в файле `test_service.py`.

```bash
# Сначала запустите сервис (в отдельном терминале)
python recommendations_service.py

# Затем запустите тесты
python test_service.py
```

## Структура проекта

```
├── recommendations.ipynb          # Ноутбук: этапы 1-3
├── recommendations_service.py     # Сервис рекомендаций (этап 4)
├── test_service.py                # Тесты сервиса
├── requirements.txt               # Зависимости
├── .gitignore                     # Исключения для git
└── README.md                      # Документация
```
