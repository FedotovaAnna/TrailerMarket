import os
import time
import requests
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


def get_mongo_collection(collection_name):
    """
    Подключение к MongoDB и возвращение коллекции.
    URI и пароль берутся из переменных окружения.
    """
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi("1"))
    client.admin.command("ping")
    print(f"✅ Подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]


def load_trailers_by_bodystyle(
        collection,
        bodystyles,
        group_name="trailers",
        year=2023,             # по умолчанию 2023
        quarter=None,
        month_range=None,
        log_col=None,
        max_pages=3,
        delay=0.5
):
    """
    Загрузка данных по списку bodystyles за указанный год и квартал.
    Пишет логи в коллекцию log_col (MongoDB).

    Аргументы:
    - collection: коллекция для данных (MongoDB)
    - bodystyles: список id bodystyle для загрузки
    - group_name: "trailers" или "semitrailers"
    - year: год загрузки (int)
    - quarter: квартал (1..4) или None
    - month_range: кортеж (start_month, end_month), если None — загружаем по годам
    - log_col: коллекция MongoDB для логов
    - max_pages: максимальное число страниц с API
    - delay: задержка между запросами (секунды)
    """
    api_key = os.environ.get("api_key_ria")
    if not api_key:
        raise ValueError("❌ api_key_ria не найден в переменных окружения")

    start_time = time.time()
    total_loaded = 0

    # Определяем параметры периода
    s_yers = year
    po_yers = year

    print(f"📅 Загрузка {group_name} за {year}, квартал {quarter if quarter else 'весь год'}")

    for body_id in bodystyles:
        loaded = 0
        seen_ids = set()
        print(f"🔍 Загрузка bodystyle {body_id}")

        for page in range(max_pages):
            params = {
                "api_key": api_key,
                "category_id": 5,
                "bodystyle": body_id,
                "s_yers": s_yers,
                "po_yers": po_yers,
                "with_price": 1,
                "countpage": 20,
                "page": page
            }
            r = requests.get("https://developers.ria.com/auto/search", params=params)
            if r.status_code != 200:
                print(f"⚠️ Ошибка запроса search: {r.status_code}")
                break

            ids = r.json().get("result", {}).get("search_result", {}).get("ids", [])
            if not ids:
                print("🚫 Нет данных на странице, переходим к следующему bodystyle")
                break

            for auto_id in ids:
                if auto_id in seen_ids:
                    continue
                info_resp = requests.get("https://developers.ria.com/auto/info",
                                         params={"api_key": api_key, "auto_id": auto_id})
                if info_resp.status_code == 200:
                    doc = info_resp.json()
                    doc["_id"] = int(auto_id)
                    doc["source"] = "AUTO.RIA"
                    doc["country"] = "UA"  # Добавляем страну
                    doc["ingested_at"] = datetime.utcnow()
                    doc["year_loaded"] = year
                    doc["quarter_loaded"] = quarter
                    try:
                        collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
                        loaded += 1
                    except Exception as e:
                        print(f"⚠️ Ошибка записи ID {auto_id}: {e}")
                else:
                    print(f"⚠️ Ошибка запроса info для ID {auto_id}: {info_resp.status_code}")
                time.sleep(delay)
                seen_ids.add(auto_id)

        print(f"📦 Загружено {loaded} для bodystyle {body_id}")
        total_loaded += loaded

    elapsed = time.time() - start_time
    print(f"✅ Всего загружено {total_loaded} записей за {elapsed:.1f} секунд")

    # Записываем лог в MongoDB
    if log_col is not None:
        log_doc = {
            "group_name": group_name,
            "year": year,
            "quarter": quarter,
            "month_range": month_range,
            "total_loaded": total_loaded,
            "elapsed_seconds": elapsed,
            "source": "RIA_UA",
            "country": "UA",
            "loaded_at": datetime.utcnow()
        }
        try:
            log_col.insert_one(log_doc)
            print("📝 Лог загрузки записан в MongoDB")
        except Exception as e:
            print(f"⚠️ Ошибка записи лога: {e}")
