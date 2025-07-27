!pip install pymongo

!pip install python-dotenv

#тест піідключення до бази SQL

import os
from dotenv import load_dotenv
import psycopg2

# Загружаем переменные окружения
load_dotenv()

# Читаем переменные
host = os.getenv("supabase_host")
user = os.getenv("supabase_user")
password = os.getenv("supabase_password")
dbname = os.getenv("supabase_name")
port = os.getenv("supabase_port", 5432)

# Отладочный вывод
print(f"host={host}")
print(f"user={user}")
print(f"password={'***' if password else None}")
print(f"dbname={dbname}")
print(f"port={port}")

# Пробуем подключиться
try:
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        dbname=dbname,
        port=port
    )
    print("✅ Подключение успешно!")
    conn.close()
except Exception as e:
    print(f"❌ Ошибка подключения: {e}")

#спроба тестовання вставки даних з монго до SQL

import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import psycopg2
from psycopg2.extras import execute_values

# Подключение к MongoDB
db_password = os.environ['db_password']
uri = f"mongodb+srv://afedotova1974:{db_password}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client["trailer"]
collection = db["ria_semitrailers_raw"]

# Подключение к Postgres (Supabase)
pg_host = os.environ['supabase_host']
pg_user = os.environ['supabase_user']
pg_password = os.environ['supabase_password']
pg_dbname = os.environ['supabase_name']
pg_port = os.environ.get('supabase_port', 5432)

conn = psycopg2.connect(
    host=pg_host,
    user=pg_user,
    password=pg_password,
    dbname=pg_dbname,
    port=pg_port
)

def safe_float(val):
    try:
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return float(val)
        # Убираем пробелы и запятые в числе, если есть
        return float(str(val).replace(' ', '').replace(',', '.'))
    except:
        return None

def transform_doc(doc):
    brand = doc.get('dealer', {}).get('markName')
    if not brand:
        brand = 'Unknown'

    model = doc.get('modelName')
    if not model:
        model = 'Unknown'

    year = doc.get('year_loaded')
    if year:
        try:
            year = int(year)
        except:
            year = None
    else:
        year = None

    price_usd = safe_float(doc.get('USD'))
    price_eur = safe_float(doc.get('EUR'))

    country = doc.get('country') or 'Unknown'
    city = doc.get('locationCityName') or ''

    sub_category = doc.get('subCategoryName') or ''

    update_date = doc.get('updateDate')

    return (
        str(doc.get('_id')),  # mongo_id для идентификации
        doc.get('color', {}).get('source', 'auto.ria'),
        brand,
        model,
        year,
        price_usd,
        price_eur,
        country,
        None,  # region (нет данных)
        city,
        None,  # axle_count (нет данных)
        None,  # payload_kg (нет данных)
        None,  # condition (нет данных)
        update_date,
        None,  # mileage_km (нет данных)
        sub_category,
        None,  # geo_lat (нет данных)
        None,  # geo_lon (нет данных)
    )

# Получаем документы из Mongo
docs = list(collection.find().limit(100))

# Подготавливаем данные для вставки
records = [transform_doc(doc) for doc in docs]

insert_query = """
INSERT INTO used_trailers (
    mongo_id, source, brand, model, year, price_usd, price_eur, country, region, city,
    axle_count, payload_kg, condition, listing_date, mileage_km, trailer_type, geo_lat, geo_lon
) VALUES %s
ON CONFLICT (mongo_id) DO NOTHING;
"""

with conn:
    with conn.cursor() as cur:
        execute_values(cur, insert_query, records)
        print(f"Успешно вставлено {len(records)} документов.")

conn.close()


#завантаження всіх актуальних назв типу транспорту

import os
import requests
from pymongo import MongoClient

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def load_categories():
    api_key = os.environ['YOUR_API_KEY']
    url = f"https://developers.ria.com/auto/categories?api_key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    categories = response.json()

    collection = get_mongo_collection("DimCategories")

    # Очистим коллекцию перед загрузкой новых данных
    collection.delete_many({})
    print("❌ Коллекция DimCategories очищена")

    # Запишем новые данные
    if isinstance(categories, list):
        for cat in categories:
            # При необходимости можно сохранить только нужные поля
            document = {
                "value": cat.get("value"),
                "name": cat.get("name")
            }
            collection.insert_one(document)
        print(f"✅ Загружено категорий: {len(categories)}")
    else:
        print("Ошибка: формат данных не список")

if __name__ == "__main__":
    load_categories()
    
    #Рекурсивно распаковываем вложенные списки кузовов.

#К каждому кузову добавляем поле categoryId.

#Загружаем всё в коллекцию DimBodyStyles, предварительно очищая её.

#Выводим информативные сообщения.


import os
import requests
from pymongo import MongoClient

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def flatten_bodystyles(data):
    result = []
    for item in data:
        if isinstance(item, dict):
            result.append(item)
        elif isinstance(item, list):
            # рекурсивно обрабатываем вложенный список
            result.extend(flatten_bodystyles(item))
    return result

def load_bodystyles(category_id, api_key):
    url = f"https://developers.ria.com/auto/categories/{category_id}/bodystyles/_group?api_key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    print(f"Тип возвращаемых данных: {type(data)}")

    flat_list = flatten_bodystyles(data)

    # Добавляем category_id в каждый объект
    for item in flat_list:
        item['categoryId'] = category_id

    print(f"Всего кузовов (после распаковки): {len(flat_list)}")

    # Сохраняем в MongoDB
    collection = get_mongo_collection("DimBodyStyles")
    # Можно сначала очистить коллекцию, если надо:
    collection.delete_many({})
    collection.insert_many(flat_list)

    print(f"✅ Данные по кузовам категории {category_id} загружены в коллекцию DimBodyStyles")

# Пример вызова
if __name__ == "__main__":
    YOUR_API_KEY = os.environ['YOUR_API_KEY']
    category_id = 5  # например, категория прицепы
    load_bodystyles(category_id, YOUR_API_KEY)


#Результат В DimBodyStyleGroups будет таблица групп кузовов.

#В DimBodyStyles — отдельные кузова с указанием группы.

#Можно легко делать join по groupId для формирования иерархии.

#После выгрузки из MongoDB в SQL таблицы, в справочниках будет полный набор кузовов с группами для выбранной категории.



import os
import requests
from pymongo import MongoClient

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

API_KEY = os.environ['YOUR_API_KEY']
BASE_URL = "https://developers.ria.com/auto"

def get_bodystyles_groups(category_id):
    url = f"{BASE_URL}/categories/{category_id}/bodystyles/_group"
    params = {"api_key": API_KEY}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def get_all_bodystyles(category_id):
    url = f"{BASE_URL}/categories/{category_id}/bodystyles"
    params = {"api_key": API_KEY}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def load_bodystyles(category_id=5):
    groups_col = get_mongo_collection("DimBodyStyleGroups")
    styles_col = get_mongo_collection("DimBodyStyles")

    # Очистка коллекций по категории
    groups_col.delete_many({"categoryId": category_id})
    styles_col.delete_many({"categoryId": category_id})

    groups = get_bodystyles_groups(category_id)
    print(f"Найдено групп кузовов: {len(groups)}")

    for g in groups:
        groups_col.insert_one({
            "groupId": g['value'],
            "name": g['name'],
            "categoryId": category_id
        })

    all_styles = get_all_bodystyles(category_id)
    print(f"Всего кузовов: {len(all_styles)}")

    for s in all_styles:
        styles_col.insert_one({
            "bodyStyleId": s['value'],
            "name": s['name'],
            "groupId": s['parentID'],  # 0 — вне группы
            "categoryId": category_id
        })

    print("Данные кузовов загружены и сохранены в MongoDB.")

if __name__ == "__main__":
    load_bodystyles()


#загрузка всех возможніх типов кузовов

import os
import requests
from pymongo import MongoClient
from pymongo.server_api import ServerApi

def get_mongo_collection(collection_name):
    """
    Подключение к MongoDB и возвращение коллекции.
    URI и пароль берутся из переменных окружения.
    """
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi("1"))
    client.admin.command("ping")  # Проверка подключения
    print(f"✅ Подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def load_bodystyles():
    url = "https://api.auto.ria.com/bodystyles"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        collection = get_mongo_collection("DimBodystyles")
        for item in data:
            collection.replace_one(
                {"value": item["value"]},  # уникальный ключ для upsert
                item,
                upsert=True
            )
        print("Данные успешно загружены в MongoDB")
    else:
        print(f"Ошибка загрузки данных: {response.status_code}")

if __name__ == "__main__":
    load_bodystyles()
    
    import os
import requests
from pymongo import MongoClient
from pymongo.server_api import ServerApi

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi("1"))
    client.admin.command("ping")
    print(f"✅ Подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def flatten_data(data):
    """
    Рекурсивно разворачиваем вложенные списки, чтобы получить плоский список словарей.
    """
    result = []
    for item in data:
        if isinstance(item, list):
            result.extend(flatten_data(item))
        else:
            result.append(item)
    return result

def load_bodystyles():
    url = "https://api.auto.ria.com/categories/5/bodystyles/_group"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        flat_data = flatten_data(data)
        collection = get_mongo_collection("bodystyles_group")
        for item in flat_data:
            collection.replace_one(
                {"value": item["value"]},
                item,
                upsert=True
            )
        print("Данные успешно загружены в MongoDB")
    else:
        print(f"Ошибка загрузки данных: {response.status_code}")

if __name__ == "__main__":
    load_bodystyles()



# загрузка всех марок для категории 5
import os
import requests
from pymongo import MongoClient

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def load_marks(category_id, api_key):
    url = f"https://developers.ria.com/auto/categories/{category_id}/marks?api_key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    marks = response.json()

    print(f"Найдено марок: {len(marks)}")

    for mark in marks:
        mark['categoryId'] = category_id

    collection = get_mongo_collection("DimMarks")
    collection.delete_many({})
    collection.insert_many(marks)

    print(f"✅ Коллекция DimMarks обновлена с марками категории {category_id}")

if __name__ == "__main__":
    YOUR_API_KEY = os.environ['YOUR_API_KEY']
    category_id = 5
    load_marks(category_id, YOUR_API_KEY)


# загрузка 

import os
import requests
from pymongo import MongoClient

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def load_models(category_id, api_key):
    # Сначала получаем список всех марок для категории
    marks_collection = get_mongo_collection("DimMarks")
    marks = list(marks_collection.find({"categoryId": category_id}))
    print(f"🔎 Найдено марок для категории {category_id}: {len(marks)}")

    # Подготовка коллекции для моделей
    models_collection = get_mongo_collection("DimModels")
    models_collection.delete_many({})

    models_saved = 0

    for mark in marks:
        mark_id = mark['value']
        mark_name = mark['name']
        url = f"https://developers.ria.com/auto/categories/{category_id}/marks/{mark_id}/models?api_key={api_key}"
        print(f"➡️ Загружаем модели для марки {mark_name} (ID {mark_id})")

        response = requests.get(url)
        if response.status_code == 200:
            models = response.json()
            for model in models:
                model['categoryId'] = category_id
                model['markId'] = mark_id
            if models:
                models_collection.insert_many(models)
                models_saved += len(models)
        else:
            print(f"⚠️ Не удалось загрузить модели для марки {mark_name} (код {response.status_code})")

    print(f"✅ Загружено моделей всего: {models_saved}")

if __name__ == "__main__":
    YOUR_API_KEY = os.environ['YOUR_API_KEY']
    category_id = 5
    load_models(category_id, YOUR_API_KEY)
    
# загрузка країн-виробників
import os
import requests
from pymongo import MongoClient

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def load_countries(api_key):
    countries_collection = get_mongo_collection("DimCountries")
    countries_collection.delete_many({})

    url = f"https://developers.ria.com/auto/countries?api_key={api_key}"
    print(f"➡️ Запрашиваем страны производителей")
    response = requests.get(url)

    if response.status_code == 200:
        countries = response.json()
        if countries:
            countries_collection.insert_many(countries)
            print(f"✅ Загружено стран-производителей: {len(countries)}")
        else:
            print("⚠️ Страны не найдены.")
    else:
        print(f"⚠️ Ошибка запроса (код {response.status_code})")

if __name__ == "__main__":
    YOUR_API_KEY = os.environ['YOUR_API_KEY']
    load_countries(YOUR_API_KEY)
    
# вставка кількості осей
from pymongo import MongoClient
import os

def insert_wheel_formulas():
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    db = client["trailer"]
    collection = db["DimWheelFormulas"]
    
    data = [
        {"wheelFormulaId": 0, "name": "За замовчуванням"},
        {"wheelFormulaId": 1, "name": "4х2"},
        {"wheelFormulaId": 2, "name": "4х4"},
        {"wheelFormulaId": 3, "name": "6х2"},
        {"wheelFormulaId": 4, "name": "6х4"},
        {"wheelFormulaId": 5, "name": "6х6"},
        {"wheelFormulaId": 7, "name": "8х2"},
        {"wheelFormulaId": 8, "name": "8х4"},
        {"wheelFormulaId": 9, "name": "8х6"},
        {"wheelFormulaId": 10, "name": "8х8"},
        {"wheelFormulaId": 11, "name": "10х4"},
        {"wheelFormulaId": 12, "name": "10х8"},
    ]
    
    collection.delete_many({})
    collection.insert_many(data)
    print("✅ Колекція DimWheelFormulas успішно оновлена!")


# Кількість осей → DimAxles
from pymongo import MongoClient
import os

def insert_axles():
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    db = client["trailer"]
    collection = db["DimAxles"]
    
    data = [
        {"axleId": 0, "name": "За замовчуванням"},
        {"axleId": 1, "name": "Одна вісь"},
        {"axleId": 2, "name": "Дві осі"},
        {"axleId": 3, "name": "Три осі"},
        {"axleId": 4, "name": "Чотири осі"},
        {"axleId": 5, "name": "Більше чотирьох осей"},
    ]
    
    collection.delete_many({})
    collection.insert_many(data)
    print("✅ Колекція DimAxles успішно оновлена!")

insert_axles()

#загрузка опций полуприцепов актуальни на укр язіке

import os
import requests
from pymongo import MongoClient

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def load_options(category_id, api_key):
    options_collection = get_mongo_collection("DimOptions")
    options_collection.delete_many({})

    url = f"https://developers.ria.com/auto/categories/{category_id}/options?api_key={api_key}"
    print(f"➡️ Запрашиваем опції для category_id {category_id}")
    response = requests.get(url)

    if response.status_code == 200:
        options = response.json()
        # добавляем categoryId к каждой опции
        for option in options:
            option['categoryId'] = category_id

        if options:
            options_collection.insert_many(options)
            print(f"✅ Загружено опцій: {len(options)}")
        else:
            print("⚠️ Опції не найдены.")
    else:
        print(f"⚠️ Помилка запиту (код {response.status_code})")

if __name__ == "__main__":
    YOUR_API_KEY = os.environ['YOUR_API_KEY']
    category_id = 5
    load_options(category_id, YOUR_API_KEY)


# загрузка категорий всего 10, а наша 5-я, потом ниже будет загрузка типов куозовов для всех 5 категорий

import os
import requests
from pymongo import MongoClient
from pymongo.server_api import ServerApi

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi("1"))
    client.admin.command("ping")
    print(f"✅ Подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def load_categories():
    url = "https://api.auto.ria.com/categories"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        collection = get_mongo_collection("DimCategories")
        for item in data:
            collection.replace_one(
                {"value": item["value"]},  # уникальный ключ
                item,
                upsert=True
            )
        print("Данные DimCategories успешно загружены в MongoDB")
    else:
        print(f"Ошибка загрузки данных: {response.status_code}")

if __name__ == "__main__":
    load_categories()


# загрузка моделей с индексами производителей
import os
import requests
from pymongo import MongoClient
from pymongo.server_api import ServerApi

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi("1"))
    client.admin.command("ping")
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def load_models_by_marks():
    # Получаем список всех марок категории 5 (прицепы)
    marks_url = "https://api.auto.ria.com/categories/5/marks"
    response = requests.get(marks_url)
    if response.status_code != 200:
        print(f"❌ ошибка загрузки марок: {response.status_code}")
        return
    
    marks_data = response.json()
    models_collection = get_mongo_collection("models")

    for mark in marks_data:
        mark_id = mark["value"]
        mark_name = mark["name"]
        
        models_url = f"https://api.auto.ria.com/categories/5/marks/{mark_id}/models"
        models_response = requests.get(models_url)
        
        if models_response.status_code != 200:
            print(f"⚠️ не удалось загрузить модели марки {mark_name} (id={mark_id}), статус {models_response.status_code}")
            continue
        
        models_data = models_response.json()
        print(f"🔹 {mark_name} ({mark_id}): найдено {len(models_data)} моделей")
        
        for model in models_data:
            # добавляем поле mark_id
            model["mark_id"] = mark_id
            models_collection.replace_one(
                {"value": model["value"], "mark_id": mark_id},
                model,
                upsert=True
            )
    
    print("✅ все модели успешно загружены и сохранены")

if __name__ == "__main__":
    load_models_by_marks()
    

# 5.1. створення колекції видів кузовов 39 штук зі збереженням ієрархії та айди категорії 5 родительской
import requests
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os

# ---------- функция подключения к Mongo ----------
def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi("1"))
    client.admin.command("ping")
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

# ---------- основная загрузка ----------
def load_bodystyles():
    url = "https://api.auto.ria.com/categories/5/bodystyles"
    headers = {
        "accept": "application/json"
    }
    # если есть API-ключ — добавь &api_key=... в url
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"⚠️ ошибка запроса: {response.status_code}")
        return

    bodystyles = response.json()
    
    collection = get_mongo_collection("DimBodystyles")
    
    documents = []
    for item in bodystyles:
        doc = {
            "category_id": 5,
            "bodystyle_id": item.get("value"),
            "name": item.get("name"),
            "parent_bodystyle_id": item.get("parentId", 0)
        }
        documents.append(doc)
    
    if documents:
        # чтобы избежать дублей — можно сделать очистку
        collection.delete_many({})
        collection.insert_many(documents)
        print(f"✅ загружено {len(documents)} кузовов в DimBodystyles")
    else:
        print("⚠️ данных нет для загрузки")

if __name__ == "__main__":
    load_bodystyles()


#загрузка всех марок производителей для категории 5 в количестве 1153
import requests
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi("1"))
    client.admin.command("ping")
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def load_marks():
    url = "https://api.auto.ria.com/categories/5/marks"
    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"⚠️ ошибка запроса: {response.status_code}")
        return

    marks_data = response.json()

    collection = get_mongo_collection("DimMarks")

    documents = []
    for mark in marks_data:
        doc = {
            "category_id": 5,
            "mark_id": mark.get("value"),
            "name": mark.get("name")
            # если будет поле bodystyle_id - можно добавить здесь
        }
        documents.append(doc)

    if documents:
        collection.delete_many({})  # очищаем коллекцию перед загрузкой
        collection.insert_many(documents)
        print(f"✅ загружено {len(documents)} марок в коллекцию DimMarks")
    else:
        print("⚠️ нет данных для загрузки")

if __name__ == "__main__":
    load_marks()
    
# загрузка всех видов моделей для всех марок типов 5 полуприцепы с созданием ключей и родительских связей

import requests
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi("1"))
    client.admin.command("ping")
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def load_models():
    url = "https://api.auto.ria.com/categories/5/models"  # пример, укажи точную ссылку на модели
    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"⚠️ ошибка запроса: {response.status_code}")
        return

    models_data = response.json()

    collection = get_mongo_collection("DimModels")

    documents = []
    for model in models_data:
        doc = {
            "category_id": 5,
            "model_id": model.get("value"),
            "name": model.get("name"),
            "mark_id": model.get("mark_id")  # должно быть в данных, если нет — поправим
        }
        documents.append(doc)

    if documents:
        collection.delete_many({})  # очистка коллекции перед загрузкой
        collection.insert_many(documents)
        print(f"✅ загружено {len(documents)} моделей в коллекцию DimModels")
    else:
        print("⚠️ нет данных для загрузки")

if __name__ == "__main__":
    load_models()
    
from pymongo import MongoClient
import os
from pprint import pprint

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def sample_documents(collection_name, limit=10):
    collection = get_mongo_collection(collection_name)
    return list(collection.find().limit(limit))

if __name__ == "__main__":
    # первая коллекция
    ia_data = sample_documents("ia_semitrailers_raw", 10)
    print("\n🔹 ia_semitrailers_raw (первые 10 записей):")
    for doc in ia_data:
        pprint(doc)
    
    # вторая коллекция
    ua_data = sample_documents("ua_trailer_raw", 10)
    print("\n🔹 ua_trailer_raw (первые 10 записей):")
    for doc in ua_data:
        pprint(doc)


#завантаження областей актуальних

import os
import requests
from pymongo import MongoClient

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def load_states(api_key):
    url = f"https://developers.ria.com/auto/states?api_key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    states = response.json()
    
    collection = get_mongo_collection("DimStates")
    # Очистим коллекцию перед загрузкой
    collection.delete_many({})
    
    # Вставим новые данные
    collection.insert_many(states)
    
    print(f"Загружено областей: {len(states)}")

if __name__ == "__main__":
    api_key = os.environ.get("YOUR_API_KEY")
    if not api_key:
        raise ValueError("API ключ не найден в переменной окружения YOUR_API_KEY")
    load_states(api_key)


#додавання актуальних наз областей и городов на укр язіке

import os
import requests
from pymongo import MongoClient

API_KEY = os.environ.get("YOUR_API_KEY")  # или как у тебя назван в окружении

def get_mongo_collection(collection_name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    print(f"✅ подключено к коллекции: {collection_name}")
    return client["trailer"][collection_name]

def get_states():
    url = f"https://developers.ria.com/auto/states?api_key={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_cities(state_id):
    url = f"https://developers.ria.com/auto/states/{state_id}/cities?api_key={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def main():
    dim_states_col = get_mongo_collection("DimStates")
    dim_cities_col = get_mongo_collection("DimCities")

    states = get_states()
    print(f"Загружено областей: {len(states)}")

    for state in states:
        dim_states_col.update_one(
            {"value": state["value"]},
            {"$set": state},
            upsert=True
        )

    total_cities = 0
    for state in states:
        cities = get_cities(state["value"])
        for city in cities:
            city["state_id"] = state["value"]  # связь с областью
            dim_cities_col.update_one(
                {"value": city["value"]},
                {"$set": city},
                upsert=True
            )
        print(f"Область '{state['name']}' ({state['value']}): загружено городов {len(cities)}")
        total_cities += len(cities)

    print(f"Всего городов загружено: {total_cities}")

if __name__ == "__main__":
    main()
    
!pip install geopy

# берёт города из коллекции DimCities ✅ запрашивает координаты через geopy (по названию на украинском)
#обновляет документы в DimCities (добавляет latitude и longitude)
#пишет подробный лог в коллекцию geocoding_log
#выводит summary после выполнения

import os
import time
from pymongo import MongoClient
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

dim_cities_col = get_mongo_collection("DimCities")
log_col = get_mongo_collection("geocoding_log")

geolocator = Nominatim(user_agent="my_geocoder")

def geocode_city(city_name_ua):
    try:
        location = geolocator.geocode(city_name_ua, language='uk')
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except (GeocoderTimedOut, GeocoderUnavailable) as e:
        print(f"Ошибка геокодинга для '{city_name_ua}': {e}")
        return None, None

def geocode_all_cities(delay=1.5):
    cities_cursor = dim_cities_col.find({})
    total = 0
    updated = 0
    not_found = []

    print("Начинаем геокодинг городов...")

    for city in cities_cursor:
        total += 1
        city_name = city.get("name_ua") or city.get("name")
        print(f"[{total}] Обрабатываем город: {city_name}")

        if not city_name:
            print(f" - Пропускаем: отсутствует название города")
            not_found.append({"_id": city["_id"], "reason": "no city name"})
            continue

        lat, lon = geocode_city(city_name)
        if lat is not None and lon is not None:
            dim_cities_col.update_one(
                {"_id": city["_id"]},
                {"$set": {"latitude": lat, "longitude": lon}}
            )
            print(f" - Координаты найдены и обновлены: {lat}, {lon}")
            updated += 1
        else:
            print(f" - Координаты не найдены")
            not_found.append(city_name)

        time.sleep(delay)

    log_entry = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_cities": total,
        "updated_cities": updated,
        "not_found_cities": not_found
    }
    log_col.insert_one(log_entry)

    print("\nГеокодинг завершён.")
    print(f"Обработано городов: {total}")
    print(f"Успешно обновлено: {updated}")
    print(f"Не найдены координаты для: {not_found}")

geocode_all_cities()


#тестовая загрузка первіх 20 городов
from pymongo import MongoClient
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time

# Подключение к Mongo
def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

dim_cities_col = get_mongo_collection("DimCities")
log_col = get_mongo_collection("geocoding_log")

geolocator = Nominatim(user_agent="my_geocoder", timeout=10)

def geocode_city(city_name):
    try:
        location = geolocator.geocode(city_name + ", Ukraine", language="uk")
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        return None, None

# Получаем первые 20 городов из коллекции
cities_cursor = dim_cities_col.find({}, {"_id": 0, "name": 1}).limit(20)
cities = list(cities_cursor)

log = {
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    "total_cities": len(cities),
    "success_count": 0,
    "failures": []
}

for city in cities:
    name = city.get("name")
    lat, lon = geocode_city(name)
    if lat is not None and lon is not None:
        # Обновляем коллекцию DimCities
        dim_cities_col.update_one({"name": name}, {"$set": {"latitude": lat, "longitude": lon}})
        log["success_count"] += 1
    else:
        log["failures"].append(name)

# Записываем лог
log_col.insert_one(log)

# Выводим результаты
print(f"Обработано городов: {log['total_cities']}")
print(f"Успешно обновлено: {log['success_count']}")
print(f"Не найдены координаты для: {log['failures']}")


# створення колекції валют
import os
from pymongo import MongoClient

# Функция подключения
def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Получаем коллекцию
dim_currency_col = get_mongo_collection("DimCurrency")

# Проверяем, есть ли уже документы
if dim_currency_col.count_documents({}) == 0:
    currencies = [
        {"_id": 1, "currencyCode": "USD", "currencyName": "Долар США"},
        {"_id": 2, "currencyCode": "EUR", "currencyName": "Євро"},
        {"_id": 3, "currencyCode": "UAH", "currencyName": "Гривня"}
    ]
    dim_currency_col.insert_many(currencies)
    print("✅ Коллекция DimCurrency успешно создана и заполнена")
else:
    print("ℹ️ Коллекция DimCurrency уже содержит данные")


# створення колеції типів продавця
import os
from pymongo import MongoClient

# Функция подключения
def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Получаем коллекцию
dim_seller_type_col = get_mongo_collection("DimSellerType")

# Проверяем, есть ли уже документы
if dim_seller_type_col.count_documents({}) == 0:
    seller_types = [
        {"_id": 0, "sellerTypeName": "Усі"},
        {"_id": 1, "sellerTypeName": "Приватна особа"},
        {"_id": 2, "sellerTypeName": "Компанія"}
    ]
    dim_seller_type_col.insert_many(seller_types)
    print("✅ Коллекция DimSellerType успешно создана и заполнена")
else:
    print("ℹ️ Коллекция DimSellerType уже содержит данные")


import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

# Загружаем .env
load_dotenv()

# Подключение к MongoDB
def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Подключение к PostgreSQL
def connect_postgres():
    return psycopg2.connect(
        host=os.getenv("supabase_host"),
        user=os.getenv("supabase_user"),
        password=os.getenv("supabase_password"),
        dbname=os.getenv("supabase_name"),
        port=os.getenv("supabase_port", 5432)
    )

# Загрузка данных
def load_data():
    df_states = pd.json_normalize(list(get_mongo_collection("DimStates").find()))
    df_cities = pd.json_normalize(list(get_mongo_collection("DimCities").find()))
    print(f"Загружено областей: {len(df_states)}")
    print(f"Загружено городов: {len(df_cities)}")
    return df_states, df_cities

# Вставка в PostgreSQL
def insert_to_postgres(df_states, df_cities):
    conn = connect_postgres()
    cursor = conn.cursor()

    # Очистим таблицы
    cursor.execute("TRUNCATE TABLE dim_cities RESTART IDENTITY CASCADE;")
    cursor.execute("TRUNCATE TABLE dim_states RESTART IDENTITY CASCADE;")

    # Вставим области
    for _, row in df_states.iterrows():
        cursor.execute("""
            INSERT INTO dim_states (mongo_id, name, value)
            VALUES (%s, %s, %s)
        """, (str(row['_id']), row['name'], row['value']))

    # Вставим города
    for _, row in df_cities.iterrows():
        cursor.execute("""
            INSERT INTO dim_cities (mongo_id, name, state_id, value, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            str(row['_id']),
            row['name'],
            row.get('state_id'),
            row.get('value'),
            row.get('latitude'),
            row.get('longitude')
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Данные успешно загружены в PostgreSQL")

# Запуск
df_states, df_cities = load_data()
insert_to_postgres(df_states, df_cities)


import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Получаем данные из коллекции DimCurrency
collection = get_mongo_collection("DimCurrency")
documents = list(collection.find({}))
df = pd.json_normalize(documents)
print(f"Получено записей из Mongo: {len(df)}")

# Подключаемся к PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("supabase_host"),
    user=os.getenv("supabase_user"),
    password=os.getenv("supabase_password"),
    dbname=os.getenv("supabase_name"),
    port=os.getenv("supabase_port", 5432)
)
cursor = conn.cursor()

# Очищаем таблицу перед загрузкой
cursor.execute('TRUNCATE TABLE public."DimCurrency";')
conn.commit()

# Колонки с двойными кавычками для чувствительности к регистру
columns = ['_id', 'currencyCode', 'currencyName']
quoted_columns = [f'"{col}"' for col in columns]

values_placeholder = ','.join(['%s'] * len(columns))
insert_query = f'INSERT INTO public."DimCurrency" ({",".join(quoted_columns)}) VALUES ({values_placeholder})'

for _, row in df.iterrows():
    data = [row.get(col, None) for col in columns]
    cursor.execute(insert_query, data)

conn.commit()
cursor.close()


#
import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Получаем коллекцию DimAxles
collection = get_mongo_collection("DimAxles")
documents = list(collection.find({}))

df = pd.json_normalize(documents)
print(f"Получено записей из Mongo: {len(df)}")

# Подключение к PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("supabase_host"),
    user=os.getenv("supabase_user"),
    password=os.getenv("supabase_password"),
    dbname=os.getenv("supabase_name"),
    port=os.getenv("supabase_port", 5432)
)
cursor = conn.cursor()

# Очищаем таблицу перед загрузкой
cursor.execute('TRUNCATE TABLE public."DimAxles";')
conn.commit()

# SQL-запрос с кавычками и точным регистром для вставки
insert_query = 'INSERT INTO public."DimAxles" ("_id", "axleId", "name") VALUES (%s, %s, %s)'

for _, row in df.iterrows():
    _id_str = str(row['_id']) if '_id' in row else None
    axle_id = row.get('axleId')
    name = row.get('name')
    cursor.execute(insert_query, (_id_str, axle_id, name))

conn.commit()
cursor.close()
conn.close()

print("Данные DimAxles успешно записаны в PostgreSQL")



import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Получаем коллекцию DimCountries из Mongo
collection = get_mongo_collection("DimCountries")
documents = list(collection.find({}))

df = pd.json_normalize(documents)
print(f"Получено записей из Mongo: {len(df)}")

# Приводим _id к строке (если это ObjectId)
df["_id"] = df["_id"].astype(str)

# Подключение к PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("supabase_host"),
    user=os.getenv("supabase_user"),
    password=os.getenv("supabase_password"),
    dbname=os.getenv("supabase_name"),
    port=os.getenv("supabase_port", 5432)
)
cursor = conn.cursor()

# Очистка таблицы (если нужно перезаписать)
cursor.execute('TRUNCATE TABLE public."DimCountries";')
conn.commit()

# Вставка данных
columns = ["_id", "name", "value"]
values_placeholder = ','.join(['%s'] * len(columns))
insert_query = f'INSERT INTO public."DimCountries" ({",".join(columns)}) VALUES ({values_placeholder})'

for _, row in df.iterrows():
    data = [row.get(col, None) for col in columns]
    cursor.execute(insert_query, data)

conn.commit()
cursor.close()
conn.close()

print("Данные успешно записаны в DimCountries")


#загрузка категорий

import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Получаем коллекцию DimCategories из Mongo
collection = get_mongo_collection("DimCategories")
documents = list(collection.find({}))

df = pd.json_normalize(documents)
print(f"Получено записей из Mongo: {len(df)}")

# Преобразуем _id в строку
df["_id"] = df["_id"].astype(str)

# Подключение к PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("supabase_host"),
    user=os.getenv("supabase_user"),
    password=os.getenv("supabase_password"),
    dbname=os.getenv("supabase_name"),
    port=os.getenv("supabase_port", 5432)
)
cursor = conn.cursor()

# Очищаем таблицу, если нужно перезаписать данные
cursor.execute('TRUNCATE TABLE public.dim_categories;')
conn.commit()

# Вставляем данные (id SERIAL — генерируется автоматически)
insert_query = """
INSERT INTO public.dim_categories (mongo_id, category_value, category_name)
VALUES (%s, %s, %s)
"""

for _, row in df.iterrows():
    data = [
        row.get("_id", None),
        row.get("value", None),
        row.get("name", None)
    ]
    cursor.execute(insert_query, data)

conn.commit()
cursor.close()
conn.close()

print("Данные успешно записаны в dim_categories")


#загрузка
import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Получаем коллекцию DimSellerType из Mongo
collection = get_mongo_collection("DimSellerType")
documents = list(collection.find({}))

df = pd.json_normalize(documents)
print(f"Получено записей из Mongo: {len(df)}")

# Преобразуем _id в int (если нужно)
df["_id"] = df["_id"].astype(int)

# Подключение к PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("supabase_host"),
    user=os.getenv("supabase_user"),
    password=os.getenv("supabase_password"),
    dbname=os.getenv("supabase_name"),
    port=os.getenv("supabase_port", 5432)
)
cursor = conn.cursor()

# Создаем таблицу, если она еще не создана
create_table_query = """
CREATE TABLE IF NOT EXISTS public.dim_seller_type (
    id INTEGER PRIMARY KEY,
    seller_type_name TEXT NOT NULL
);
"""
cursor.execute(create_table_query)
conn.commit()

# Очищаем таблицу (если нужно)
cursor.execute('TRUNCATE TABLE public.dim_seller_type;')
conn.commit()

# Вставляем данные
insert_query = """
INSERT INTO public.dim_seller_type (id, seller_type_name)
VALUES (%s, %s)
"""

for _, row in df.iterrows():
    data = [
        row.get("_id", None),
        row.get("sellerTypeName", None)
    ]
    cursor.execute(insert_query, data)

conn.commit()
cursor.close()
conn.close()

print("Данные успешно записаны в dim_seller_type")

import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Получаем коллекцию DimWheelFormulas из Mongo
collection = get_mongo_collection("DimWheelFormulas")
documents = list(collection.find({}))

df = pd.json_normalize(documents)
print(f"Получено записей из Mongo: {len(df)}")

# Преобразуем _id в строку
df["_id"] = df["_id"].astype(str)

# Подключение к PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("supabase_host"),
    user=os.getenv("supabase_user"),
    password=os.getenv("supabase_password"),
    dbname=os.getenv("supabase_name"),
    port=os.getenv("supabase_port", 5432)
)
cursor = conn.cursor()

# Создаем таблицу, если ее еще нет
create_table_query = """
CREATE TABLE IF NOT EXISTS public.dim_wheel_formulas (
    mongo_id VARCHAR(24) PRIMARY KEY,
    wheel_formula_id INTEGER NOT NULL,
    name TEXT NOT NULL
);
"""
cursor.execute(create_table_query)
conn.commit()

# Очищаем таблицу перед загрузкой новых данных
cursor.execute('TRUNCATE TABLE public.dim_wheel_formulas;')
conn.commit()

# Вставка данных
insert_query = """
INSERT INTO public.dim_wheel_formulas (mongo_id, wheel_formula_id, name)
VALUES (%s, %s, %s)
"""

for _, row in df.iterrows():
    data = [
        row.get("_id", None),
        row.get("wheelFormulaId", None),
        row.get("name", None)
    ]
    cursor.execute(insert_query, data)

conn.commit()
cursor.close()
conn.close()

print("Данные успешно записаны в dim_wheel_formulas")

import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Получаем коллекцию DimOptions из Mongo
collection = get_mongo_collection("DimOptions")
documents = list(collection.find({}))

df = pd.json_normalize(documents)
print(f"Получено записей из Mongo: {len(df)}")

# Преобразуем _id в строку
df["_id"] = df["_id"].astype(str)

# Подключение к PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("supabase_host"),
    user=os.getenv("supabase_user"),
    password=os.getenv("supabase_password"),
    dbname=os.getenv("supabase_name"),
    port=os.getenv("supabase_port", 5432)
)
cursor = conn.cursor()

# Создаем таблицу, если ее еще нет
create_table_query = """
CREATE TABLE IF NOT EXISTS public.dim_options (
    mongo_id VARCHAR(24) PRIMARY KEY,
    name TEXT NOT NULL,
    value INTEGER,
    category_id INTEGER
);
"""
cursor.execute(create_table_query)
conn.commit()

# Очищаем таблицу перед загрузкой новых данных
cursor.execute('TRUNCATE TABLE public.dim_options;')
conn.commit()

# Вставка данных
insert_query = """
INSERT INTO public.dim_options (mongo_id, name, value, category_id)
VALUES (%s, %s, %s, %s)
"""

for _, row in df.iterrows():
    data = [
        row.get("_id", None),
        row.get("name", None),
        row.get("value", None),
        row.get("categoryId", None)
    ]
    cursor.execute(insert_query, data)

conn.commit()
cursor.close()
conn.close()

print("Данные успешно записаны в dim_options")


#создание таблицы марок 
import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Получаем коллекцию DimMarks из Mongo
collection = get_mongo_collection("DimMarks")
documents = list(collection.find({}))

df = pd.json_normalize(documents)
print(f"Получено записей из Mongo: {len(df)}")

# Преобразуем _id в строку
df["_id"] = df["_id"].astype(str)

# Подключение к PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("supabase_host"),
    user=os.getenv("supabase_user"),
    password=os.getenv("supabase_password"),
    dbname=os.getenv("supabase_name"),
    port=os.getenv("supabase_port", 5432)
)
cursor = conn.cursor()

# Создаем таблицу, если ее еще нет
create_table_query = """
CREATE TABLE IF NOT EXISTS public.dim_marks (
    mongo_id VARCHAR(24) PRIMARY KEY,
    name TEXT NOT NULL,
    value INTEGER,
    category_id INTEGER
);
"""
cursor.execute(create_table_query)
conn.commit()

# Очищаем таблицу перед загрузкой новых данных
cursor.execute('TRUNCATE TABLE public.dim_marks;')
conn.commit()

# Вставка данных
insert_query = """
INSERT INTO public.dim_marks (mongo_id, name, value, category_id)
VALUES (%s, %s, %s, %s)
"""

for _, row in df.iterrows():
    data = [
        row.get("_id", None),
        row.get("name", None),
        row.get("value", None),
        row.get("categoryId", None)
    ]
    cursor.execute(insert_query, data)

conn.commit()
cursor.close()
conn.close()

print("Данные успешно записаны в dim_marks")


import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

def main():
    # Подключаемся к Mongo
    collection = get_mongo_collection("DimModels")
    documents = list(collection.find({}))
    df = pd.json_normalize(documents)

    print(f"Получено записей из Mongo: {len(df)}")

    # Подключаемся к PostgreSQL
    conn = psycopg2.connect(
        host=os.getenv("supabase_host"),
        user=os.getenv("supabase_user"),
        password=os.getenv("supabase_password"),
        dbname=os.getenv("supabase_name"),
        port=os.getenv("supabase_port", 5432)
    )
    cursor = conn.cursor()

    # Очищаем таблицу, если нужно
    cursor.execute("TRUNCATE TABLE public.dim_models;")
    conn.commit()

    # Столбцы для вставки — подкорректируй под свою структуру
    columns = ['_id', 'name', 'value', 'categoryId', 'markId']
    placeholders = ','.join(['%s'] * len(columns))
    insert_query = f"""
        INSERT INTO public.dim_models (mongo_id, name, value, category_id, mark_id)
        VALUES ({placeholders})
    """

    # Вставляем данные построчно
    for _, row in df.iterrows():
        data = [
            str(row.get('_id')), 
            row.get('name'), 
            row.get('value'), 
            row.get('categoryId'), 
            row.get('markId')
        ]
        cursor.execute(insert_query, data)

    conn.commit()
    cursor.close()
    conn.close()

    print("Данные успешно загружены в dim_models")

if __name__ == "__main__":
    main()


ef load_dim_models():
    collection = get_mongo_collection("DimModels")
    documents = list(collection.find({}))
    df_models = pd.json_normalize(documents)
    
    cursor.execute("TRUNCATE TABLE public.dim_models;")
    conn.commit()
    
    # Получаем все существующие значения markId из dim_marks.value для проверки
    cursor.execute("SELECT value FROM public.dim_marks;")
    valid_mark_values = set(row[0] for row in cursor.fetchall())
    
    insert_query = """
        INSERT INTO public.dim_models (mongo_id, name, value, category_id, mark_id)
        VALUES (%s, %s, %s, %s, %s)
    """
    
    for _, row in df_models.iterrows():
        mark_id = row.get('markId')
        if mark_id not in valid_mark_values:
            print(f"Пропускаем модель {row.get('name')} — markId {mark_id} отсутствует в dim_marks")
            continue
        cursor.execute(insert_query, (
            str(row['_id']),
            row.get('name'),
            row.get('value'),
            row.get('categoryId'),
            mark_id
        ))
    conn.commit()
    print("DimModels загружены успешно.")


def load_dim_models():
    try:
        collection = get_mongo_collection("DimModels")
        documents = list(collection.find({}))
        print(f"Получено моделей из Mongo: {len(documents)}")
        if not documents:
            print("В коллекции DimModels нет данных.")
            return
        
        df_models = pd.json_normalize(documents)
        
        cursor.execute("TRUNCATE TABLE public.dim_models;")
        conn.commit()
        print("Таблица public.dim_models очищена.")
        
        # Получаем все существующие markId из dim_marks.value
        cursor.execute("SELECT value FROM public.dim_marks;")
        valid_mark_values = set(row[0] for row in cursor.fetchall())
        print(f"Количество допустимых markId в dim_marks: {len(valid_mark_values)}")
        
        insert_query = """
            INSERT INTO public.dim_models (mongo_id, name, value, category_id, mark_id)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        inserted_count = 0
        skipped_count = 0
        for _, row in df_models.iterrows():
            mark_id = row.get('markId')
            if mark_id not in valid_mark_values:
                print(f"Пропускаем модель '{row.get('name')}' — markId {mark_id} отсутствует в dim_marks")
                skipped_count += 1
                continue
            
            cursor.execute(insert_query, (
                str(row['_id']),
                row.get('name'),
                row.get('value'),
                row.get('categoryId'),
                mark_id
            ))
            inserted_count += 1
        
        conn.commit()
        print(f"Вставлено моделей: {inserted_count}")
        print(f"Пропущено моделей: {skipped_count}")
        print("DimModels загружены успешно.")
        
    except Exception as e:
        print(f"Ошибка при загрузке DimModels: {e}")


def load_dim_models_debug():
    try:
        print("Начинаем загрузку DimModels...")
        collection = get_mongo_collection("DimModels")
        documents = list(collection.find({}))
        print(f"Документов в DimModels: {len(documents)}")
        if len(documents) == 0:
            print("Коллекция DimModels пуста!")
            return
        
        df_models = pd.json_normalize(documents)
        print("Преобразовали документы в DataFrame")
        print(df_models.head())

        cursor.execute("TRUNCATE TABLE public.dim_models;")
        conn.commit()
        print("Таблица public.dim_models очищена")
        
        cursor.execute("SELECT value FROM public.dim_marks;")
        valid_mark_values = set(row[0] for row in cursor.fetchall())
        print(f"Допустимых markId в dim_marks: {valid_mark_values}")
        
        insert_query = """
            INSERT INTO public.dim_models (mongo_id, name, value, category_id, mark_id)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        count_inserted = 0
        count_skipped = 0
        for idx, row in df_models.iterrows():
            mark_id = row.get('markId')
            if mark_id not in valid_mark_values:
                print(f"Пропускаем модель {row.get('name')} с markId={mark_id}")
                count_skipped += 1
                continue
            cursor.execute(insert_query, (
                str(row['_id']),
                row.get('name'),
                row.get('value'),
                row.get('categoryId'),
                mark_id
            ))
            count_inserted += 1
        
        conn.commit()
        print(f"Вставлено моделей: {count_inserted}, пропущено: {count_skipped}")
    except Exception as e:
        print(f"Ошибка: {e}")


from pymongo import MongoClient
import os

def test_mongo_connection():
    try:
        uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        client = MongoClient(uri)
        db = client["trailer"]
        collection = db["DimModels"]
        docs = list(collection.find({}).limit(3))
        print(f"Получено документов: {len(docs)}")
        for d in docs:
            print(d)
    except Exception as e:
        print(f"Ошибка при подключении или запросе: {e}")

test_mongo_connection()


import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Получаем коллекцию DimModels
collection = get_mongo_collection("DimModels")
documents = list(collection.find({}))

# Преобразуем в DataFrame
df = pd.json_normalize(documents)
print(f"Получено документов: {len(df)}")
print(df.head())

# Подключаемся к PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("supabase_host"),
    user=os.getenv("supabase_user"),
    password=os.getenv("supabase_password"),
    dbname=os.getenv("supabase_name"),
    port=os.getenv("supabase_port", 5432)
)
cursor = conn.cursor()

# Очистка таблицы перед загрузкой (если нужно)
cursor.execute("TRUNCATE TABLE public.dim_models;")
conn.commit()

# Колонки для вставки в SQL (соблюдаем регистр и имена из таблицы)
columns = ["mongo_id", "name", "value", "category_id", "mark_id"]
placeholders = ','.join(['%s'] * len(columns))

insert_query = f"""
    INSERT INTO public.dim_models ({','.join(columns)})
    VALUES ({placeholders})
"""

# Подготовка данных и вставка
for _, row in df.iterrows():
    data = [
        str(row["_id"]),             # mongo_id как строка
        row.get("name"),
        row.get("value"),
        row.get("categoryId"),
        row.get("markId")
    ]
    cursor.execute(insert_query, data)

conn.commit()
cursor.close()
conn.close()

print("Данные успешно загружены в dim_models")


import os
import psycopg2
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Получаем данные из Mongo
collection = get_mongo_collection("DimModels")
documents = list(collection.find({}))

df = pd.json_normalize(documents)
print(f"Получено документов: {len(df)}")
print(df.head())

conn = psycopg2.connect(
    host=os.getenv("supabase_host"),
    user=os.getenv("supabase_user"),
    password=os.getenv("supabase_password"),
    dbname=os.getenv("supabase_name"),
    port=os.getenv("supabase_port", 5432)
)
cursor = conn.cursor()

cursor.execute("TRUNCATE TABLE public.dim_models;")
conn.commit()

columns = ["mongo_id", "name", "value", "category_id", "mark_id"]

insert_query = f"""
    INSERT INTO public.dim_models ({','.join(columns)})
    VALUES %s
"""

from psycopg2.extras import execute_values

# Формируем список кортежей для вставки
records = []
for _, row in df.iterrows():
    record = (
        str(row["_id"]),
        row.get("name"),
        row.get("value"),
        row.get("categoryId"),
        row.get("markId")
    )
    records.append(record)

# Вставляем батчами по 1000 записей
batch_size = 1000
for i in range(0, len(records), batch_size):
    batch = records[i:i+batch_size]
    execute_values(cursor, insert_query, batch)
    conn.commit()
    print(f"Вставлено записей: {i + len(batch)}")

cursor.close()
conn.close()

print("Данные успешно загружены в dim_models")


import os
import pandas as pd
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

def load_collection_to_postgres(collection_name, table_name, columns_map, cursor, conn):
    collection = get_mongo_collection(collection_name)
    documents = list(collection.find({}))
    print(f"Получено документов из {collection_name}: {len(documents)}")

    df = pd.json_normalize(documents)
    # Выбираем только нужные колонки, переименовываем
    df = df[list(columns_map.keys())].rename(columns=columns_map)

    # Преобразуем в список кортежей для вставки
    data_tuples = [tuple(x) for x in df.to_numpy()]

    # Создаем шаблон для execute_values
    columns_str = ', '.join(columns_map.values())
    values_str = ', '.join(['%s'] * len(columns_map))

    insert_query = f"""
    INSERT INTO {table_name} ({columns_str})
    VALUES %s
    ON CONFLICT DO NOTHING
    """

    # Вставляем данные пакетно
    execute_values(cursor, insert_query, data_tuples)
    conn.commit()
    print(f"Данные из {collection_name} загружены в таблицу {table_name}")

# Подключение к Postgres
conn = psycopg2.connect(
    host=os.getenv("supabase_host"),
    user=os.getenv("supabase_user"),
    password=os.getenv("supabase_password"),
    dbname=os.getenv("supabase_name"),
    port=os.getenv("supabase_port", 5432)
)
cursor = conn.cursor()

# Загрузка DimBodyStyleGroups
load_collection_to_postgres(
    collection_name="DimBodyStyleGroups",
    table_name="public.dim_body_style_groups",
    columns_map={
        "groupId": "group_id",
        "name": "name",
        "categoryId": "category_id"
    },
    cursor=cursor,
    conn=conn
)

# Загрузка DimBodystyles
load_collection_to_postgres(
    collection_name="DimBodystyles",
    table_name="public.dim_body_styles",
    columns_map={
        "bodystyle_id": "bodystyle_id",
        "name": "name",
        "category_id": "category_id",
        "parent_bodystyle_id": "parent_bodystyle_id"
    },
    cursor=cursor,
    conn=conn
)

cursor.close()
conn.close()


!pip install pymongo psycopg2 pandas sqlalchemy

import os
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
from datetime import datetime

# Подключение к Mongo
def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client["trailer"][name]

# Загружаем данные
collections = ["FactTrailer1", "FactTrailer2"]  # Названия твоих коллекций
all_docs = []

for coll in collections:
    documents = list(get_mongo_collection(coll).find({}))
    all_docs.extend(documents)

df = pd.json_normalize(all_docs)

# Преобразуем поля
df_clean = pd.DataFrame({
    "listing_id": df["_id"].astype("int"),
    "model_id": df["modelId"].fillna(-1).astype("Int64"),
    "mark_id": df["markId"].fillna(-1).astype("Int64"),
    "body_id": df["autoData.bodyId"].fillna(-1).astype("Int64"),
    "category_id": df["autoData.categoryId"].fillna(-1).astype("Int64"),
    "state_id": df["stateData.stateId"].fillna(-1).astype("Int64"),
    "city_id": df["stateData.cityId"].fillna(-1).astype("Int64"),
    "user_id": df["userBlocked.userId"].fillna(-1).astype("Int64"),
    "dealer_id": df["dealer.id"].fillna(-1).astype("Int64"),
    "price_usd": df["prices.0.USD"].fillna(0).astype("Int64"),
    "price_eur": df["prices.0.EUR"].fillna(0).astype("Int64"),
    "price_uah": df["prices.0.UAH"].fillna(0).astype("Int64"),
    "currency_code": df["autoData.mainCurrency"],
    "year": df["autoData.year"].fillna(0).astype("Int64"),
    "race_km": df["autoData.raceInt"].fillna(0).astype("Int64"),
    "is_active": df["autoData.active"].fillna(False).astype(bool),
    "is_sold": df["autoData.isSold"].fillna(False).astype(bool),
    "exchange_possible": df["prices.0.exchangePossible"].fillna(False).astype(bool),
    "auction_possible": df["autoData.auctionPossible"].fillna(False).astype(bool),
    "with_video": df["autoData.withVideo"].fillna(False).astype(bool),
    "level": df["levelData.level"].fillna(0).astype("Int64"),
    "listing_date": pd.to_datetime(df["prices.0.updateDate"], errors="coerce"),
    "expire_date": pd.to_datetime(df["prices.0.expireDate"], errors="coerce"),
    "country_id": 804,  # Украина
    "wheel_formula_id": df["autoData.wheelFormulaId"].fillna(-1).astype("Int64")
})

# Подключение к PostgreSQL
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('supabase_user')}:{os.getenv('supabase_password')}@{os.getenv('supabase_host')}:{os.getenv('supabase_port', 5432)}/{os.getenv('supabase_name')}"
)

# Загрузка в таблицу
df_clean.to_sql("fact_trailer", engine, if_exists="append", index=False)
print(f"Загружено записей: {len(df_clean)}")


#загрузка 

import os
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
from datetime import datetime

def get_mongo_collection(name):
    uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri)
    return client, client["trailer"][name]

collections = ["ua_trailer_raw", "ria_semitrailers_raw"]
all_docs = []

for coll in collections:
    client, collection = get_mongo_collection(coll)
    documents = list(collection.find({}))
    print(f"[INFO] Загружено документов из коллекции '{coll}': {len(documents)}")
    all_docs.extend(documents)
    client.close()

if not all_docs:
    print("[WARNING] Нет данных для обработки. Скрипт завершён.")
    exit()

df = pd.json_normalize(all_docs)

df_clean = pd.DataFrame({
    "listing_id": df["_id"].astype("int"),
    "model_id": df.get("modelId", pd.Series()).fillna(-1).astype("Int64"),
    "mark_id": df.get("markId", pd.Series()).fillna(-1).astype("Int64"),
    "body_id": df.get("autoData.bodyId", pd.Series()).fillna(-1).astype("Int64"),
    "category_id": df.get("autoData.categoryId", pd.Series()).fillna(-1).astype("Int64"),
    "state_id": df.get("stateData.stateId", pd.Series()).fillna(-1).astype("Int64"),
    "city_id": df.get("stateData.cityId", pd.Series()).fillna(-1).astype("Int64"),
    "user_id": df.get("userBlocked.userId", pd.Series()).fillna(-1).astype("Int64"),
    "dealer_id": df.get("dealer.id", pd.Series()).fillna(-1).astype("Int64"),
    "price_usd": df.get("prices.0.USD", pd.Series()).fillna(0).astype("Int64"),
    "price_eur": df.get("prices.0.EUR", pd.Series()).fillna(0).astype("Int64"),
    "price_uah": df.get("prices.0.UAH", pd.Series()).fillna(0).astype("Int64"),
    "currency_code": df.get("autoData.mainCurrency", pd.Series()),
    "year": df.get("autoData.year", pd.Series()).fillna(0).astype("Int64"),
    "race_km": df.get("autoData.raceInt", pd.Series()).fillna(0).astype("Int64"),
    "is_active": df.get("autoData.active", pd.Series()).fillna(False).astype(bool),
    "is_sold": df.get("autoData.isSold", pd.Series()).fillna(False).astype(bool),
    "exchange_possible": df.get("prices.0.exchangePossible", pd.Series()).fillna(False).astype(bool),
    "auction_possible": df.get("autoData.auctionPossible", pd.Series()).fillna(False).astype(bool),
    "with_video": df.get("autoData.withVideo", pd.Series()).fillna(False).astype(bool),
    "level": df.get("levelData.level", pd.Series()).fillna(0).astype("Int64"),
    "listing_date": pd.to_datetime(df.get("prices.0.updateDate", pd.Series()), errors="coerce"),
    "expire_date": pd.to_datetime(df.get("prices.0.expireDate", pd.Series()), errors="coerce"),
    "country_id": 804,  # Украина
    "wheel_formula_id": df.get("autoData.wheelFormulaId", pd.Series()).fillna(-1).astype("Int64")
})

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('supabase_user')}:{os.getenv('supabase_password')}@{os.getenv('supabase_host')}:{os.getenv('supabase_port', 5432)}/{os.getenv('supabase_name')}"
)

df_clean.to_sql("fact_trailer", engine, if_exists="append", index=False)
print(f"[INFO] Загружено записей в fact_trailer: {len(df_clean)}")

import os
import logging
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Загружаем переменные окружения из .env
load_dotenv()

# Подключение к MongoDB
db_password = os.getenv('db_password')
mongo_uri = f"mongodb+srv://afedotova1974:{db_password}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(mongo_uri, server_api=ServerApi('1'))
db = client["trailer"]

# Подключение к Postgres
pg_host = os.getenv("supabase_host")
pg_user = os.getenv("supabase_user")
pg_password = os.getenv("supabase_password")
pg_dbname = os.getenv("supabase_name")
pg_port = os.getenv("supabase_port", 5432)

def safe_parse_price(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        return float(str(value).replace(' ', '').replace(',', '.'))
    except Exception:
        return None

def doc_to_tuple(doc):
    prices = doc.get('prices', [])
    price_eur = price_usd = price_uah = None
    currency_code = None
    if prices and isinstance(prices, list):
        price_entry = prices[0]
        price_eur = safe_parse_price(price_entry.get('EUR'))
        price_usd = safe_parse_price(price_entry.get('USD'))
        price_uah = safe_parse_price(price_entry.get('UAH'))
        currency_code = doc.get('autoData', {}).get('mainCurrency')

    return (
        doc.get('autoData', {}).get('autoId'),
        doc.get('markId'),
        doc.get('modelId'),
        doc.get('autoData', {}).get('categoryId'),
        doc.get('autoData', {}).get('bodyId'),
        doc.get('stateData', {}).get('cityId'),
        doc.get('stateData', {}).get('stateId'),
        doc.get('dealer', {}).get('id'),
        doc.get('autoData', {}).get('year'),
        price_eur,
        price_usd,
        price_uah,
        None,  # mileage_km - если есть, добавьте аналогично
        currency_code,
        doc.get('autoInfoBar', {}).get('addDate'),
        prices[0].get('expireDate') if prices else None,
        '804'  # принудительно прописываем country_code
    )

def create_table_if_not_exists(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS factvehicles_new (
        vehicle_id BIGINT PRIMARY KEY,
        mark_id INTEGER,
        model_id INTEGER,
        category_id INTEGER,
        body_id INTEGER,
        city_id INTEGER,
        state_id INTEGER,
        dealer_id INTEGER,
        year INTEGER,
        price_eur NUMERIC,
        price_usd NUMERIC,
        price_uah NUMERIC,
        mileage_km INTEGER,
        currency_code VARCHAR(10),
        added_at TIMESTAMP,
        expired_at TIMESTAMP,
        country_code VARCHAR(5)
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
    logging.info("Таблица factvehicles_new готова.")

def main():
    collections = ["ua_trailer_raw", "ria_semitrailers_raw"]

    try:
        conn = psycopg2.connect(
            host=pg_host,
            user=pg_user,
            password=pg_password,
            dbname=pg_dbname,
            port=pg_port
        )
    except Exception as e:
        logging.error(f"Ошибка подключения к Postgres: {e}")
        return

    create_table_if_not_exists(conn)

    all_records = []

    for coll_name in collections:
        logging.info(f"Обрабатываем коллекцию: {coll_name}")
        collection = db[coll_name]

        docs = list(collection.find({}))
        logging.info(f"Найдено документов: {len(docs)}")

        for doc in docs:
            record = doc_to_tuple(doc)
            if record[0] is not None:  # vehicle_id не должен быть None
                all_records.append(record)

    if not all_records:
        logging.warning("Нет данных для загрузки в Postgres")
        return

    insert_query = """
        INSERT INTO factvehicles_new (
            vehicle_id, mark_id, model_id, category_id, body_id,
            city_id, state_id, dealer_id, year,
            price_eur, price_usd, price_uah, mileage_km,
            currency_code, added_at, expired_at, country_code
        ) VALUES %s
        ON CONFLICT (vehicle_id) DO UPDATE SET
            mark_id = EXCLUDED.mark_id,
            model_id = EXCLUDED.model_id,
            category_id = EXCLUDED.category_id,
            body_id = EXCLUDED.body_id,
            city_id = EXCLUDED.city_id,
            state_id = EXCLUDED.state_id,
            dealer_id = EXCLUDED.dealer_id,
            year = EXCLUDED.year,
            price_eur = EXCLUDED.price_eur,
            price_usd = EXCLUDED.price_usd,
            price_uah = EXCLUDED.price_uah,
            mileage_km = EXCLUDED.mileage_km,
            currency_code = EXCLUDED.currency_code,
            added_at = EXCLUDED.added_at,
            expired_at = EXCLUDED.expired_at,
            country_code = EXCLUDED.country_code;
    """

    try:
        with conn.cursor() as cur:
            logging.info(f"Начинаем вставку {len(all_records)} записей в factvehicles_new")
            execute_values(cur, insert_query, all_records, page_size=1000)
            conn.commit()
            logging.info("Данные успешно загружены в factvehicles_new")
    except Exception as e:
        logging.error(f"Ошибка вставки в Postgres: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()


import os
import random
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import psycopg2
from psycopg2.extras import execute_values
from bson import ObjectId

# Подключение к MongoDB
db_password = os.getenv('db_password')
mongo_uri = f"mongodb+srv://afedotova1974:{db_password}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(mongo_uri, server_api=ServerApi('1'))
db = client["trailer"]

# Подключение к Postgres (Supabase)
pg_host = os.getenv("supabase_host")
pg_user = os.getenv("supabase_user")
pg_password = os.getenv("supabase_password")
pg_dbname = os.getenv("supabase_name")
pg_port = os.getenv("supabase_port", 5432)

conn = psycopg2.connect(
    host=pg_host,
    user=pg_user,
    password=pg_password,
    dbname=pg_dbname,
    port=pg_port
)
cur = conn.cursor()

# Преобразование ObjectId в строку и получение уникальных значений
def get_unique_values_str(collection_name, field_name):
    values = db[collection_name].distinct(field_name)
    return [str(v) if isinstance(v, ObjectId) else v for v in values]

mark_ids = get_unique_values_str("DimMarks", "_id")
model_ids = get_unique_values_str("DimModels", "_id")
category_ids = get_unique_values_str("DimBodyStyleGroups", "_id")
body_ids = get_unique_values_str("DimBodystyles", "_id")
dealer_ids = get_unique_values_str("DimSellerType", "_id")

# Европейские коды стран, которые ты указала
european_country_codes = [56, 100, 208, 233, 380, 428, 440, 528, 276, 616, 642, 246, 250, 203, 752]

# Создаем таблицу fact_vehicles_eur если не существует
create_table_sql = """
CREATE TABLE IF NOT EXISTS fact_vehicles_eur (
    vehicle_id SERIAL PRIMARY KEY,
    mark_id TEXT,
    model_id TEXT,
    category_id TEXT,
    body_id TEXT,
    state_id TEXT,
    dealer_id TEXT,
    year INT,
    price_eur NUMERIC,
    mileage_km INT,
    currency_code TEXT,
    added_at TIMESTAMP,
    expired_at TIMESTAMP,
    country_code INT
);
"""
cur.execute(create_table_sql)
conn.commit()

# Генерация данных
def generate_rows(n):
    rows = []
    for _ in range(n):
        mark_id = random.choice(mark_ids)
        model_id = random.choice(model_ids)
        category_id = random.choice(category_ids)
        body_id = random.choice(body_ids)
        state_id = random.choice(dealer_ids)
        dealer_id = random.choice(dealer_ids)
        year = random.randint(1990, 2025)
        price_eur = round(random.uniform(1000, 50000), 2)
        mileage_km = random.randint(0, 500000)
        currency_code = "EUR"
        added_at = datetime.now() - timedelta(days=random.randint(0, 365))
        expired_at = added_at + timedelta(days=365)
        country_code = random.choice(european_country_codes)
        row = (
            mark_id,
            model_id,
            category_id,
            body_id,
            state_id,
            dealer_id,
            year,
            price_eur,
            mileage_km,
            currency_code,
            added_at,
            expired_at,
            country_code,
        )
        rows.append(row)
    return rows

rows = generate_rows(100000)

# Вставка данных в Postgres
insert_sql = """
INSERT INTO fact_vehicles_eur (
    mark_id, model_id, category_id, body_id, state_id, dealer_id,
    year, price_eur, mileage_km, currency_code, added_at, expired_at, country_code
) VALUES %s
"""

execute_values(cur, insert_sql, rows, page_size=1000)
conn.commit()

cur.close()
conn.close()

print("Генерация и загрузка данных завершена.")


import os
import random
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import psycopg2
from psycopg2.extras import execute_values
from collections import defaultdict

# --- Подключение к MongoDB ---
db_password = os.getenv('db_password')
mongo_uri = f"mongodb+srv://afedotova1974:{db_password}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(mongo_uri, server_api=ServerApi('1'))
db = client["trailer"]

# --- Подключение к Postgres (Supabase) ---
pg_host = os.getenv("supabase_host")
pg_user = os.getenv("supabase_user")
pg_password = os.getenv("supabase_password")
pg_dbname = os.getenv("supabase_name")
pg_port = os.getenv("supabase_port", 5432)

conn = psycopg2.connect(
    host=pg_host,
    user=pg_user,
    password=pg_password,
    dbname=pg_dbname,
    port=pg_port
)
cur = conn.cursor()

# --- Функция для получения документов из коллекции ---
def get_collection_with_fields(collection_name, fields):
    docs = list(db[collection_name].find({}, fields))
    return [{k: (str(v) if isinstance(v, ObjectId) else v) for k, v in doc.items()} for doc in docs]

from bson import ObjectId

# --- Загружаем справочники ---
bodystyles = get_collection_with_fields("DimBodystyles", {"_id": 1, "groupId": 1, "categoryId": 1, "name": 1})
groups = get_collection_with_fields("DimBodyStyleGroups", {"_id": 1, "name": 1})
marks = get_collection_with_fields("DimMarks", {"_id": 1, "name": 1})
models = get_collection_with_fields("DimModels", {"_id": 1, "markId": 1, "name": 1})
sellers = get_collection_with_fields("DimSellerType", {"_id": 1, "name": 1})

# --- Индекс моделей по маркам (ключи как строки) ---
models_by_mark = defaultdict(list)
for model in models:
    mark_id = model.get("markId")
    if mark_id is not None:
        models_by_mark[str(mark_id)].append(model)

# --- Индекс бодистилей по группам ---
bodystyles_by_group = defaultdict(list)
for body in bodystyles:
    group_id = body.get("groupId")
    if group_id is not None:
        bodystyles_by_group[group_id].append(body)

# --- Европейские коды стран ---
european_country_codes = [56, 100, 208, 233, 380, 428, 440, 528, 276, 616, 642, 246, 250, 203, 752]

# --- Создаем таблицу fact_vehicles_eur ---
create_table_sql = """
CREATE TABLE IF NOT EXISTS fact_vehicles_eur (
    vehicle_id SERIAL PRIMARY KEY,
    mark_id TEXT,
    model_id TEXT,
    category_id TEXT,
    body_id TEXT,
    state_id TEXT,
    dealer_id TEXT,
    year INT,
    price_eur NUMERIC,
    mileage_km INT,
    currency_code TEXT,
    added_at TIMESTAMP,
    expired_at TIMESTAMP,
    country_code INT
);
"""
cur.execute(create_table_sql)
conn.commit()

# --- Генерация данных ---
def generate_rows(n):
    rows = []
    skipped_models = 0
    skipped_bodystyles = 0
    for _ in range(n):
        mark = random.choice(marks)
        mark_id = mark["_id"]  # строка

        models_list = models_by_mark.get(mark_id)
        if not models_list:
            skipped_models += 1
            continue
        model = random.choice(models_list)
        model_id = model["_id"]

        group = random.choice(groups)
        group_id = group["_id"]
        bodystyles_list = bodystyles_by_group.get(group_id)
        if not bodystyles_list:
            skipped_bodystyles += 1
            continue
        body = random.choice(bodystyles_list)
        body_id = body["_id"]
        category_id = body.get("categoryId")
        if category_id is None:
            category_id = "0"
        else:
            category_id = str(category_id)

        seller = random.choice(sellers)
        dealer_id = seller["_id"]
        state_id = dealer_id

        year = random.randint(1990, 2025)
        price_eur = round(random.uniform(1000, 50000), 2)
        mileage_km = random.randint(0, 500000)
        currency_code = "EUR"
        added_at = datetime.now() - timedelta(days=random.randint(0, 365))
        expired_at = added_at + timedelta(days=365)
        country_code = random.choice(european_country_codes)

        row = (
            mark_id,
            model_id,
            category_id,
            body_id,
            state_id,
            dealer_id,
            year,
            price_eur,
            mileage_km,
            currency_code,
            added_at,
            expired_at,
            country_code,
        )
        rows.append(row)

    print(f"✅ Сгенерировано строк: {len(rows)}")
    print(f"⛔ Пропущено по моделям: {skipped_models}")
    print(f"⛔ Пропущено по бодистилям: {skipped_bodystyles}")

    if len(rows) == 0:
        print("⛔ Вставка отменена: нет данных.")
        return []

    return rows

print("Генерируем данные...")
rows = generate_rows(100000)

if rows:
    print("Вставляем данные в базу...")
    insert_sql = """
    INSERT INTO fact_vehicles_eur (
        mark_id, model_id, category_id, body_id, state_id, dealer_id,
        year, price_eur, mileage_km, currency_code, added_at, expired_at, country_code
    ) VALUES %s
    """
    execute_values(cur, insert_sql, rows, page_size=1000)
    conn.commit()

cur.close()
conn.close()

print("Генерация и загрузка данных завершена.")

!pip install psycopg2-binary

import os
import random
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

# --- Конфигурация подключения ---
pg_host = os.getenv("supabase_host")
pg_user = os.getenv("supabase_user")
pg_password = os.getenv("supabase_password")
pg_dbname = os.getenv("supabase_name")
pg_port = os.getenv("supabase_port", 5432)

# --- Коды стран Европы ---
european_country_codes = [56, 100, 208, 233, 380, 428, 440, 528, 276, 616, 642, 246, 250, 203, 752]

# --- Подключение к БД ---
def connect_db():
    return psycopg2.connect(
        host=pg_host,
        user=pg_user,
        password=pg_password,
        dbname=pg_dbname,
        port=pg_port
    )

# --- Получение реальных марок и моделей ---
def fetch_marks_models(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT mark_id, model_id FROM factvehicles_new WHERE category_id = 5 AND mark_id IS NOT NULL AND model_id IS NOT NULL;")
        return cur.fetchall()

# --- Генерация фейковых строк ---
def generate_fake_data(marks_models, start_id=9000000, count=1000):
    data = []
    for i in range(count):
        vehicle_id = start_id + i
        mark_id, model_id = random.choice(marks_models)
        category_id = 5
        body_id = random.choice([149, 150, 151, 152, 303])
        city_id = None
        state_id = None
        dealer_id = random.randint(0, 5)
        year = random.randint(1990, 2022)
        price_eur = round(random.uniform(500.0, 15000.0), 2)
        price_usd = None
        price_uah = None
        mileage_km = random.randint(0, 250000)
        currency_code = 'EUR'
        added_at = datetime.now() - timedelta(days=random.randint(30, 180))
        expired_at = added_at + timedelta(days=random.randint(10, 90))
        country_code = random.choice(european_country_codes)

        data.append((
            vehicle_id, mark_id, model_id, category_id, body_id, city_id,
            state_id, dealer_id, year, price_eur, price_usd, price_uah,
            mileage_km, currency_code, added_at, expired_at, country_code
        ))
    return data

# --- Вставка в базу ---
def insert_batch(conn, data_batch):
    with conn.cursor() as cur:
        sql = """
            INSERT INTO factvehicles_new (
                vehicle_id, mark_id, model_id, category_id, body_id,
                city_id, state_id, dealer_id, year,
                price_eur, price_usd, price_uah, mileage_km,
                currency_code, added_at, expired_at, country_code
            )
            VALUES %s
        """
        execute_values(cur, sql, data_batch)
    conn.commit()

# --- Главный процесс ---
def main():
    conn = connect_db()
    try:
        print("🔄 Загружаем mark_id и model_id...")
        marks_models = fetch_marks_models(conn)
        if not marks_models:
            print("⛔ Нет данных по маркам и моделям.")
            return

        print(f"✅ Найдено {len(marks_models)} уникальных пар mark_id + model_id.")
        print("⚙️ Генерируем фейковые данные...")
        fake_data = generate_fake_data(marks_models, start_id=9000000, count=1000)

        print("📤 Вставляем в базу данных...")
        insert_batch(conn, fake_data)

        print(f"🎉 Успешно вставлено {len(fake_data)} строк.")
    finally:
        conn.close()
        print("🔒 Соединение закрыто.")

# --- Запуск ---
if __name__ == "__main__":
    main()


#генерація другої таблички для порівняння

import os
import psycopg2
from psycopg2.extras import execute_values
import random
from datetime import datetime, timedelta

# --- Параметры подключения к Supabase Postgres из переменных окружения ---
pg_host = os.getenv("supabase_host")
pg_user = os.getenv("supabase_user")
pg_password = os.getenv("supabase_password")
pg_dbname = os.getenv("supabase_name")
pg_port = os.getenv("supabase_port", 5432)

# --- Европейские коды стран для генерации ---
european_country_codes = [56, 100, 208, 233, 380, 428, 440, 528, 276, 616, 642, 246, 250, 203, 752]

def connect_db():
    return psycopg2.connect(
        host=pg_host,
        user=pg_user,
        password=pg_password,
        dbname=pg_dbname,
        port=pg_port
    )

def fetch_source_data(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT mark_id, model_id, category_id, body_id, dealer_id, year, price_eur, mileage_km, currency_code, added_at, expired_at
            FROM factvehicles_new
            WHERE category_id = 5 AND price_eur IS NOT NULL
        """)
        return cur.fetchall()

def generate_random_date(start_date, end_date):
    delta = end_date - start_date
    return start_date + timedelta(days=random.randint(0, delta.days))

def generate_fake_rows(source_data, target_rows):
    fake_rows = []
    now = datetime.now()
    expire_min = now + timedelta(days=30)
    expire_max = now + timedelta(days=90)

    for _ in range(target_rows):
        src = random.choice(source_data)
        mark_id, model_id, category_id, body_id, dealer_id, year, price_eur, mileage_km, currency_code, added_at, expired_at = src

        country_code = random.choice(european_country_codes)

        price_eur_float = float(price_eur)
        price_eur_new = round(price_eur_float * random.uniform(0.8, 1.2), 2)

        price_usd_new = None
        price_uah_new = None

        currency_code_new = "EUR"

        added_at_new = added_at if added_at else now
        expired_at_new = generate_random_date(expire_min, expire_max)

        fake_rows.append((
            mark_id,
            model_id,
            category_id,
            body_id,
            dealer_id,
            year,
            price_eur_new,
            price_usd_new,
            price_uah_new,
            mileage_km,
            currency_code_new,
            added_at_new,
            expired_at_new,
            country_code
        ))

    return fake_rows

def insert_batch(conn, data_batch):
    with conn.cursor() as cur:
        sql = """
            INSERT INTO factvehicles_comparison (
                mark_id, model_id, category_id, body_id,
                dealer_id, year,
                price_eur, price_usd, price_uah, mileage_km,
                currency_code, added_at, expired_at, country_code
            ) VALUES %s
        """
        execute_values(cur, sql, data_batch)
    conn.commit()

def main():
    print("🔄 Подключаемся к базе данных...")
    conn = connect_db()

    print("📥 Загружаем исходные данные из factvehicles_new...")
    source_data = fetch_source_data(conn)
    if not source_data:
        print("❌ Нет данных для генерации. Завершаем.")
        conn.close()
        return
    print(f"✅ Загружено {len(source_data)} строк с category_id=5")

    total_rows_to_generate = 100_000
    batch_size = 1000
    generated_rows = 0

    print("⚙️ Генерируем и загружаем фейковые данные партиями...")
    while generated_rows < total_rows_to_generate:
        rows_left = total_rows_to_generate - generated_rows
        current_batch_size = batch_size if rows_left >= batch_size else rows_left
        batch = generate_fake_rows(source_data, current_batch_size)
        insert_batch(conn, batch)
        generated_rows += current_batch_size
        print(f"📤 Вставлено строк: {generated_rows}")

    print("🎉 Генерация и загрузка данных завершена.")
    conn.close()
    print("🔒 Соединение закрыто.")

if __name__ == "__main__":
    main()
    
