import psycopg2
import os
import requests

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cats (
                id TEXT PRIMARY KEY,
                name TEXT,
                origin TEXT,
                temperament TEXT[],
                life_span TEXT,
                image_url TEXT
            );
        """)
        conn.commit()

def save_data(conn, cats):
    with conn.cursor() as cur:
        for cat in cats:
            cur.execute("""
                INSERT INTO cats (id, name, origin, temperament, life_span, image_url)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                cat["id"], cat["name"], cat["origin"], cat["temperament"],
                cat["life_span"], cat["image_url"]
            ))
        conn.commit()

def download_images(cats, folder="data/images"):
    os.makedirs(folder, exist_ok=True)
    for cat in cats:
        filename = f"{folder}/{cat['id']}.jpg"
        try:
            response = requests.get(cat["image_url"])
            with open(filename, "wb") as f:
                f.write(response.content)
        except Exception as e:
            print(f"Lỗi tải ảnh {cat['id']}: {e}")

if __name__ == "__main__":
    from transform import transform_cat_data
    from crawl import crawl_cat_data

    # Crawl và xử lý dữ liệu
    raw = crawl_cat_data()
    cats = transform_cat_data(raw)

    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="localhost",   # chạy từ local ⇒ dùng localhost
        port="5432"
    )

    create_table(conn)
    save_data(conn, cats)
    download_images(cats)

    print("✅ Đã lưu dữ liệu và tải ảnh mèo thành công.")
