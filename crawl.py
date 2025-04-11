import requests

def crawl_cat_data():
    url = "https://api.thecatapi.com/v1/breeds"
    res = requests.get(url)
    data = res.json()

    cats = []
    for cat in data:
        cats.append({
            "id": cat["id"],
            "name": cat["name"],
            "origin": cat.get("origin", ""),
            "temperament": cat.get("temperament", ""),
            "life_span": cat.get("life_span", ""),
            "image_url": cat.get("image", {}).get("url", "")
        })
    return cats

if __name__ == "__main__":
    cats = crawl_cat_data()
    print(f"Đã lấy được {len(cats)} giống mèo.")
    print(cats[:2])  # In 2 dòng đầu để kiểm tra
