def transform_cat_data(cats):
    default_img = "https://cdn2.thecatapi.com/images/aae.jpg"
    for cat in cats:
        # Chuyển temperament thành list
        if cat["temperament"]:
            cat["temperament"] = [t.strip() for t in cat["temperament"].split(",")]
        else:
            cat["temperament"] = []

        # Gán ảnh mặc định nếu thiếu
        if not cat["image_url"]:
            cat["image_url"] = default_img
    return cats

if __name__ == "__main__":
    from crawl import crawl_cat_data
    raw = crawl_cat_data()
    clean = transform_cat_data(raw)
    print(clean[:2])
