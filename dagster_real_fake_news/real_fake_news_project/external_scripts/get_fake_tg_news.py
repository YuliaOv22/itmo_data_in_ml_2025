import requests
from bs4 import BeautifulSoup
import pandas as pd
import tqdm

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def parse_tgstat_post(url):
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        author = soup.find("h5").text.strip()
        date = soup.find("p", class_="text-muted m-0").text.strip()
        post_text_tag = soup.find("div", class_="post-text")
        post_text = post_text_tag.get_text("\n", strip=True) if post_text_tag else None
        views = soup.find(
            "a",
            class_="btn btn-light btn-rounded py-05 px-13 mr-1 popup_ajax font-12 font-sm-13",
        ).text.strip()

        return {
            "url": url,
            "title": None,
            "full_text": post_text,
            "date_published": date,
            "views": views,
            "author": author,
        }

    except Exception as e:
        print(f"Ошибка при парсинге {url}: {str(e)}")
        return None


def main(df):

    try:
        links = df["link"].dropna().tolist() if "link" in df.columns else []
        print(f"Найдено {len(links)} ссылок для парсинга")

        # 2. Парсим ссылки
        results = []
        for link in tqdm.tqdm(links):
            if "tgstat" in link:

                print(f"Парсим {link}...")
                post_data = parse_tgstat_post(link)
                if post_data:
                    results.append(post_data)
    finally:
        df = pd.DataFrame(results)
        df.to_csv("data/tgstat_posts_fake.csv", index=False, encoding="utf-8-sig")
        print(f"Данные сохранены")

        return df


if __name__ == "__main__":
    df = pd.read_csv("data/fake_news_links.csv")
    main(df)
