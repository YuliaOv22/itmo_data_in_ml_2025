import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
from tqdm import tqdm
import time
import random

BASE_URL = "https://lenta.ru"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def parse_news_page(page_url):
    news_items = []
    try:
        response = requests.get(page_url, headers=HEADERS)
        soup = BeautifulSoup(response.text, "html.parser")

        news_blocks = soup.find_all("ul", class_="archive-page__container")

        for item in news_blocks:
            try:
                url = (
                    urljoin(
                        BASE_URL,
                        item.find("a", class_="card-full-news _archive")["href"],
                    )
                    if item.find("a")
                    else None
                )
                title = item.find("h3").text.strip() if item.find("h3") else None
                time = item.find("time").text.strip()
                tag = item.find(
                    "span", class_="card-full-news__info-item card-full-news__rubric"
                ).text.strip()
                response_page = requests.get(url, headers=HEADERS)

                soup_page = BeautifulSoup(response_page.text, "html.parser")
                news_blocks_page = soup_page.find("div", class_="topic-body__content")
                text = [" ".join(item.text.strip() for item in news_blocks_page)]

                news_items.append(
                    {
                        "url": url,
                        "title": title,
                        "full_text": text,
                        "date_published": time,
                        "views": None,
                        "tags": [tag],
                        "page": None,
                        "target": "true",  # Ставим метку, что новость относится к реальной
                    }
                )

            except Exception as e:
                print(f"Ошибка парсинга элемента: {e}")
                continue

    except Exception as e:
        print(f"Ошибка загрузки страницы {page_url}: {e}")

    return news_items


def generate_date_urls(start_year, start_month, end_month):

    date_urls = []
    for month in range(start_month, end_month + 1):
        for day in range(13, 14):  # Проверяем все возможные дни
            date_urls.append(f"{BASE_URL}/{start_year}/{month:02d}/{day:02d}/")
            [
                date_urls.append(
                    f"{BASE_URL}/{start_year}/{month:02d}/{day:02d}/page/{i}/"
                )
                for i in range(1, 10)
            ]

    return date_urls


def main():
    all_news = []

    # Генерируем URL по датам
    date_urls = generate_date_urls(start_year=2025, start_month=4, end_month=4)

    try:
        for page_url in tqdm(date_urls, desc="Ссылок для парсинга"):
            news = parse_news_page(page_url)
            all_news.append(news)
            time.sleep(random.uniform(0.5, 1.5))
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        df = pd.DataFrame(all_news)
        df.to_csv("data/lentaru_news_data.csv", index=False)
        print(f"Спарсено {len(df)} новостей")

        return df


if __name__ == "__main__":
    main()
