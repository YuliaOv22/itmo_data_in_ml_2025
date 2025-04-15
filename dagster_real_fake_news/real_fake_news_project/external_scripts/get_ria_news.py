from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import pandas as pd

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)

parsed_data = []


def parse_article_page(url):
    try:
        driver.execute_script(f"window.open('{url}', '_blank');")
        driver.switch_to.window(driver.window_handles[1])

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.article__text"))
        )

        article_text = driver.find_element(By.CSS_SELECTOR, "div.article__text").text
        date_published = driver.find_element(
            By.CSS_SELECTOR, "div.article__info-date a"
        ).text

        driver.close()
        driver.switch_to.window(driver.window_handles[0])

        return {"full_text": article_text, "date_published": date_published}

    except Exception as e:
        print(f"Ошибка при парсинге статьи {url}: {str(e)}")
        driver.switch_to.window(driver.window_handles[0])
        return {"full_text": None, "date_published": None}


def main():
    try:
        driver.get("https://ria.ru/archive/")

        # Основной цикл подгрузки и парсинга
        page_num = 1
        while page_num < 2:  # 2 страницы
            print(f"Обработка страницы {page_num}...")

            items = driver.find_elements(By.CSS_SELECTOR, "div.list-item")
            for item in items:
                try:
                    title = item.find_element(
                        By.CSS_SELECTOR, "a.list-item__title.color-font-hover-only"
                    ).text.strip()
                    url = item.find_element(
                        By.CSS_SELECTOR, "a.list-item__title.color-font-hover-only"
                    ).get_attribute("href")
                    views_count = item.find_element(
                        By.XPATH, './/div[@data-type="views"]//span'
                    ).text.strip()
                    tag_elements = item.find_elements(
                        By.CSS_SELECTOR, "div.list-item__tags-list a.list-tag"
                    )

                    tags = [
                        tag.find_element(
                            By.CSS_SELECTOR, "span.list-tag__text"
                        ).text.strip()
                        for tag in tag_elements
                        if tag.is_displayed()
                    ]

                    article_data = parse_article_page(url)

                    parsed_data.append(
                        {
                            "url": url,
                            "title": title,
                            "full_text": article_data["full_text"],
                            "date_published": article_data["date_published"],
                            "views": views_count,
                            "tags": tags,
                            "page": page_num,
                            "target": "true",  # Ставим метку, что новость относится к реальной
                        }
                    )

                except Exception as e:
                    print(f"Ошибка при парсинге элемента: {str(e)}")
                    continue

            # Проверяем наличие кнопки "Еще"
            try:
                more_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "div.list-more.color-btn-second-hover")
                    )
                )

                # Прокрутка и клик
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", more_button
                )
                time.sleep(1)
                driver.execute_script("arguments[0].click();", more_button)

                # Ожидание загрузки новых элементов
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.list-item:last-child")
                    )
                )

                page_num += 1
                time.sleep(2)  # Задержка между подгрузками

            except TimeoutException:
                print("Достигнут конец списка материалов")
                break

            except Exception as e:
                print(f"Ошибка при подгрузке: {str(e)}")
                break

    finally:
        df = pd.DataFrame(parsed_data)
        df.to_csv("data/ria_news_data.csv", index=False, encoding="utf-8-sig")
        print(f"Сохранено {len(df)} новостей с полным текстом")
        driver.quit()

        return df


if __name__ == "__main__":
    main()
