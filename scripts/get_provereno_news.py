from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
import time
import pandas as pd

# Настройка драйвера
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)

parsed_data = []
parsed_urls = set()  # Для отслеживания уникальных URL


def parse_article_page(url):
    try:
        driver.execute_script(f"window.open('{url}', '_blank');")
        driver.switch_to.window(driver.window_handles[1])
        result = {"sources": [], "verdict": None}

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(2)

        # 1. Парсим блок с ссылками на источники
        try:
            sources_block = driver.find_element(
                By.XPATH,
                "//p[@class='has-background' and contains(@style, 'background-color:#fef7d9')]",
            )

            links = sources_block.find_elements(By.TAG_NAME, "a")
            for link in links:
                result["sources"].append(
                    {"text": link.text, "url": link.get_attribute("href")}
                )
        except NoSuchElementException:
            pass  # Блок может отсутствовать

        # 2. Парсим вердикт
        try:
            verdict_block = driver.find_element(
                By.XPATH,
                "//h3[@class='wp-block-heading has-text-align-center' and contains(@style, 'margin-top:0')]",
            )
            result["verdict"] = verdict_block.text
        except NoSuchElementException:
            pass  # Блок может отсутствовать

        # Закрываем вкладку и возвращаемся
        driver.close()
        driver.switch_to.window(driver.window_handles[0])

        return result

    except Exception as e:
        print(f"Ошибка при парсинге статьи {url}: {str(e)}")

        # В случае ошибки закрываем вкладку, если она открыта
        if len(driver.window_handles) > 1:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        return {"sources": [], "verdict": None}


def parse_articles(page_num):
    """Парсинг статей на текущей странице"""
    articles = driver.find_elements(By.CSS_SELECTOR, "article.ebpg-grid-post")
    new_articles = []

    for article in articles:
        try:
            title_element = article.find_element(
                By.CSS_SELECTOR, "h2.ebpg-entry-title a"
            )
            title = title_element.text.strip()
            url = title_element.get_attribute("href")

            # Пропускаем уже обработанные URL
            if url in parsed_urls:
                continue

            tags = []
            try:
                tags_container = article.find_element(
                    By.CSS_SELECTOR, "div.ebpg-tags-meta"
                )
                tag_elements = tags_container.find_elements(By.TAG_NAME, "a")
                tags = [
                    tag.get_attribute("title").strip()
                    for tag in tag_elements
                    if tag.get_attribute("title")
                ]
            except NoSuchElementException:
                pass

            # Парсим дополнительную информацию со страницы статьи
            article_details = parse_article_page(url)

            new_articles.append(
                {
                    "title": title,
                    "url": url,
                    "tags": tags,
                    "sources": article_details["sources"],
                    "verdict": article_details["verdict"],
                    "page": page_num,
                }
            )

            parsed_urls.add(url)

        except Exception as e:
            print(f"Ошибка при парсинге статьи: {str(e)}")
            continue

    return new_articles


def main():
    try:
        driver.get("https://provereno.media")
        page_num = 1

        while True:
            print(f"Парсинг страницы {page_num}...")

            # Ждем загрузки статей
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "article.ebpg-grid-post")
                )
            )
            time.sleep(2)

            # Парсим текущую страницу
            new_articles = parse_articles(page_num)
            parsed_data.extend(new_articles)
            print(
                f"Найдено {len(new_articles)} новых статей. Всего: {len(parsed_data)}"
            )

            # Пробуем перейти на следующую страницу
            try:
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable(
                        (
                            By.CSS_SELECTOR,
                            "button.ebpg-pagination-item-next:not([disabled])",
                        )
                    )
                )

                # Прокручиваем к кнопке
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", next_button
                )
                time.sleep(1)

                # Кликаем через ActionChains
                ActionChains(driver).move_to_element(next_button).click().perform()
                page_num += 1

                # Ждем загрузки новой страницы
                time.sleep(3)

            except (TimeoutException, NoSuchElementException):
                print(
                    "Кнопка 'далее' не найдена или неактивна - это последняя страница"
                )
                break
            except Exception as e:
                print(f"Ошибка при переходе на следующую страницу: {str(e)}")
                break

    finally:
        df = pd.DataFrame(parsed_data)
        df["sources_dict"] = df["sources"].apply(
            lambda x: {s["text"]: s["url"] for s in x}
        )
        df.to_csv(
            "provereno_articles_with_details.csv", index=False, encoding="utf-8-sig"
        )
        print(f"Сохранено {len(df)} уникальных статей с дополнительной информацией")
        driver.quit()


if __name__ == "__main__":
    main()
