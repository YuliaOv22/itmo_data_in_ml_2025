import dagster as dg
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pandas as pd
import time
from typing import List, Dict, Optional
from pydantic import Field

logger = dg.get_dagster_logger()

# ==================== Ресурсы ====================
class SeleniumResource(dg.ConfigurableResource):
    """Ресурс для управления Selenium WebDriver"""
    headless: bool = Field(default=True, description="Запуск в headless-режиме")
    user_agent: str = Field(
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        description="User-Agent для браузера"
    )

    def setup_for_execution(self, context: dg.AssetExecutionContext) -> webdriver.Chrome:
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument(f"user-agent={self.user_agent}")
        options.add_argument("--start-maximized")
        
        driver = webdriver.Chrome(options=options)
        context.log.info("Selenium WebDriver инициализирован")
        return driver

    def teardown_after_execution(self, context: dg.AssetExecutionContext, resource: webdriver.Chrome):
        resource.quit()
        context.log.info("Selenium WebDriver закрыт")

# ==================== Ассеты ====================
@dg.asset(description="Сырые данные новостей с РИА Новости")
def ria_news_raw(context: dg.AssetExecutionContext, selenium: SeleniumResource) -> List[Dict]:
    driver = selenium.setup_for_execution(context)
    parsed_data = []
    
    try:
        driver.get("https://ria.ru/archive/")
        
        page_num = 1
        while page_num < 3:  # Для демо - только 3 страницы
            context.log.info(f"Обработка страницы {page_num}...")
            
            items = driver.find_elements(By.CSS_SELECTOR, "div.list-item")
            for item in items:
                try:
                    # Парсинг основных данных
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
                    
                    # Парсинг полного текста статьи
                    article_data = _parse_article_page(context, driver, url)
                    
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
                    context.log.warning(f"Ошибка при парсинге элемента: {str(e)}")
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
                time.sleep(4)

            except TimeoutException:
                print("Достигнут конец списка материалов")
                break

            except Exception as e:
                print(f"Ошибка при подгрузке: {str(e)}")
                break
                
    finally:
        context.log.info(f"Сохранено {len(parsed_data)} новостей с полным текстом")
        selenium.teardown_after_execution(context, driver)
        return parsed_data





@dg.asset(description="Очищенные и структурированные новости")
def ria_news_processed(context: dg.AssetExecutionContext, ria_news_raw: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(ria_news_raw)
    
    # # Очистка данных
    # df = df.dropna(subset=["full_text"])
    # df = df.drop_duplicates(subset=["url"])
    
    # # Генерация метаданных
    # df["text_length"] = df["full_text"].str.len()
    # df["target"] = True  # Метка для реальных новостей
    
    context.add_output_metadata({
        "num_records": len(df),
        "columns": str(list(df.columns))
    })
    
    return df

# ==================== Вспомогательные функции ====================
def _parse_article_page(context: dg.AssetExecutionContext, driver: webdriver.Chrome, url: str) -> Dict:
    try:
        driver.execute_script(f"window.open('{url}', '_blank');")
        driver.switch_to.window(driver.window_handles[1])

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.article__text"))
        )

        text_blocks = driver.find_elements(By.CSS_SELECTOR, "div.article__text")
        article_text = "\n\n".join([block.text for block in text_blocks])
        date_published = driver.find_element(
            By.CSS_SELECTOR, "div.article__info-date a"
        ).text

        # Проверяем, есть ли вторая вкладка перед закрытием
        if len(driver.window_handles) > 1:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])

        return {"full_text": article_text, "date_published": date_published}

    except Exception as e:
        print(f"Ошибка при парсинге статьи {url}: {str(e)}")

        # Если вкладка не закрылась, пробуем переключиться обратно
        if len(driver.window_handles) > 1:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        return {"full_text": None, "date_published": None}

# ==================== Определения ====================
defs = dg.Definitions(
    assets=[ria_news_raw, ria_news_processed],
    resources={
        "selenium": SeleniumResource(headless=True)
    }
)