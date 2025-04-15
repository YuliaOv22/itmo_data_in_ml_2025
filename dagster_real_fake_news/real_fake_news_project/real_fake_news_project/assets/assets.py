import dagster as dg
import pandas as pd
import importlib.util
from pathlib import Path
import sys
from contextlib import contextmanager
import builtins
from datetime import datetime
import json
import re
from urllib.parse import urlparse
from transformers import pipeline
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from pymorphy3 import MorphAnalyzer


@contextmanager
def redirect_print_to_log(logger_func):
    """Контекстный менеджер для временного перенаправления print в логгер"""
    original_print = builtins.print
    builtins.print = lambda *args: logger_func(" ".join(map(str, args)))
    try:
        yield
    finally:
        builtins.print = original_print


def import_external_script(script_name: str, context: dg.AssetExecutionContext):
    """Импорт внешнего скрипта"""
    script_path = Path(__file__).parents[2] / "external_scripts" / f"{script_name}.py"

    if not script_path.exists():
        context.log.error(f"Скрипт {script_path} не найден")
        raise FileNotFoundError(f"Скрипт {script_path} не найден")

    try:
        spec = importlib.util.spec_from_file_location(script_name, script_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[script_name] = module
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        context.log.error(f"Ошибка загрузки скрипта {script_name}: {str(e)}")
        raise


@dg.asset(group_name="external")
def ria_news_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Парсинг РИА Новости"""
    try:
        with redirect_print_to_log(context.log.info):
            ria_module = import_external_script("get_ria_news", context)
            data = ria_module.main()
        context.log.info(f"Успешно загружено {len(data)} записей из РИА Новости")
        return data
    except Exception as e:
        context.log.error(f"Ошибка в ria_news_asset: {str(e)}")
        raise


@dg.asset(group_name="external")
def lentaru_news_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Парсинг Lenta.ru"""
    try:
        with redirect_print_to_log(context.log.info):
            lenta_module = import_external_script("get_lenta_news", context)
            data = lenta_module.main()
        context.log.info(f"Успешно загружено {len(data)} записей из Lenta.ru")
        return data
    except Exception as e:
        context.log.error(f"Ошибка в lentaru_news_asset: {str(e)}")
        raise


@dg.asset(group_name="external")
def provereno_news_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Парсинг Проверено"""
    try:
        with redirect_print_to_log(context.log.info):
            provereno_module = import_external_script("get_provereno_news", context)
            data = provereno_module.main()

        context.log.info(f"Успешно загружено {len(data)} записей")
        return data
    except Exception as e:
        context.log.error(f"Ошибка: {str(e)}")
        raise


@dg.asset(group_name="external")
def fake_news_links_from_provereno_asset(
    context: dg.AssetExecutionContext,
    provereno_news_asset: pd.DataFrame = None,  # Делаем опциональным
) -> pd.DataFrame:
    """Получение данных из Проверено и обработка их для получения ссылок на фейковые новости"""
    try:
        if provereno_news_asset is not None:
            context.log.info("Используем данные из ассета")
            input_data = provereno_news_asset
        else:
            context.log.info("Загружаем данные из файла")
            input_path = Path("data/provereno_news_data.csv")
            input_data = pd.read_csv(input_path)

        # Обработка данных
        with redirect_print_to_log(context.log.info):
            fake_news_links_module = import_external_script(
                "get_fake_news_links", context
            )
            return fake_news_links_module.main(input_data)

    except Exception as e:
        context.log.error(f"Ошибка обработки: {str(e)}")
        raise


@dg.asset(group_name="external")
def fake_tg_news_asset(
    context: dg.AssetExecutionContext,
    fake_news_links_from_provereno_asset: pd.DataFrame = None,  # Делаем опциональным
) -> pd.DataFrame:
    """Парсинг ссылок с фейковыми новостями из Telegram-каналов"""
    try:
        if fake_news_links_from_provereno_asset is not None:
            context.log.info("Используем данные из ассета")
            input_data = fake_news_links_from_provereno_asset
        else:
            context.log.info("Загружаем данные из файла")
            input_path = Path("data/fake_news_links.csv")
            input_data = pd.read_csv(input_path)

        # Обработка данных
        with redirect_print_to_log(context.log.info):
            fake_tg_news_module = import_external_script("get_fake_tg_news", context)
            return fake_tg_news_module.main(input_data)

    except Exception as e:
        context.log.error(f"Ошибка обработки: {str(e)}")
        raise


@dg.asset(group_name="external")
def concat_news_asset(
    context: dg.AssetExecutionContext,
    ria_news_asset: pd.DataFrame = None,
    lentaru_news_asset: pd.DataFrame = None,
    fake_tg_news_asset: pd.DataFrame = None,
) -> pd.DataFrame:
    """Объединение всех источников новостей"""
    try:
        if (
            (ria_news_asset is not None)
            and (lentaru_news_asset is not None)
            and (fake_tg_news_asset is not None)
        ):
            context.log.info("Используем данные из ассета")
            ria_news_data = ria_news_asset
            lentaru_news_data = lentaru_news_asset
            fake_tg_news_data = fake_tg_news_asset

        else:
            context.log.info("Загружаем данные из файлов")
            path_list = [
                "data/ria_news.csv",
                "data/lentaru_news.csv",
                "data/tgstat_posts_fake.csv",
            ]
            ria_news_data = pd.read_csv(path_list[0]).drop("page", axis=1)
            lentaru_news_data = pd.read_csv(path_list[1])
            fake_tg_news_data = pd.read_csv(path_list[2])

        # Обработка данных из ria_news_data
        ria_news_data = ria_news_data.drop("page", axis=1)

        # Обработка данных из lentaru_news_data
        data_list = lentaru_news_data[0].tolist()
        lentaru_news_data = pd.DataFrame([x for x in data_list if x])

        # Обработка столбцов со списками
        if "full_text" in lentaru_news_data.columns:
            lentaru_news_data["full_text"] = lentaru_news_data["full_text"].apply(
                lambda x: " ".join(x) if isinstance(x, list) else x
            )
        if "tags" in lentaru_news_data.columns:
            lentaru_news_data["tags"] = lentaru_news_data["tags"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) else x
            )

        lentaru_news_data = lentaru_news_data.drop("page", axis=1)

        # # Обработка данных из fake_tg_news_data
        fake_tg_news_data["tags"] = None
        fake_tg_news_data["target"] = "false"
        fake_tg_news_data = fake_tg_news_data.drop("author", axis=1)

        # Объединение данных
        concat = pd.concat(
            [ria_news_data, lentaru_news_data, fake_tg_news_data], axis=0
        )
        concat.to_csv("data/real_fake_news_dataset.csv", encoding="utf-8-sig")

        with redirect_print_to_log(context.log.info):
            print("Данные сохранены", concat.head())

        return concat

    except Exception as e:
        context.log.error(f"Ошибка обработки: {str(e)}")
        raise


@dg.asset(group_name="external")
def clean_news_asset(
    context: dg.AssetExecutionContext,
    concat_news_asset: pd.DataFrame = None,
) -> pd.DataFrame:
    """Работа с пропусками и выбросами в данных."""
    try:
        if concat_news_asset is not None:
            context.log.info("Используем данные из ассета")
            data = concat_news_asset
        else:
            context.log.info("Загружаем данные из файлов")
            data = pd.read_csv("data/real_fake_news_dataset.csv", index_col=0)

        with redirect_print_to_log(context.log.info):
            print(f"Количество записей: {data.shape[0]}")
            print(f"Пропуски: {data.isnull().sum()}")

        # Выделение источника новостей в графу 'source'
        def extract_source(url):
            if isinstance(url, str):
                parsed_url = urlparse(url)
                return parsed_url.netloc
            return None

        def create_source_column(df, url_column="url", new_column="source"):
            df[new_column] = df[url_column].apply(extract_source)
            return df

        data = create_source_column(data)

        # Преобразование 'views' в числовой формат
        def preprocess_views(data):
            if pd.isna(data):
                return 0
            elif "k" in data:
                data = pd.to_numeric(data[:-1]) * 1000
            elif "m" in data:
                data = pd.to_numeric(data[:-1]) * 1000000
            else:
                data = pd.to_numeric(data)
            return data

        data["views_clean"] = data.views.apply(preprocess_views)

        # Преобразование даты
        def parse_custom_date(date_str):
            if pd.isna(date_str) or not isinstance(date_str, str):
                return pd.NaT

            date_str = date_str.strip()

            month_translation = {
                "января": "01",
                "февраля": "02",
                "марта": "03",
                "апреля": "04",
                "мая": "05",
                "июня": "06",
                "июля": "07",
                "августа": "08",
                "сентября": "09",
                "октября": "10",
                "ноября": "11",
                "декабря": "12",
            }

            # Формат 1: "00:00, 20 марта 2025"
            match = re.match(r"(\d{2}:\d{2}), (\d{1,2}) ([а-я]+) (\d{4})", date_str)
            if match:
                time_part, day, month, year = match.groups()
                month = month_translation.get(month.lower(), "01")
                return datetime.strptime(
                    f"{day} {month} {year} {time_part}", "%d %m %Y %H:%M"
                )

            # Формат 2: "10 Oct 2024, 15:42"
            match = re.match(
                r"(\d{1,2}) ([A-Za-z]{3}) (\d{4}), (\d{2}:\d{2})", date_str
            )
            if match:
                day, month, year, time = match.groups()
                return datetime.strptime(
                    f"{day} {month} {year} {time}", "%d %b %Y %H:%M"
                )

            # Формат 3: "09:57" (только время) - с фиксированной датой 03.04.2025
            match = re.match(r"(\d{2}:\d{2})", date_str)
            if match:
                fixed_date = datetime(2025, 4, 3).date()
                return datetime.combine(
                    fixed_date, datetime.strptime(match.group(), "%H:%M").time()
                )

            return pd.NaT

        # Применяем к DataFrame
        data["date_published"] = data["date_published"].apply(parse_custom_date)

        # Удаляем строки, где в full_text, date_published есть NaN
        data = data.dropna(subset=["full_text"])
        data = data.dropna(subset=["date_published"])

        # Удаление дублирующих новостей
        data.drop_duplicates(subset="full_text", keep="first", inplace=True)

        # Заполнение пустых строк заголовков с помощью 'Без заголовка'
        data["title"] = data.title.fillna("Без заголовка")

        # Формирование заголовков с помощью LLM + обрезка слишком длинных заголовков
        def generate_missing_titles(data, summarizer):
            if data.title in ["", "Без заголовка"]:
                summary = summarizer(data.full_text, max_length=50)[0]["summary_text"]
                return summary[:150]
            return data.title[:150]

        summarizer = pipeline(
            "summarization", model="csebuetnlp/mT5_multilingual_XLSum"
        )
        data["title_sum_llm"] = data.apply(
            generate_missing_titles, args=(summarizer,), axis=1
        )

        # Удаление новостей с просмотрами меньше 1 млн.
        popular_news = data[data.views_clean > 1000000]["views_clean"].index.tolist()
        print(f"Количество новостей с просмотром больше 1 млн.: {len(popular_news)}")
        data = data.drop(index=popular_news)

        # Заполнение пропусков в views медианой
        data["views_clean_median"] = data["views_clean"].replace(
            0, data["views_clean"].median()
        )

        # Заполнение пустых строк тегов с помощью 'Без тегов'
        data["tags"] = data.tags.fillna("Без тегов")

        with redirect_print_to_log(context.log.info):
            print(f"Количество записей: {data.shape[0]}")
            print(f"Пропуски: {data.isnull().sum()}")

        # Выделение тематических слов из текста с помощью LDA
        def strict_noun_extractor(data, n_topics=1, n_words=4):

            if data.tags == "Без тегов":
                morph = MorphAnalyzer()

                def get_nouns(text):

                    words = re.findall(r"[а-яё]+", text.lower())
                    nouns = []
                    for word in words:
                        for parsed in morph.parse(word):
                            if "NOUN" in parsed.tag:
                                try:
                                    nouns.append(parsed.inflect({"nomn"}).word)
                                    break
                                except:
                                    continue
                    if not nouns:
                        nouns.append("Невозможно определить тему")

                    return nouns

                noun_texts = get_nouns(data.full_text)

                # Проверка количества уникальных слов
                vectorizer = CountVectorizer(max_features=500)
                X = vectorizer.fit_transform(noun_texts)

                # Проверка перед обучением
                n_topics = min(n_topics, X.shape[0])  # Не больше тем, чем документов
                n_words = min(n_words, X.shape[1])  # Не больше слов, чем в словаре

                lda = LatentDirichletAllocation(n_components=n_topics, random_state=42)
                lda.fit(X)

                feature_names = vectorizer.get_feature_names_out()
                topics = []
                for topic in lda.components_:
                    topics.append(
                        [
                            str(feature_names[i])
                            for i in topic.argsort()[: -n_words - 1 : -1]
                        ]
                    )
                return topics[0]
            else:
                return data.tags

        data["tags_no_nan"] = data.apply(strict_noun_extractor, axis=1)

        # Конвертирование тегов из строки в список
        def convert_tags_list(data):
            if data == str:
                data = data.replace("'", '"')
                return json.loads(data)
            else:
                return data

        data["tags_no_nan"] = data.tags_no_nan.apply(convert_tags_list)

        # Подсчет длины текста
        def get_text_len(data):
            if pd.isna(data):
                return 0
            return len(data)

        data["full_text_len"] = data.full_text.apply(get_text_len)

        # Индексы длинных текстов
        thresholder = 2000
        long_texts_idx = data[(data.full_text_len > thresholder)].index

        # Сокращение длинных текстов
        def cut_selected_rows(df, text_col, indexes, max_len=100):
            df = df.copy()

            for i in indexes:
                if i in df.index:
                    text = str(df.loc[i, text_col])
                    if len(text) > max_len:
                        df.loc[i, text_col] = text[:max_len]

            return df

        # Обрезаем строки с индексами long_texts_idx
        data["cut_text"] = cut_selected_rows(
            data, "full_text", long_texts_idx, max_len=thresholder
        )["full_text"]

        # Удаление строк с выбросами в графе full_text
        data = data.drop(index=long_texts_idx)

        with redirect_print_to_log(context.log.info):
            print("Данные очищены")

        # Сохраняем итоговый обработанный датасет
        try:
            prev_data = pd.read_csv("data/real_fake_news_processed.csv", index_col=0)
        except FileNotFoundError:
            prev_data = pd.DataFrame(
                columns=data.columns
            )  # если файла нет, создаем пустой df
        data = pd.concat([prev_data, data], ignore_index=True).drop_duplicates(
            subset=["url", "full_text"], keep="first"
        )

        data.to_csv("data/real_fake_news_processed.csv", index=False)

        context.log.info(f"Датасет содержит {data.shape[0]} записей")
        return data

    except Exception as e:
        context.log.error(f"Ошибка обработки: {str(e)}")
        raise
