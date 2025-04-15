import re
import pandas as pd
import json


def main(df: pd.DataFrame = None):
    """
    Распределение меток:
        Фейк                         209
        Неправда                     152
        Большей частью неправда       59
        Полуправда                    30
        Неверная атрибуция цитаты     28
        Скорее всего, неправда        26
        Сатирические новости          24
        Вырвано из контекста          20
        Правда                        14
        Это не точно                  13
        Большей частью правда          9
        Заблуждение                    6
        Скорее всего, правда           5
        Искажённая цитата              4
        Легенда                        2
        Мошенничество                  1
        Cатирические новости           1
        Верная цитата                  1
        Name: label, dtype: int64
    """

    # Список значений, которые нужно исключить
    exclude_verdicts = [
        "Скорее всего, правда",
        "Сатирические новости",
        "Правда",
        "Большей частью правда",
        "Cатирические новости",
        "Верная цитата",
    ]

    filtered_df = df[~df["verdict"].isin(exclude_verdicts)]

    new_rows = []

    for idx, row in filtered_df.iterrows():

        # Получаем данные из текущей строки
        original_index = idx
        tags = row["tags"]
        sources = row["sources_dict"]

        # Создаем новую строку для каждого источника
        if isinstance(sources, dict):
            for source_text, link in sources.items():
                new_rows.append(
                    {
                        "index_source": original_index,
                        "tags": tags,
                        "link": link,
                        "label": "fake",
                    }
                )

    new_df = pd.DataFrame(new_rows, columns=["index_source", "tags", "link", "label"])
    new_df.to_csv("data/fake_news_links.csv", index=False, encoding="utf-8-sig")
    print(f"Ссылки на новости сохранены. Количетсво: {new_df.shape[0]}.")

    return new_df


if __name__ == "__main__":
    df = pd.read_csv("data/provereno_news_data.csv")
    main(df)
