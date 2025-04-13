from dagster import Definitions, load_assets_from_modules

from real_fake_news_project import assets  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
)
