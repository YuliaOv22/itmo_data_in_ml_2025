from dagster import Definitions
from .assets.external_assets import ria_news_asset, lentaru_news_asset
from .jobs.scraping_jobs import parallel_scraping

defs = Definitions(
    assets=[ria_news_asset, lentaru_news_asset],
    jobs=[parallel_scraping]
)