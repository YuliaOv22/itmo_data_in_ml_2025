import dagster as dg
from ..assets.external_assets import ria_news_asset, lentaru_news_asset

@dg.op
def preprocess_ria(context):
    """Дополнительная обработка перед запуском"""
    context.log.info("Подготовка к парсингу РИА")
    return ria_news_asset()

@dg.op
def preprocess_lentaru(context):
    """Дополнительная обработка перед запуском"""
    context.log.info("Подготовка к парсингу Ленты.ру")
    return lentaru_news_asset()

# @dg.job
# def run_external_scripts_job():
#     """Джоб для последовательного выполнения"""
#     preprocess_ria()
#     preprocess_lentaru()

@dg.job
def parallel_scraping():
    """Параллельное выполнение"""
    preprocess_ria()
    preprocess_lentaru()

# @dg.job
# def run_ria_only_job():
#     """Джоб для запуска только РИА"""
#     preprocess_ria()