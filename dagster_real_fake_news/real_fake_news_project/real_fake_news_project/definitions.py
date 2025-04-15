import dagster as dg
from .assets.assets import (
    ria_news_asset,
    lentaru_news_asset,
    provereno_news_asset,
    fake_news_links_from_provereno_asset,
    fake_tg_news_asset,
    concat_news_asset,
    clean_news_asset,
)

# Job для выполнения всех ассетов (автоматически учитывает зависимости)
all_assets_job = dg.define_asset_job(
    name="all_assets_job", selection="*"  # Включает все ассеты
)

# Каждые 5 минут
frequent_schedule = dg.ScheduleDefinition(
    name="frequent_schedule", job=all_assets_job, cron_schedule="*/5 * * * *"
)


defs = dg.Definitions(
    assets=[
        ria_news_asset,
        lentaru_news_asset,
        provereno_news_asset,
        fake_news_links_from_provereno_asset,
        fake_tg_news_asset,
        concat_news_asset,
        clean_news_asset,
    ],
    jobs=[all_assets_job],
    resources={"io_manager": dg.FilesystemIOManager(base_dir="data/dagster_storage")},
    schedules=[frequent_schedule],
)
