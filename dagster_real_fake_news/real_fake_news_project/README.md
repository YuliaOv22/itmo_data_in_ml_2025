## Запуск проекта

Склонируйте репозиторий

```python
git clone <repository_url>
cd /dagster_real_fake_news/real_fake_news_project
```

Установите зависимости (в проекте используется `conda`-окружение)

```python
pip install -r requirements.txt
```

Запустите Dagster UI

```python
dagster dev
```

Откройте http://localhost:3000

Пайплайн можно запустить вручную из папки `real_fake_news_project`
```python
dagster job execute -m real_fake_news_project.definitions -j all_assets_job
```
