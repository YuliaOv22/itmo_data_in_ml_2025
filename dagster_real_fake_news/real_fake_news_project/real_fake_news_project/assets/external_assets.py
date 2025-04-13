import dagster as dg
import pandas as pd
import importlib.util
from pathlib import Path
import sys
from contextlib import contextmanager
import builtins

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
            ria_module.main()
            
        df = pd.read_csv("ria_news_data.csv")
        context.log.info(f"Успешно загружено {len(df)} записей из РИА Новостей")
        return df
    except Exception as e:
        context.log.error(f"Ошибка в ria_news_asset: {str(e)}")
        raise

@dg.asset(group_name="external")
def lentaru_news_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Парсинг Lenta.ru"""
    try:
        with redirect_print_to_log(context.log.info):
            lenta_module = import_external_script("get_lenta_news", context)
            lenta_module.main()
            
        df = pd.read_csv("lentaru_news_data.csv")
        context.log.info(f"Успешно загружено {len(df)} записей из Lenta.ru")
        return df
    except Exception as e:
        context.log.error(f"Ошибка в lentaru_news_asset: {str(e)}")
        raise