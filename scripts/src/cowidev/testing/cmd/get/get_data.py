import time
import importlib

from joblib import Parallel, delayed
import pandas as pd

from cowidev.testing.countries import MODULES_NAME
from cowidev.utils.log import get_logger, print_eoe


# Logger
logger = get_logger()


class CountryDataGetter:
    def __init__(self, skip_countries: list = []):
        self.skip_countries = skip_countries

    def _skip_module(self, module_name):
        country = module_name.split(".")[-1]
        return country.lower() in self.skip_countries

    def run(self, module_name: str):
        t0 = time.time()
        # Check country skipping
        if self._skip_module(module_name):
            logger.info(f"TEST - {module_name}: skipped! ⚠️")
            return {"module_name": module_name, "success": None, "skipped": True, "time": None}
        # Start country scraping
        logger.info(f"TEST - {module_name}: started")
        module = importlib.import_module(module_name)
        try:
            module.main()
        except Exception as err:
            success = False
            logger.error(f"TEST - {module_name}: ❌ {err}", exc_info=True)
        else:
            success = True
            logger.info(f"TEST - {module_name}: SUCCESS ✅")
        t = round(time.time() - t0, 2)
        return {"module_name": module_name, "success": success, "skipped": False, "time": t}


def main_get_data(
    parallel: bool = False,
    n_jobs: int = -2,
    modules_name: list = MODULES_NAME,
    skip_countries: list = [],
):
    """Get data from sources and export to output folder.

    Is equivalent to script `run_python_scripts.py`
    """
    t0 = time.time()
    print("-- Getting data... --")
    skip_countries = skip_countries if skip_countries else []
    skip_countries = [x.lower() for x in skip_countries]
    country_data_getter = CountryDataGetter(skip_countries)
    if parallel:
        modules_execution_results = Parallel(n_jobs=n_jobs, backend="threading")(
            delayed(country_data_getter.run)(
                module_name,
            )
            for module_name in modules_name
        )
    else:
        modules_execution_results = []
        for module_name in modules_name:
            modules_execution_results.append(
                country_data_getter.run(
                    module_name,
                )
            )
    t_sec_1 = round(time.time() - t0, 2)
    # Get timing dataframe
    df_exec = _build_df_execution(modules_execution_results)
    # Retry failed modules
    _retry_modules_failed(modules_execution_results, country_data_getter)
    # Print timing details
    t_sec_1, t_min_1, t_sec_2, t_min_2 = _print_timing(t0, t_sec_1, df_exec)
    print_eoe()


def _build_df_execution(modules_execution_results):
    df_exec = (
        pd.DataFrame(
            [
                {"module": m["module_name"], "execution_time (sec)": m["time"], "success": m["success"]}
                for m in modules_execution_results
            ]
        )
        .set_index("module")
        .sort_values(by="execution_time (sec)", ascending=False)
    )
    return df_exec


def _retry_modules_failed(modules_execution_results, country_data_getter):
    modules_failed = [m["module_name"] for m in modules_execution_results if m["success"] is False]
    logger.info(f"\n---\n\nRETRIES ({len(modules_failed)})")
    modules_execution_results = []
    for module_name in modules_failed:
        modules_execution_results.append(country_data_getter.run(module_name))
    modules_failed_retrial = [m["module_name"] for m in modules_execution_results if m["success"] is False]
    if len(modules_failed_retrial) > 0:
        failed_str = "\n".join([f"* {m}" for m in modules_failed_retrial])
        print(f"\n---\n\nFAILED\nThe following scripts failed to run ({len(modules_failed_retrial)}):\n{failed_str}")


def _print_timing(t0, t_sec_1, df_time):
    t_min_1 = round(t_sec_1 / 60, 2)
    t_sec_2 = round(time.time() - t0, 2)
    t_min_2 = round(t_sec_2 / 60, 2)
    print("---")
    print("TIMING DETAILS")
    print(f"Took {t_sec_1} seconds (i.e. {t_min_1} minutes).")
    print(f"Top 20 most time consuming scripts:")
    print(df_time[["execution_time (sec)"]].head(20))
    print(f"\nTook {t_sec_2} seconds (i.e. {t_min_2} minutes) [AFTER RETRIALS].")
    print("---")
    return t_sec_1, t_min_1, t_sec_2, t_min_2
