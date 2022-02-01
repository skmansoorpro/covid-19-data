from cowidev.testing.cmd import main_get_data


def main():
    mode = "get"
    if "get" in mode:
        main_get_data(
            parallel=cfg.parallel,
            n_jobs=cfg.njobs,
            modules_name=cfg.countries,
            skip_countries=cfg.skip_countries,
            gsheets_api=config.gsheets_api,
        )
