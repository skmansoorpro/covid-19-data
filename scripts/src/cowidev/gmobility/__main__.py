import os

from cowidev import PATHS
from cowidev.gmobility.etl import run_etl
from cowidev.gmobility.grapher import run_grapheriser, run_db_updater
from cowidev.gmobility._parser import _parse_args


FILE_DS = os.path.join("/tmp", "google-mobility.csv")
FILE_GRAPHER = os.path.join(PATHS.INTERNAL_GRAPHER_DIR, "Google Mobility Trends (2020).csv")
FILE_COUNTRY_STD = PATHS.INTERNAL_INPUT_GMOB_STD_FILE


def run_step(step: str):
    if step == "etl":
        run_etl(FILE_DS)
    elif step == "grapher-file":
        run_grapheriser(FILE_DS, FILE_COUNTRY_STD, FILE_GRAPHER)
    elif step == "grapher-db":
        run_db_updater(FILE_GRAPHER)


if __name__ == "__main__":
    args = _parse_args()
    run_step(args.step)
