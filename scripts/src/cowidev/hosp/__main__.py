import os

from cowidev.hosp.etl import run_etl
from cowidev.hosp.grapher import run_grapheriser, run_db_updater
from cowidev.hosp._parser import _parse_args
from cowidev import PATHS


FILE_DS = PATHS.DATA_HOSP_MAIN_FILE
FILE_LOCATIONS = PATHS.DATA_HOSP_META_FILE
FILE_GRAPHER = os.path.join(PATHS.INTERNAL_GRAPHER_DIR, "COVID-2019 - Hospital & ICU.csv")


def run_step(args):
    if args.step == "etl":
        run_etl(FILE_DS, FILE_LOCATIONS, args.monothread, args.njobs)
    elif args.step == "grapher-file":
        run_grapheriser(FILE_DS, FILE_GRAPHER)
    elif args.step == "grapher-db":
        run_db_updater(FILE_GRAPHER)


if __name__ == "__main__":
    args = _parse_args()
    run_step(args)
