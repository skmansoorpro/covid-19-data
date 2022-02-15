from cowidev.utils.log import get_logger
from cowidev.utils.s3 import obj_to_s3
from cowidev import PATHS

from cowidev.vax.batch.latvia import Latvia


PATH_ICE = PATHS.S3_VAX_ICE_DIR
logger = get_logger()

countries = [Latvia()]


def main():
    for country in countries:
        logger.info(f"VAX - ICE - {country.location}")
        df = country.read()
        obj_to_s3(df, f"{PATH_ICE}/{country.location}.csv")


if __name__ == "__main__":
    main()
