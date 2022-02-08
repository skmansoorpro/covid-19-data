import os
import webbrowser
import pyperclip
from cowidev.utils.log import get_logger
from cowidev.megafile.generate import generate_megafile
from cowidev.utils import paths

logger = get_logger()


def main_export(url):
    main_source_table_html(url)
    main_megafile()


def main_source_table_html(url):
    # Read html content
    print("-- Reading HTML table... --")
    with open(paths.INTERNAL_OUTPUT_VAX_TABLE_FILE, "r") as f:
        html = f.read()
    logger.info("Redirecting to owid editing platform...")
    try:
        pyperclip.copy(html)
        webbrowser.open(url)
    except:
        print(f"Can't copy content and open browser. Please visit {url} and copy the content from {path}")


def main_megafile():
    """Executes scripts/scripts/megafile.py."""
    print("-- Generating megafiles... --")
    generate_megafile()
