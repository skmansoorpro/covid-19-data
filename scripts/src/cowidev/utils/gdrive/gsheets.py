import os
from gsheets import Sheets

from cowidev.utils.gdrive.credentials import CLIENT_SECRETS_PATH, CREDENTIALS_PATH

# CREDENTIALS_PATH


class GSheetApi:
    def __init__(self, clients_secrets=CLIENT_SECRETS_PATH, credentials=CREDENTIALS_PATH) -> None:
        self.credentials = clients_secrets
        self._init_config_folder(credentials=credentials)
        self.sheets = Sheets.from_files(clients_secrets, credentials, no_webserver=True)

    def _init_config_folder(self, credentials):
        credentials_folder = os.path.expanduser(os.path.dirname(credentials))
        if not os.path.isdir(credentials_folder):
            os.makedirs(credentials_folder, exist_ok=True)

    def get_sheet(self, sheet_id):
        return self.sheets.get(sheet_id)

    def get_worksheet(self, sheet_id, worksheet_title):
        sheet = self.sheets.get(sheet_id)
        try:
            return sheet.find(worksheet_title)
        except KeyError:
            raise KeyError(f"No worksheet with title {worksheet_title} was found.")
