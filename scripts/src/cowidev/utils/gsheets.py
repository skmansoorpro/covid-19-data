from gsheets import Sheets


class GSheetApi:
    def __init__(self, credentials) -> None:
        self.credentials = credentials
        self.sheets = Sheets.from_files(credentials)

    def get_sheet(self, sheet_id):
        return self.sheets.get(sheet_id)

    def get_worksheet(self, sheet_id, worksheet_title):
        sheet = self.sheets.get(sheet_id)
        try:
            return sheet.find(worksheet_title)
        except KeyError:
            raise KeyError(f"No worksheet with title {worksheet_title} was found.")