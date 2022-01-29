import requests
import zipfile
import io


def extract_zip(input_path, output_folder):
    if input_path.startswith("http"):
        content = requests.get(input_path).content
        z = zipfile.ZipFile(io.BytesIO(content))
    else:
        z = zipfile.ZipFile(input_path)
    z.extractall(output_folder)
