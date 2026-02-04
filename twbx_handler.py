import zipfile
import os

def extract_hyper_from_twbx(twbx_path: str, output_dir: str) -> str:
    with zipfile.ZipFile(twbx_path, "r") as zip_ref:
        zip_ref.extractall(output_dir)

    for root, _, files in os.walk(output_dir):
        for file in files:
            if file.endswith(".hyper"):
                return os.path.join(root, file)

    raise RuntimeError("No .hyper file found inside TWBX")

