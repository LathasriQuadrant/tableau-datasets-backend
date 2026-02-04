# from twbx_handler import extract_hyper_from_twbx
# from hyper_reader import extract_hyper_to_csv


# def extract_from_twbx(twbx_path: str, output_dir: str, workbook_name: str):
#     hyper_path = extract_hyper_from_twbx(twbx_path, output_dir)
#     return extract_hyper_to_csv(hyper_path, output_dir, workbook_name)


from twbx_handler import extract_hyper_from_twbx
from hyper_reader import extract_hyper_to_csv


def extract_from_twbx(twbx_path: str, output_dir: str, workbook_name: str):
    hyper_path = extract_hyper_from_twbx(twbx_path, output_dir)

    # Extract exists → proceed normally
    return extract_hyper_to_csv(hyper_path, output_dir, workbook_name)
