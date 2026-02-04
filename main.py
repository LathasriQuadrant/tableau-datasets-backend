from fastapi import FastAPI
from pydantic import BaseModel
import uuid, tempfile, os
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Response

from azure_blob import download_twbx, upload_csv
from extractor import extract_from_twbx

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://id-preview--1115fb10-6ea8-4052-8d1b-31238016c02e.lovable.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def health():
    return {"status": "ok"}

@app.options("/{path:path}")
def options_handler(path: str):
    return Response(status_code=204)

class ExtractRequest(BaseModel):
    blob_path: str  # path inside tableau-raw container

# @app.post("/extract-data")
# def extract_data(req: ExtractRequest):
#     job_id = str(uuid.uuid4())
#     work_dir = tempfile.mkdtemp()

#     # Workbook name from file
#     workbook_name = os.path.splitext(
#         os.path.basename(req.blob_path)
#     )[0]

#     # Download TWBX (ALWAYS from tableau-raw)
#     local_twbx = os.path.join(work_dir, "input.twbx")
#     download_twbx(req.blob_path, local_twbx)

#     #  Extraction 
#     result = extract_from_twbx(
#         local_twbx,
#         work_dir,
#         workbook_name
#     )

#     # Upload CSVs
#     uploaded = []

#     for csv in result["csv_files"]:
#         blob_name = f"{workbook_name}/{os.path.basename(csv)}"
#         uploaded.append(upload_csv(csv, blob_name))

#     return {
#         "job_id": job_id,
#         "workbook": workbook_name,
#         "output_container": "tableau-datasources",
#         "output_folder": workbook_name,
#         "output_files": uploaded,
#         "tables": result["tables"]
#     }

@app.post("/extract-data")
def extract_data(req: ExtractRequest):
    import traceback

    print("===== /extract-data called =====")
    print("Request body:", req)

    try:
        print("Generating job ID")
        job_id = str(uuid.uuid4())
        print("Job ID:", job_id)

        print("Creating temp directory")
        work_dir = tempfile.mkdtemp()
        print("Work dir:", work_dir)

        print("Blob path received:", req.blob_path)

        workbook_name = os.path.splitext(
            os.path.basename(req.blob_path)
        )[0]
        print("Workbook name:", workbook_name)

        local_twbx = os.path.join(work_dir, "input.twbx")
        print("Local TWBX path:", local_twbx)

        print("Calling download_twbx()")
        download_twbx(req.blob_path, local_twbx)
        print("Download completed")

        print("Calling extract_from_twbx()")
        result = extract_from_twbx(
            local_twbx,
            work_dir,
            workbook_name
        )
        print("Extraction completed")

        print("CSV files:", result.get("csv_files"))

        uploaded = []
        for csv in result.get("csv_files", []):
            print("Uploading CSV:", csv)
            blob_name = f"{workbook_name}/{os.path.basename(csv)}"
            uploaded.append(upload_csv(csv, blob_name))

        print("Upload completed")

        return {
            "job_id": job_id,
            "workbook": workbook_name,
            "output_files": uploaded,
            "tables": result.get("tables"),
        }

    except Exception as e:
        print("❌ ERROR OCCURRED")
        traceback.print_exc()
        return {"error": str(e)}

