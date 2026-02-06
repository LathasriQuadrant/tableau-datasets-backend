from fastapi import FastAPI
from pydantic import BaseModel
import uuid, os
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Response
import shutil

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
    blob_path: str


def get_work_directory():
    """
    Get appropriate temp directory for current environment.
    
    - Azure: /home/site/wwwroot/temp_extractions (persistent)
    - Local: ./temp_extractions (project folder)
    - Override: Set EXTRACTION_TEMP_DIR environment variable
    """
    if "EXTRACTION_TEMP_DIR" in os.environ:
        return os.getenv("EXTRACTION_TEMP_DIR")
    
    if os.path.exists("/home/site/wwwroot"):
        return "/home/site/wwwroot/temp_extractions"
    
    return os.path.join(os.getcwd(), "temp_extractions")


@app.post("/extract-data")
def extract_data(req: ExtractRequest):
    import traceback
    import logging
    import sys

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        stream=sys.stdout,
        force=True
    )
    logger = logging.getLogger(__name__)

    logger.info("="*70)
    logger.info("EXTRACTION REQUEST RECEIVED")
    logger.info(f"Blob path: {req.blob_path}")
    logger.info("="*70)

    work_dir = None
    try:
        job_id = str(uuid.uuid4())
        logger.info(f"Job ID: {job_id}")

        base_temp_dir = get_work_directory()
        logger.info(f"Using temp base: {base_temp_dir}")
        
        os.makedirs(base_temp_dir, exist_ok=True)
        work_dir = os.path.join(base_temp_dir, job_id)
        os.makedirs(work_dir, exist_ok=True)
        logger.info(f"Work dir: {work_dir}")

        workbook_name = os.path.splitext(os.path.basename(req.blob_path))[0]
        logger.info(f"Workbook name: {workbook_name}")

        local_twbx = os.path.join(work_dir, "input.twbx")
        
        logger.info("Downloading TWBX...")
        download_twbx(req.blob_path, local_twbx)
        logger.info("Download completed")

        logger.info("Extracting data...")
        result = extract_from_twbx(local_twbx, work_dir, workbook_name)
        logger.info(f"Extraction completed - {len(result.get('csv_files'))} CSVs extracted")

        logger.info("Uploading CSVs to blob storage...")
        uploaded = []
        
        for csv_file in result.get("csv_files", []):
            csv_filename = os.path.basename(csv_file)
            
            # Organize files by schema
            # If CSV has schema prefix (e.g., "Extract_customers.csv"), extract it
            if "_" in csv_filename and not csv_filename.startswith("_"):
                parts = csv_filename.rsplit("_", 1)  # Split from right to handle multi-underscore names
                if len(parts) == 2:
                    schema = parts[0]
                    # Store as: workbook/schema/tablename.csv
                    blob_name = f"{workbook_name}/{schema}/{csv_filename}"
                else:
                    # Fallback if split fails
                    blob_name = f"{workbook_name}/{csv_filename}"
            else:
                # No schema prefix, store directly under workbook
                blob_name = f"{workbook_name}/{csv_filename}"
            
            logger.info(f"Uploading: {csv_filename} → {blob_name}")
            url = upload_csv(csv_file, blob_name)
            uploaded.append({
                "filename": csv_filename,
                "blob_path": blob_name,
                "url": url
            })
            logger.info(f"Uploaded: {url}")

        logger.info("="*70)
        logger.info(f"EXTRACTION SUCCESS - {len(uploaded)} files uploaded")
        logger.info("="*70)

        return {
            "job_id": job_id,
            "workbook": workbook_name,
            "output_files": uploaded,
            "tables": result.get("tables"),
        }

    except Exception as e:
        logger.error("="*70)
        logger.error(f"EXTRACTION FAILED: {str(e)}")
        logger.error(traceback.format_exc())
        logger.error("="*70)
        return {"error": str(e)}
    
    finally:
        if work_dir and os.path.exists(work_dir):
            try:
                shutil.rmtree(work_dir)
                logger.info(f"Cleaned up: {work_dir}")
            except Exception as cleanup_error:
                logger.warning(f"Cleanup warning: {cleanup_error}")