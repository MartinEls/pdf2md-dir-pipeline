import os
from dagster import sensor, RunRequest
from pdf2md_pipeline.jobs import process_single_pdf_job

@sensor(job=process_single_pdf_job)
def new_pdf_sensor(context):
    """
    Watches a directory for new PDF files and triggers the processing job.
    """
    watch_dir = os.getenv("PDF_WATCH_DIR", "./input_pdfs")

    if not os.path.exists(watch_dir):
        try:
            os.makedirs(watch_dir, exist_ok=True)
        except OSError:
            context.log.warning(f"Could not create watch dir {watch_dir}")
            return

    for filename in os.listdir(watch_dir):
        if filename.lower().endswith(".pdf"):
            file_path = os.path.abspath(os.path.join(watch_dir, filename))

            # Check if output or error file already exists to avoid re-processing
            base_name = os.path.splitext(filename)[0]
            md_path = os.path.join(watch_dir, f"{base_name}.md")
            err_path = os.path.join(watch_dir, f"{base_name}.err")

            if os.path.exists(md_path) or os.path.exists(err_path):
                continue

            # Use mtime in run_key to handle file updates
            mtime = os.path.getmtime(file_path)
            run_key = f"{filename}_{mtime}"

            yield RunRequest(
                run_key=run_key,
                run_config={
                    "ops": {
                        "ingest_and_split_pdf": {"config": {"file_path": file_path}},
                        "merge_and_write_markdown": {"config": {"file_path": file_path}}
                    }
                }
            )
