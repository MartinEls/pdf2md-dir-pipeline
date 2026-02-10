from dagster import Definitions
from pdf2md_pipeline.jobs import process_single_pdf_job
from pdf2md_pipeline.sensors import new_pdf_sensor
from pdf2md_pipeline.resources import DoclingResource
from pdf2md_pipeline.io_managers import mem_io_manager_def

# The job process_single_pdf_job is tagged with {"concurrency_group": "vlm"}
# Concurrency limits should be enforced by the RunCoordinator (configured in dagster.yaml).

defs = Definitions(
    jobs=[process_single_pdf_job],
    sensors=[new_pdf_sensor],
    resources={
        "docling": DoclingResource(),
        "io_manager": mem_io_manager_def
    }
)
