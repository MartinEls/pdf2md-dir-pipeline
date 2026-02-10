from dagster import job, in_process_executor
from pdf2md_pipeline.ops import ingest_and_split_pdf, convert_chunk_granite, merge_and_write_markdown
from pdf2md_pipeline.io_managers import mem_io_manager_def
from pdf2md_pipeline.resources import DoclingResource
from pdf2md_pipeline.hooks import error_hook

@job(
    resource_defs={
        "io_manager": mem_io_manager_def,
        "docling": DoclingResource(),
    },
    hooks={error_hook},
    tags={"concurrency_group": "vlm"},
    executor_def=in_process_executor
)
def process_single_pdf_job():
    chunks = ingest_and_split_pdf()

    # Use map to process chunks in parallel (or sequentially if using in_process_executor)
    markdown_chunks = chunks.map(convert_chunk_granite)

    # Collect all markdown chunks into a list and merge them
    merge_and_write_markdown(markdown_chunks.collect())
