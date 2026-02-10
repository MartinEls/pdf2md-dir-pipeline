import fitz
import os
from dagster import op, Out, DynamicOut, DynamicOutput, Config
from io import BytesIO
from typing import List

from pdf2md_pipeline.resources import DoclingResource
from docling.datamodel.base_models import DocumentStream

class IngestConfig(Config):
    file_path: str
    chunk_size: int = 10

@op(out=DynamicOut())
def ingest_and_split_pdf(context, config: IngestConfig):
    """
    Reads a PDF file and splits it into smaller chunks in-memory.
    strictly avoiding intermediate disk I/O via PyMuPDF.
    Yields DynamicOutput for each chunk to enable parallel processing.
    """
    file_path = config.file_path
    context.log.info(f"Ingesting {file_path}")

    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Open the document
    try:
        doc = fitz.open(file_path)
    except Exception as e:
        raise ValueError(f"Failed to open PDF: {e}")

    num_pages = doc.page_count
    chunks = []
    chunk_size = config.chunk_size

    context.log.info(f"Splitting {num_pages} pages into chunks of {chunk_size}")

    for start_page in range(0, num_pages, chunk_size):
        end_page = min(start_page + chunk_size, num_pages)

        # Create a new document for the chunk
        chunk_doc = fitz.open()
        chunk_doc.insert_pdf(doc, from_page=start_page, to_page=end_page-1)

        # Save to BytesIO
        stream = BytesIO()
        chunk_doc.save(stream)
        stream.seek(0)
        chunks.append(stream)
        chunk_doc.close()

    doc.close()
    context.log.info(f"Split into {len(chunks)} chunks")

    for i, chunk in enumerate(chunks):
        # We yield DynamicOutput, mapping_key must be valid string (alphanumeric, underscores, dashes)
        yield DynamicOutput(chunk, mapping_key=str(i))

@op
def convert_chunk_granite(context, chunk: BytesIO, docling: DoclingResource) -> str:
    """
    Converts a PDF chunk (BytesIO) to Markdown using Granite VLM via DoclingResource.
    """
    context.log.info("Converting chunk with Granite VLM")

    # Get the converter from the resource
    converter = docling.get_converter()

    # Create a DocumentStream wrapping the chunk
    # We use a generic name as it's a chunk
    stream_source = DocumentStream(name="chunk.pdf", stream=chunk)

    # Convert
    result = converter.convert(stream_source)

    # Export to Markdown
    md = result.document.export_to_markdown()

    return md

@op
def merge_and_write_markdown(context, markdown_chunks: List[str], config: IngestConfig) -> str:
    """
    Merges markdown chunks and writes to the output file.
    Derives output path from the original file path.
    """
    file_path = config.file_path
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_dir = os.path.dirname(file_path)

    # Construct output path
    output_path = os.path.join(output_dir, f"{base_name}.md")

    full_md = "\n\n".join(markdown_chunks)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(full_md)

    context.log.info(f"Written markdown to {output_path}")
    return output_path
