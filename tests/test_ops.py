import fitz
import os
from io import BytesIO
from unittest.mock import MagicMock
from dagster import build_op_context, DynamicOutput

from pdf2md_pipeline.ops import ingest_and_split_pdf, convert_chunk, merge_and_write_markdown, IngestConfig
from pdf2md_pipeline.resources import DoclingResource

class MockDoclingResource(DoclingResource):
    def get_converter(self):
        mock_converter = MagicMock()
        mock_result = MagicMock()
        mock_result.document.export_to_markdown.return_value = "# Markdown Content"
        mock_converter.convert.return_value = mock_result
        return mock_converter

def test_ingest_and_split_pdf():
    # Create a dummy PDF
    doc = fitz.open()
    doc.new_page()
    doc.new_page()
    pdf_path = "dummy.pdf"
    doc.save(pdf_path)
    doc.close()

    try:
        context = build_op_context()
        config = IngestConfig(file_path=pdf_path, chunk_size=1)

        chunks_gen = ingest_and_split_pdf(context, config)
        chunks = list(chunks_gen)

        assert len(chunks) == 2
        assert isinstance(chunks[0], DynamicOutput)
        assert isinstance(chunks[0].value, BytesIO)
        print("ingest_and_split_pdf test passed!")
    finally:
        if os.path.exists(pdf_path):
            os.remove(pdf_path)

def test_convert_chunk():
    # Use subclass to satisfy type check
    mock_resource = MockDoclingResource()

    context = build_op_context()
    chunk = BytesIO(b"dummy pdf content")

    md = convert_chunk(context, chunk, docling=mock_resource)

    assert md == "# Markdown Content"
    print("convert_chunk test passed!")

def test_merge_and_write_markdown():
    context = build_op_context()
    chunks = ["# Part 1", "# Part 2"]
    config = IngestConfig(file_path="test_output.pdf")

    # We expect output to be test_output.md
    output_path = merge_and_write_markdown(context, markdown_chunks=chunks, config=config)

    assert output_path == "test_output.md"

    with open("test_output.md", "r") as f:
        content = f.read()

    assert content == "# Part 1\n\n# Part 2"

    # Cleanup
    if os.path.exists("test_output.md"):
        os.remove("test_output.md")
    print("merge_and_write_markdown test passed!")

if __name__ == "__main__":
    test_ingest_and_split_pdf()
    test_convert_chunk()
    test_merge_and_write_markdown()
