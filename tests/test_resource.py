from unittest.mock import patch, MagicMock
from pdf2md_pipeline.resources import DoclingResource

@patch("pdf2md_pipeline.resources.DocumentConverter")
@patch("pdf2md_pipeline.resources.VlmPipelineOptions")
@patch("pdf2md_pipeline.resources.PdfFormatOption")
def test_resource_get_converter_caching(mock_pdf_option, mock_options, mock_converter):
    resource = DoclingResource()
    converter1 = resource.get_converter()
    converter2 = resource.get_converter()

    # Should only instantiate once due to caching
    mock_converter.assert_called_once()

    # Verify both calls return the same instance
    assert converter1 == converter2
    assert converter1 == mock_converter.return_value
    print("DoclingResource caching test passed!")

if __name__ == "__main__":
    test_resource_get_converter_caching()
