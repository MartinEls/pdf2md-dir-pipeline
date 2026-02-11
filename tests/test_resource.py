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

@patch("pdf2md_pipeline.resources.DocumentConverter")
@patch("pdf2md_pipeline.resources.VlmPipelineOptions")
@patch("pdf2md_pipeline.resources.PdfFormatOption")
def test_resource_get_converter_config(mock_pdf_option, mock_options, mock_converter):
    # Test VLM (default)
    resource_vlm = DoclingResource(use_vlm=True)
    resource_vlm.get_converter()

    # Expect VlmPipelineOptions to be instantiated
    mock_options.assert_called()
    mock_pdf_option.assert_called()
    # Expect DocumentConverter to be called with format_options
    args, kwargs = mock_converter.call_args
    assert "format_options" in kwargs

    mock_converter.reset_mock()
    mock_options.reset_mock()
    mock_pdf_option.reset_mock()

    # Test Default (non-VLM)
    resource_default = DoclingResource(use_vlm=False)
    resource_default.get_converter()

    # Expect DocumentConverter to be called with no args (or at least different args)
    # And VlmPipelineOptions should NOT be called
    mock_options.assert_not_called()
    mock_pdf_option.assert_not_called()

    # Verify DocumentConverter call
    # It should be called without format_options (or empty)
    # The current implementation calls DocumentConverter()
    mock_converter.assert_called_once_with()

    print("DoclingResource config test passed!")

if __name__ == "__main__":
    test_resource_get_converter_caching()
    test_resource_get_converter_config()
