from dagster import ConfigurableResource
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.pipeline.vlm_pipeline import VlmPipeline
from docling.datamodel.pipeline_options import VlmPipelineOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.vlm_model_specs import GRANITEDOCLING_TRANSFORMERS
from pydantic import PrivateAttr

class DoclingResource(ConfigurableResource):
    """
    A Dagster resource that provides a configured DocumentConverter instance.
    Can be configured to use the Granite VLM backend or the default Docling pipeline.
    Caches the converter instance to avoid reloading models for each chunk.
    """
    use_vlm: bool = False  # Flag to determine whether to use the Granite VLM backend
    _converter: object = PrivateAttr(default=None)

    def get_converter(self) -> DocumentConverter:
        if self._converter is None:
            if self.use_vlm:
                # Initialize with Granite VLM backend
                # Using GRANITEDOCLING_TRANSFORMERS for local execution via HuggingFace Transformers
                pipeline_options = VlmPipelineOptions(
                    vlm_options=GRANITEDOCLING_TRANSFORMERS
                )

                # Configure format options specifically for PDF
                format_options = {
                    InputFormat.PDF: PdfFormatOption(
                        pipeline_cls=VlmPipeline,
                        pipeline_options=pipeline_options
                    )
                }

                self._converter = DocumentConverter(format_options=format_options)
            else:
                # Initialize with default Docling pipeline
                self._converter = DocumentConverter()

        return self._converter
