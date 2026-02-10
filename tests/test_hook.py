from unittest.mock import MagicMock
from pdf2md_pipeline.hooks import error_hook
import os

def test_error_hook():
    context = MagicMock() # Mock everything
    context.run_config = {
        "ops": {
            "ingest_and_split_pdf": {
                "config": {
                    "file_path": "input_pdfs/test_hook.pdf"
                }
            }
        }
    }
    context.op_exception = ValueError("Simulated failure")
    context.log = MagicMock()

    # Create dir if not exists
    if not os.path.exists("input_pdfs"):
        os.makedirs("input_pdfs")

    # Call the underlying function
    error_hook.decorated_fn(context)

    # Check if file exists
    err_path = "input_pdfs/test_hook.err"
    assert os.path.exists(err_path)

    with open(err_path, "r") as f:
        content = f.read()
        assert "Simulated failure" in content

    # Cleanup
    if os.path.exists(err_path):
        os.remove(err_path)
    print("error_hook test passed!")

if __name__ == "__main__":
    test_error_hook()
