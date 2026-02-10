from dagster import OutputContext, InputContext
from pdf2md_pipeline.io_managers import MemIOManager

def test_mem_io_manager():
    manager = MemIOManager()

    # Mock context for handle_output
    class MockOutputContext:
        def get_output_identifier(self):
            return ["step1", "output1"]

    output_context = MockOutputContext()
    manager.handle_output(output_context, "test_data")

    # Mock context for load_input
    class MockInputContext:
        @property
        def upstream_output(self):
            return MockOutputContext()

    input_context = MockInputContext()
    result = manager.load_input(input_context)

    assert result == "test_data"
    print("MemIOManager test passed!")

if __name__ == "__main__":
    test_mem_io_manager()
