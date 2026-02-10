from dagster import IOManager, io_manager

class MemIOManager(IOManager):
    """
    A simple in-memory IO manager that stores intermediate results in a dictionary.
    This is suitable for single-process execution where data needs to be passed strictly in RAM.
    """
    def __init__(self):
        self.values = {}

    def handle_output(self, context, obj):
        # We use the step key (or similar identifier) to store the output.
        # For mapped outputs, we might need a more complex key, but let's start simple.
        # context.step_key is unique for the step execution.
        # However, for mapped steps, inputs come from upstream outputs.
        # context.upstream_output.step_key is used in load_input.

        # When mapping, downstream steps need to find the specific upstream output.
        # The key should probably involve run_id and step_key, but since we instantiate per run usually...
        # Actually, IOManager is a resource, instantiated once per run if using in-process executor.

        # Using (step_key, name) as key
        keys = tuple(context.get_output_identifier())
        self.values[keys] = obj

    def load_input(self, context):
        # Using (upstream_step_key, upstream_output_name) as key
        keys = tuple(context.upstream_output.get_output_identifier())
        return self.values[keys]

@io_manager
def mem_io_manager_def(_):
    return MemIOManager()
