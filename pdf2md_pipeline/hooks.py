import os
from dagster import failure_hook, HookContext

@failure_hook
def error_hook(context: HookContext):
    """
    Writes a .err file to the output directory upon job failure.
    """
    # Debug log
    context.log.info("FAILURE HOOK TRIGGERED")

    run_config = context.run_config

    # Try to extract file path from config
    try:
        file_path = None
        ops_config = run_config.get("ops", {})
        if "ingest_and_split_pdf" in ops_config:
            file_path = ops_config["ingest_and_split_pdf"]["config"].get("file_path")

        if not file_path:
            context.log.warning("Could not find file_path in run config to write .err file.")
            return

        output_dir = os.path.dirname(file_path)
        filename = os.path.basename(file_path)
        base_name = os.path.splitext(filename)[0]
        err_path = os.path.join(output_dir, f"{base_name}.err")

        # Write exception to .err file
        with open(err_path, "w", encoding="utf-8") as f:
            # Check if op_exception is available
            error_msg = "Unknown error"
            if context.op_exception:
                error_msg = str(context.op_exception)
            elif context.job_exception: # Does job_exception exist? Not in HookContext usually.
                 pass

            # If op_exception is None, maybe we can look at step failure events?
            # But context doesn't expose them easily.
            # Assuming op_exception is populated for step failure triggering the hook.

            f.write(error_msg)

        context.log.info(f"Written error log to {err_path}")

    except Exception as e:
        context.log.error(f"Failed to write .err file: {e}")
