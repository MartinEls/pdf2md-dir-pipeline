# pdf2md-dir-pipeline
Dagster-orchestrated pipeline designed to convert PDF documents into Markdown using the Docling Granite VLM backend. The system must act as a sensor-driven workflow that ingests files from a watched directory, splits large documents into in-memory chunks (strictly avoiding intermediate disk I/O via PyMuPDF), and processes these fragments purely in RAM before merging them into a final output file. The implementation requires composable, reusable Ops, a custom failure hook to generate <filename>.err logs upon exception, and strict concurrency management to ensure the heavy VLM inference runs stably on a single machine.

Here is the high-level design specification for the document handling pipeline.

### 0. General
- use `uv` as the package manager and for venv creation

### 1. Architectural Strategy

We will use a **Sensor-Driven Architecture** rather than a simple scheduled job.

* **Why:** A sensor allows us to react to new files individually. This isolates the failure domain; if one PDF is corrupt, it fails its own run without crashing the batch. It also simplifies the logic for generating the specific `<filename>.err` file on a per-run basis.
* **Orchestrator:** Dagster (Local instance).
* **Execution Mode:** `In-process` or `Multiprocess` (depending on VLM VRAM usage). Given the single machine constraint, we will enforce strict concurrency limits (max 1 or 2 concurrent runs) to prevent the Granite model from OOMing (Out of Memory) the GPU/CPU.

### 2. Pipeline Components

We will treat the logic as composable **Ops** (Operations) rather than pure Assets, as this is an imperative data processing pipeline (File A  File B) rather than a state synchronizer.

#### A. The Resources

* **`DoclingResource`**:
* **Responsibility:** Initialize the `DocumentConverter` with the `granitedocling` VLM pipeline artifacts once.
* **Reasoning:** Model loading is expensive. We don't want to re-initialize the VLM for every chunk. Passing this as a Dagster resource allows reuse across ops if we run in a persistent process, or cleanly manages setup/teardown.



#### B. The Ops (Composable Units)

1. **`ingest_and_split_pdf`**
* **Input:** `file_path` (String).
* **Logic:**
* Use `pymupdf` to open the PDF.
* Calculate split boundaries (e.g., every 5-10 pages depending on density).
* Extract pages and save them into `io.BytesIO` objects (in-memory streams).


* **Output:** `List[BytesIO]` (or a custom wrapper object containing metadata + stream).
* **Constraint Check:** Strictly in-memory; no intermediate disk writes.


2. **`convert_chunk_granite`**
* **Input:** `pdf_chunk` (BytesIO).
* **Logic:**
* Accept the stream.
* Invoke the `DoclingResource` to run the Granite VLM conversion.


* **Output:** `String` (Markdown fragment).


3. **`merge_and_write_markdown`**
* **Input:** `List[String]` (The ordered Markdown fragments).
* **Logic:**
* Concatenate fragments with appropriate spacing.
* Derive output path from the original run configuration.
* Write final `.md` to the output folder.


* **Output:** `String` (Path to the generated file, for logging).



### 3. Data Flow & Orchestration (The "How")

#### The Job Graph

The job `process_single_pdf_job` will wire the ops together:

1. **Splitting:** `ingest_and_split_pdf` receives the path.
2. **Dynamic Mapping:** Use Dagster's `map` feature to run `convert_chunk_granite` over the list of chunks output by step 1.
3. **Reducing:** The results of the map are collected into `merge_and_write_markdown`.

#### The Memory Constraint (`MemoryIOManager`)

* **Critical:** By default, Dagster pickles intermediate data to disk (`fs_io_manager`). Because you explicitly requested *not* to write chunks to disk, we must configure the Job to use the `MemoryIOManager`.
* This ensures the `BytesIO` chunks pass from Op 1 to Op 2 solely in RAM.

#### The Error Handling (Failure Hook)

We will implement a **Job-level Failure Hook**.

* **Trigger:** Activates if the Job fails at any step (splitting, conversion, or writing).
* **Logic:**
1. Extract the original `file_path` from the run configuration.
2. Capture the stack trace/error message from the Dagster event log.
3. Write to `output_folder/<filename>.err`.



### 4. Implementation Specification for the Coding Agent

Pass the following instructions to the developer/agent:

1. **Setup**:
* Create a Dagster project structure.
* Dependency check: Ensure `docling`, `pymupdf`, and `dagster` are installed.
* Configure `docling` to use the Granite pipeline options explicitly.


2. **Step 1: Define the IO Manager**:
* Implement/configure `mem_io_manager` to prevent intermediate disk persistence.


3. **Step 2: Build the Ops**:
* Write `ingest_and_split_pdf`: Ensure it returns `List[BytesIO]`. Add logic to handle large page counts safely.
* Write `convert_chunk_granite`: This must accept a `BytesIO` stream. *Note:* Docling's `convert` method usually expects a path; ensure you use the `InputDocument(stream=...)` or equivalent API for in-memory streams.
* Write `merge_and_write_markdown`.


4. **Step 3: The Sensor**:
* Create `new_pdf_sensor`.
* It should poll a target directory.
* On finding a new PDF, yield a `RunRequest` for `process_single_pdf_job`.
* Pass the file path as run configuration (e.g., `ops: {ingest_and_split_pdf: {config: {file_path: "..."}}}`).


5. **Step 4: Error Hook**:
* Define a function decorated with `@failure_hook`.
* Inside, use `context.run_config` to identify the target file and write the exception to `.err`.


6. **Concurrency Control**:
* Set the Dagster `tag_concurrency_limits` or use a `Limit` resource to ensure only 1 processing job runs at a time (VLM inference is heavy).
