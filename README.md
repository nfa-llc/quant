# Gexbot Real-Time Data Client (Python)

This script connects to the Gexbot real-time data API. It negotiates a connection, joins specified data groups, and decompresses/parses incoming Zstandard-compressed Protobuf messages.

## Project Structure
```
.
├── proto/                  # Source .proto definitions (e.g., gex.proto)
├── generated_proto/        # (Ignored by git) Compiled _pb2.py files
├── main.py                 # The main client script
├── decompression_utils.py  # Decompression helper functions
├── requirements.txt        # Python dependencies
├── .gitignore
└── README.md
```
---

## 1. Setup and Installation

1.  **Clone Repository**
    Clone this repository to your local machine.

2.  **Create Virtual Environment**
    It's highly recommended to use a virtual environment.
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install Dependencies**
    Install all required Python packages.
    ```bash
    pip install -r requirements.txt
    ```

---

## 2. Compile Protobuf Definitions

This client reads data from Protobuf messages. You must compile the source `.proto` files (located in the `proto/` directory) into Python modules.

1.  **Ensure `generated_proto` Exists**
    Create the output directory.
    ```bash
    mkdir -p generated_proto
    ```

2.  **Create Python Package**
    The directory must be a Python package to be importable.
    ```bash
    touch generated_proto/__init__.py
    ```

3.  **Run `protoc` Compiler**
    Use `grpc_tools.protoc` (installed from `requirements.txt`) to generate the Python files. This command finds all `.proto` files in the `proto/` directory and outputs the compiled `_pb2.py` and `_pb2.pyi` files into `generated_proto/`.

    ```bash
    python3 -m grpc_tools.protoc -I=proto --python_out=generated_proto --pyi_out=generated_proto proto/*.proto
    ```
    * `-I=proto`: Specifies the `proto` directory as the import path.
    * `--python_out=generated_proto`: Outputs `_pb2.py` files to this directory.
    * `--pyi_out=generated_proto`: Outputs `_pb2.pyi` type-hinting files to this directory.
    * `proto/*.proto`: Compiles all `.proto` files in the `proto` directory.

---

## 3. Configuration

1.  **Set API Key**
    The script requires your `GEXBOT_API_KEY` to be set as an environment variable.

    ```bash
    export GEXBOT_API_KEY="your_api_key_here"
    ```

2.  **Configure Subscriptions**
    Open `main.py` and edit the "USER SELECTION" section (lines 70-137) to subscribe to your desired tickers and data feeds. Uncomment or add items to:
    * `ACTIVE_TICKERS`
    * `ACTIVE_CLASSIC_CATEGORIES`
    * `ACTIVE_STATE_GEX_CATEGORIES`
    * `ACTIVE_STATE_GREEKS_ZERO_CATEGORIES`
    * `ACTIVE_STATE_GREEKS_ONE_CATEGORIES`
    * `ACTIVE_ORDERFLOW_CATEGORIES`

---

## 4. Run the Client

After setting your API key and configuring subscriptions, run the script:

```bash
python main.py