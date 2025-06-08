# End-to-end tests for `dvc import-url` with various remote types.

import os
import pytest
import subprocess
import threading
from http.server import SimpleHTTPRequestHandler, HTTPServer

import boto3
from moto import mock_s3

from google.cloud import storage
from testcontainers.google import GCloudContainer

from azure.storage.blob import BlobServiceClient
from testcontainers.azurite import AzuriteContainer

@pytest.fixture
def http_server(tmp_path):
    data_file = tmp_path / "data.txt"
    data_file.write_text("test http data")

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(tmp_path), **kwargs)

    host = "localhost"
    port = 8000
    server_address = (host, port)
    HTTPServer.allow_reuse_address = True
    httpd = HTTPServer(server_address, Handler)

    server_thread = threading.Thread(target=httpd.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    yield f"http://{host}:{port}"

    httpd.shutdown()
    httpd.server_close()
    server_thread.join()

def test_import_url_http(tmp_path, dvc, http_server):
    http_url = f"{http_server}/data.txt"
    dest_file = tmp_path / "dest.txt"

    result = subprocess.run(
        ["dvc", "import-url", http_url, str(dest_file)],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Error: {result.stderr}"

    assert dest_file.exists()
    assert dest_file.read_text() == "test http data"

    dvc_file = dest_file.with_suffix(".dvc")
    assert dvc_file.exists()

    os.remove(dest_file)
    os.remove(dvc_file)

@pytest.fixture
def s3_bucket(tmp_path): # tmp_path is kept for consistency, though not strictly used by moto for bucket creation
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = "mybucket"
        s3.create_bucket(Bucket=bucket_name)

        data_content = "test s3 data"
        s3.put_object(Bucket=bucket_name, Key="data.txt", Body=data_content)

        yield f"s3://{bucket_name}"
        # moto handles cleanup when the context manager exits

def test_import_url_s3(tmp_path, dvc, s3_bucket):
    s3_url = f"{s3_bucket}/data.txt"
    dest_file = tmp_path / "dest_s3.txt"

    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = "testing"
    env["AWS_SECRET_ACCESS_KEY"] = "testing"
    env["AWS_SECURITY_TOKEN"] = "testing"
    env["AWS_SESSION_TOKEN"] = "testing"
    env["AWS_DEFAULT_REGION"] = "us-east-1"

    result = subprocess.run(
        ["dvc", "import-url", s3_url, str(dest_file)],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, f"Error: {result.stderr}"

    assert dest_file.exists()
    assert dest_file.read_text() == "test s3 data"

    dvc_file = dest_file.with_suffix(".dvc")
    assert dvc_file.exists()

    os.remove(dest_file)
    os.remove(dvc_file)

@pytest.fixture(scope="session")
def gcs_emulator():
    try:
        container = GCloudContainer()
        container.start()
        os.environ["STORAGE_EMULATOR_HOST"] = container.get_container_host_ip() + ":" + container.get_exposed_port(4443)
        # Unset credentials to use anonymous access with emulator, GCLOUD_PROJECT is a dummy project
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
        os.environ["GCLOUD_PROJECT"] = "test-project"
        yield container
    finally:
        if 'container' in locals():
            container.stop()
        # Clean up environment variables
        os.environ.pop("STORAGE_EMULATOR_HOST", None)
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        os.environ.pop("GCLOUD_PROJECT", None)

@pytest.fixture
def gcs_bucket(gcs_emulator, tmp_path): # tmp_path can be used if creating local files to upload
    # Client automatically uses STORAGE_EMULATOR_HOST from environment
    client = storage.Client(project="test-project") # Ensure project matches if client doesn't pick from env

    bucket_name = "mygcsbucket"
    try:
        bucket = client.create_bucket(bucket_name)
    except Exception as e:
        # Handle cases where bucket might already exist (e.g. from a previous interrupted run if emulator state persists)
        if "conflict" in str(e).lower() or "Your previous request to create the named bucket succeeded and you already own it" in str(e):
            bucket = client.get_bucket(bucket_name)
        else:
            raise

    data_content = "test gcs data"
    blob = bucket.blob("data.txt")
    blob.upload_from_string(data_content)

    yield f"gs://{bucket_name}"
    # Emulator is session-scoped; explicit bucket cleanup can be added if needed
    # but typically emulator provides fresh state or is torn down.

def test_import_url_gcs(tmp_path, dvc, gcs_bucket):
    gcs_url = f"{gcs_bucket}/data.txt"
    dest_file = tmp_path / "dest_gcs.txt"

    # Environment for subprocess, ensuring it also sees the emulator settings
    env = os.environ.copy()
    # STORAGE_EMULATOR_HOST, GOOGLE_APPLICATION_CREDENTIALS, GCLOUD_PROJECT are set by gcs_emulator

    result = subprocess.run(
        ["dvc", "import-url", gcs_url, str(dest_file)],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, f"Error: {result.stderr}"

    assert dest_file.exists()
    assert dest_file.read_text() == "test gcs data"

    dvc_file = dest_file.with_suffix(".dvc")
    assert dvc_file.exists()

    os.remove(dest_file)
    os.remove(dvc_file)

@pytest.fixture(scope="session")
def azurite_emulator():
    try:
        container = AzuriteContainer() # Uses default image mcr.microsoft.com/azure-storage/azurite
        container.start()
        # The AzuriteContainer class provides a method to get the connection string
        os.environ["AZURE_STORAGE_CONNECTION_STRING"] = container.get_connection_string()
        yield container
    finally:
        if 'container' in locals():
            container.stop()
        os.environ.pop("AZURE_STORAGE_CONNECTION_STRING", None)

@pytest.fixture
def azure_blob_container_fixture(azurite_emulator, tmp_path): # tmp_path for consistency or local file prep
    connection_string = azurite_emulator.get_connection_string()
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_name = "myazurecontainer"
    try:
        container_client = blob_service_client.create_container(container_name)
    except Exception as e: # Handle if container already exists (e.g. ResourceExistsError)
         if "already exists" in str(e).lower() or "The specified container already exists" in str(e):
            container_client = blob_service_client.get_container_client(container_name)
         else:
            raise

    data_content = "test azure data"
    blob_client = container_client.get_blob_client("data.txt")
    blob_client.upload_blob(data_content, overwrite=True)

    yield f"azure://{container_name}"
    # Emulator is session-scoped; explicit container cleanup can be added if needed.

def test_import_url_azure(tmp_path, dvc, azure_blob_container_fixture):
    azure_url = f"{azure_blob_container_fixture}/data.txt" # Using the renamed fixture
    dest_file = tmp_path / "dest_azure.txt"

    # Environment for subprocess, ensuring it sees the emulator settings
    env = os.environ.copy()
    # AZURE_STORAGE_CONNECTION_STRING should be set by the azurite_emulator fixture

    result = subprocess.run(
        ["dvc", "import-url", azure_url, str(dest_file)],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, f"Error: {result.stderr}"

    assert dest_file.exists()
    assert dest_file.read_text() == "test azure data"

    dvc_file = dest_file.with_suffix(".dvc")
    assert dvc_file.exists()

    os.remove(dest_file)
    os.remove(dvc_file)
