from runtime.artifact_store import (
    LocalArtifactStore,
    build_artifact_store,
    is_gcs_uri,
    parse_gcs_uri,
)


def test_local_store_roundtrip(tmp_path):
    store = LocalArtifactStore(tmp_path)

    store.write_text("run_a/example.txt", "hello", content_type="text/plain")
    assert store.exists("run_a/example.txt")
    assert store.read_text("run_a/example.txt") == "hello"

    store.write_bytes("run_a/data.bin", b"\x01\x02\x03", content_type="application/octet-stream")
    assert store.read_bytes("run_a/data.bin") == b"\x01\x02\x03"

    keys = set(store.list(""))
    assert "run_a/example.txt" in keys
    assert "run_a/data.bin" in keys
    assert store.uri_for_key("run_a/example.txt").startswith("file://")


def test_build_artifact_store_local(tmp_path):
    store = build_artifact_store(str(tmp_path))
    assert isinstance(store, LocalArtifactStore)


def test_gcs_uri_parsing():
    assert is_gcs_uri("gs://bucket-name/path/value")
    bucket, prefix = parse_gcs_uri("gs://bucket-name/path/value")
    assert bucket == "bucket-name"
    assert prefix == "path/value"
