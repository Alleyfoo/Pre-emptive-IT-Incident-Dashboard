import os
from typing import List, Optional


class ArtifactStore:
    """
    Minimal interface for reading and writing artifacts.
    Keys are logical, relative to the store root (e.g., "run_id/header_spec.json").
    """

    def read_text(self, key: str) -> str:
        raise NotImplementedError

    def write_text(self, key: str, text: str, content_type: Optional[str] = None) -> None:
        raise NotImplementedError

    def read_bytes(self, key: str) -> bytes:
        raise NotImplementedError

    def write_bytes(self, key: str, data: bytes, content_type: Optional[str] = None) -> None:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def list(self, prefix: str = "") -> List[str]:
        raise NotImplementedError

    def uri_for_key(self, key: str) -> str:
        """
        Returns a fully qualified URI for the key when available (gs://... for GCS, file:// for local).
        """
        raise NotImplementedError


class LocalArtifactStore(ArtifactStore):
    def __init__(self, root_dir: str):
        self.root_dir = os.path.abspath(root_dir)

    def _path(self, key: str) -> str:
        normalized = key.lstrip("/").replace("/", os.sep)
        return os.path.join(self.root_dir, normalized)

    def read_text(self, key: str) -> str:
        with open(self._path(key), "r", encoding="utf-8") as handle:
            return handle.read()

    def write_text(self, key: str, text: str, content_type: Optional[str] = None) -> None:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(text)

    def read_bytes(self, key: str) -> bytes:
        with open(self._path(key), "rb") as handle:
            return handle.read()

    def write_bytes(self, key: str, data: bytes, content_type: Optional[str] = None) -> None:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(data)

    def exists(self, key: str) -> bool:
        return os.path.exists(self._path(key))

    def list(self, prefix: str = "") -> List[str]:
        prefix = prefix.lstrip("/").replace("\\", "/")
        keys: List[str] = []
        root = self._path(prefix)
        if not os.path.exists(root):
            return keys
        if os.path.isfile(root):
            keys.append(prefix or os.path.basename(root))
            return keys
        for dirpath, _, filenames in os.walk(root):
            for filename in filenames:
                absolute = os.path.join(dirpath, filename)
                rel = os.path.relpath(absolute, self.root_dir)
                keys.append(rel.replace("\\", "/"))
        return keys

    def uri_for_key(self, key: str) -> str:
        return f"file://{self._path(key)}"


class GCSArtifactStore(ArtifactStore):
    def __init__(self, bucket: str, prefix: str = "", client=None):
        try:
            from google.cloud import storage
        except Exception as exc:  # pragma: no cover - import guard
            raise ImportError("google-cloud-storage is required for GCSArtifactStore") from exc

        self.bucket_name = bucket
        self.prefix = prefix.strip("/")
        self.client = client or storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)

    def _full_key(self, key: str) -> str:
        normalized = key.lstrip("/")
        if self.prefix:
            return f"{self.prefix}/{normalized}"
        return normalized

    def _strip_prefix(self, full_key: str) -> str:
        if self.prefix and full_key.startswith(f"{self.prefix}/"):
            return full_key[len(self.prefix) + 1 :]
        return full_key

    def read_text(self, key: str) -> str:
        blob = self.bucket.blob(self._full_key(key))
        return blob.download_as_text(encoding="utf-8")

    def write_text(self, key: str, text: str, content_type: Optional[str] = None) -> None:
        blob = self.bucket.blob(self._full_key(key))
        blob.upload_from_string(text, content_type=content_type or "text/plain")

    def read_bytes(self, key: str) -> bytes:
        blob = self.bucket.blob(self._full_key(key))
        return blob.download_as_bytes()

    def write_bytes(self, key: str, data: bytes, content_type: Optional[str] = None) -> None:
        blob = self.bucket.blob(self._full_key(key))
        blob.upload_from_string(data, content_type=content_type)

    def exists(self, key: str) -> bool:
        blob = self.bucket.blob(self._full_key(key))
        return blob.exists()

    def list(self, prefix: str = "") -> List[str]:
        search_prefix = self._full_key(prefix)
        blobs = self.client.list_blobs(self.bucket, prefix=search_prefix)
        keys: List[str] = []
        for blob in blobs:
            if blob.name.endswith("/"):
                continue
            keys.append(self._strip_prefix(blob.name))
        return keys

    def uri_for_key(self, key: str) -> str:
        full_key = self._full_key(key)
        return f"gs://{self.bucket_name}/{full_key}"


def is_gcs_uri(uri: str) -> bool:
    return uri.lower().startswith("gs://")


def parse_gcs_uri(uri: str) -> (str, str):
    normalized = uri[len("gs://") :]
    if "/" in normalized:
        bucket, prefix = normalized.split("/", 1)
    else:
        bucket, prefix = normalized, ""
    return bucket, prefix


def build_artifact_store(artifacts_root: str) -> ArtifactStore:
    """
    Pick a store based on artifacts_root. Supports local paths and gs://bucket/prefix.
    """
    if is_gcs_uri(artifacts_root):
        bucket, prefix = parse_gcs_uri(artifacts_root)
        return GCSArtifactStore(bucket=bucket, prefix=prefix)
    return LocalArtifactStore(artifacts_root)
