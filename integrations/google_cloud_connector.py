"""
Google Cloud Connector for Helix Spirals
Provides integration with Google Cloud services: GCS, Compute Engine, Cloud Functions, etc.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

try:
    from google.auth import default
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import compute_v1, functions_v1, pubsub_v1, storage

    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class GoogleCloudConfig:
    """Google Cloud connector configuration."""

    project_id: str
    credentials_json: str | None = None
    service_account_key_path: str | None = None


@dataclass
class GCSObject:
    """Represents a GCS object."""

    bucket: str
    name: str
    size: int
    updated: datetime
    content_type: str | None = None
    md5_hash: str | None = None


@dataclass
class ComputeInstance:
    """Represents a Compute Engine instance."""

    name: str
    zone: str
    machine_type: str
    status: str
    internal_ip: str | None = None
    external_ip: str | None = None
    creation_timestamp: datetime | None = None
    labels: dict[str, str] = None


@dataclass
class CloudFunction:
    """Represents a Cloud Function."""

    name: str
    runtime: str
    entry_point: str
    status: str
    available_memory_mb: int
    timeout_seconds: int
    update_time: datetime


class GoogleCloudConnector:
    """
    Google Cloud integration connector supporting multiple services.
    """

    def __init__(self, config: GoogleCloudConfig):
        if not GCP_AVAILABLE:
            raise ImportError(
                "google-cloud libraries are required for GCP connector. "
                "Install with: pip install google-cloud-storage google-cloud-compute "
                "google-cloud-functions google-cloud-pubsub"
            )

        self.config = config
        self._clients = {}
        self._credentials = self._initialize_credentials()
        self._initialize_clients()

    def _initialize_credentials(self):
        """Initialize Google Cloud credentials."""
        if self.config.credentials_json:
            import json

            from google.oauth2 import service_account

            creds_dict = json.loads(self.config.credentials_json)
            return service_account.Credentials.from_service_account_info(creds_dict)

        elif self.config.service_account_key_path:
            from google.oauth2 import service_account

            return service_account.Credentials.from_service_account_file(self.config.service_account_key_path)

        else:
            # Use default credentials (Application Default Credentials)
            try:
                creds, _ = default()
                return creds
            except DefaultCredentialsError as e:
                logger.error("Failed to get default credentials: %s", e)
                raise

    def _initialize_clients(self):
        """Initialize Google Cloud service clients."""
        project_id = self.config.project_id

        self._clients["storage"] = storage.Client(project=project_id, credentials=self._credentials)

        self._clients["compute"] = compute_v1.InstancesClient(credentials=self._credentials)

        self._clients["functions"] = functions_v1.CloudFunctionsServiceClient(credentials=self._credentials)

        self._clients["pubsub"] = pubsub_v1.PublisherClient(credentials=self._credentials)

        logger.info("✅ Google Cloud clients initialized for project %s", project_id)

    # ==================== GCS Operations ====================

    async def list_buckets(self) -> list[dict[str, Any]]:
        """List all GCS buckets."""
        try:
            buckets = list(self._clients["storage"].list_buckets())
            return [
                {
                    "name": bucket.name,
                    "location": bucket.location,
                    "created": bucket.time_created,
                }
                for bucket in buckets
            ]
        except Exception as e:
            logger.error("Failed to list GCS buckets: %s", e)
            raise

    async def list_objects(self, bucket: str, prefix: str = "", max_results: int = 1000) -> list[GCSObject]:
        """List objects in a GCS bucket."""
        try:
            blobs = list(self.bucket.list_blobs(prefix=prefix, max_results=max_results))

            objects = []
            for blob in blobs:
                objects.append(
                    GCSObject(
                        bucket=bucket,
                        name=blob.name,
                        size=blob.size or 0,
                        updated=blob.updated,
                        content_type=blob.content_type,
                        md5_hash=blob.md5_hash,
                    )
                )

            return objects
        except Exception as e:
            logger.error("Failed to list GCS objects: %s", e)
            raise

    async def upload_object(
        self,
        bucket: str,
        name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> dict[str, str]:
        """Upload an object to GCS."""
        try:
            bucket_obj = self._clients["storage"].bucket(bucket)
            blob = bucket_obj.blob(name)
            blob.upload_from_string(data, content_type=content_type)

            return {"bucket": bucket, "name": name, "status": "uploaded"}
        except Exception as e:
            logger.error("Failed to upload GCS object: %s", e)
            raise

    async def download_object(self, bucket: str, name: str) -> bytes:
        """Download an object from GCS."""
        try:
            bucket_obj = self._clients["storage"].bucket(bucket)
            blob = bucket_obj.blob(name)
            return blob.download_as_bytes()
        except Exception as e:
            logger.error("Failed to download GCS object: %s", e)
            raise

    async def delete_object(self, bucket: str, name: str) -> bool:
        """Delete an object from GCS."""
        try:
            bucket_obj = self._clients["storage"].bucket(bucket)
            blob = bucket_obj.blob(name)
            blob.delete()
            return True
        except Exception as e:
            logger.error("Failed to delete GCS object: %s", e)
            return False

    # ==================== Compute Engine Operations ====================

    async def list_instances(self, zone: str | None = None) -> list[ComputeInstance]:
        """List all Compute Engine instances."""
        try:
            project = self.config.project_id

            if zone:
                request = compute_v1.ListInstancesRequest(project=project, zone=zone)
                instances = list(self._clients["compute"].list(request=request))
            else:
                # List from all zones
                instances = []
                zones_client = compute_v1.ZonesClient(credentials=self._credentials)
                zones = list(zones_client.list(project=project))

                for z in zones:
                    request = compute_v1.ListInstancesRequest(project=project, zone=z.name)
                    zone_instances = list(self._clients["compute"].list(request=request))
                    instances.extend(zone_instances)

            result = []
            for instance in instances:
                # Get IP addresses
                internal_ip = None
                external_ip = None

                for interface in instance.network_interfaces:
                    if interface.network_i_p:
                        internal_ip = interface.network_i_p
                    for access_config in interface.access_configs:
                        if access_config.nat_i_p:
                            external_ip = access_config.nat_i_p

                # Get labels
                labels = instance.labels or {}

                result.append(
                    ComputeInstance(
                        name=instance.name,
                        zone=instance.zone.split("/")[-1],
                        machine_type=instance.machine_type.split("/")[-1],
                        status=instance.status,
                        internal_ip=internal_ip,
                        external_ip=external_ip,
                        creation_timestamp=datetime.fromisoformat(instance.creation_timestamp.replace("Z", "+00:00")),
                        labels=labels,
                    )
                )

            return result
        except Exception as e:
            logger.error("Failed to list Compute instances: %s", e)
            raise

    async def start_instance(self, zone: str, instance_name: str) -> dict[str, Any]:
        """Start a Compute Engine instance."""
        try:
            project = self.config.project_id
            request = compute_v1.StartInstanceRequest(project=project, zone=zone, instance=instance_name)
            operation = self._clients["compute"].start(request=request)
            return {"name": operation.name, "status": "starting"}
        except Exception as e:
            logger.error("Failed to start Compute instance: %s", e)
            raise

    async def stop_instance(self, zone: str, instance_name: str) -> dict[str, Any]:
        """Stop a Compute Engine instance."""
        try:
            project = self.config.project_id
            request = compute_v1.StopInstanceRequest(project=project, zone=zone, instance=instance_name)
            operation = self._clients["compute"].stop(request=request)
            return {"name": operation.name, "status": "stopping"}
        except Exception as e:
            logger.error("Failed to stop Compute instance: %s", e)
            raise

    # ==================== Cloud Functions Operations ====================

    async def list_functions(self, location: str = "-") -> list[CloudFunction]:
        """List all Cloud Functions."""
        try:
            project = self.config.project_id
            parent = f"projects/{project}/locations/{location}"

            request = functions_v1.ListFunctionsRequest(parent=parent)
            functions = list(self._clients["functions"].list_functions(request=request))

            result = []
            for func in functions:
                result.append(
                    CloudFunction(
                        name=func.name.split("/")[-1],
                        runtime=func.build_config.runtime,
                        entry_point=func.build_config.entry_point,
                        status=func.status,
                        available_memory_mb=func.build_config.available_memory_mb,
                        timeout_seconds=func.build_config.timeout_seconds,
                        update_time=datetime.fromisoformat(func.update_time.replace("Z", "+00:00")),
                    )
                )

            return result
        except Exception as e:
            logger.error("Failed to list Cloud Functions: %s", e)
            raise

    async def call_function(self, name: str, data: dict[str, Any], location: str = "-") -> dict[str, Any]:
        """Call a Cloud Function."""
        try:
            project = self.config.project_id
            function_path = f"projects/{project}/locations/{location}/functions/{name}"

            request = functions_v1.CallFunctionRequest(name=function_path, data=str(data).encode())

            response = self._clients["functions"].call_function(request=request)
            return {"result": response.result.decode() if response.result else None}
        except Exception as e:
            logger.error("Failed to call Cloud Function: %s", e)
            raise

    # ==================== Pub/Sub Operations ====================

    async def list_topics(self) -> list[str]:
        """List all Pub/Sub topics."""
        try:
            project = self.config.project_id
            project_path = f"projects/{project}"

            topics = list(self._clients["pubsub"].list_topics(project=project_path))
            return [topic.name for topic in topics]
        except Exception as e:
            logger.error("Failed to list Pub/Sub topics: %s", e)
            raise

    async def publish_message(self, topic: str, message: str, attributes: dict[str, str] | None = None) -> str:
        """Publish a message to a Pub/Sub topic."""
        try:
            project = self.config.project_id
            topic_path = self._clients["pubsub"].topic_path(project, topic)

            future = self._clients["pubsub"].publish(
                topic_path,
                data=message.encode(),
                **{"attributes": attributes} if attributes else {},
            )

            return future.result()
        except Exception as e:
            logger.error("Failed to publish Pub/Sub message: %s", e)
            raise

    # ==================== Utility Methods ====================

    async def test_connection(self) -> bool:
        """Test the Google Cloud connection."""
        try:
            list(self._clients["storage"].list_buckets(max_results=1))
            return True
        except Exception as e:
            logger.error("Google Cloud connection test failed: %s", e)
            return False

    def get_supported_services(self) -> list[str]:
        """Get list of supported Google Cloud services."""
        return ["storage", "compute", "functions", "pubsub"]
