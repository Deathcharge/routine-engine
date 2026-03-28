"""
AWS Connector for Helix Spirals
Provides integration with AWS services: S3, EC2, Lambda, etc.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError

    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class AWSConfig:
    """AWS connector configuration."""

    access_key_id: str
    secret_access_key: str
    region: str = "us-east-1"
    session_token: str | None = None
    role_arn: str | None = None


@dataclass
class S3Object:
    """Represents an S3 object."""

    bucket: str
    key: str
    size: int
    last_modified: datetime
    etag: str
    content_type: str | None = None


@dataclass
class EC2Instance:
    """Represents an EC2 instance."""

    instance_id: str
    instance_type: str
    state: str
    launch_time: datetime
    private_ip: str | None = None
    public_ip: str | None = None
    tags: dict[str, str] = None


@dataclass
class LambdaFunction:
    """Represents a Lambda function."""

    function_name: str
    runtime: str
    handler: str
    code_size: int
    timeout: int
    memory_size: int
    last_modified: datetime
    state: str


class AWSConnector:
    """
    AWS integration connector supporting multiple services.
    """

    def __init__(self, config: AWSConfig):
        if not AWS_AVAILABLE:
            raise ImportError("boto3 is required for AWS connector. Install with: pip install boto3")

        self.config = config
        self._clients = {}
        self._initialize_clients()

    def _initialize_clients(self):
        """Initialize AWS service clients."""
        session_kwargs = {
            "aws_access_key_id": self.config.access_key_id,
            "aws_secret_access_key": self.config.secret_access_key,
            "region_name": self.config.region,
        }

        if self.config.session_token:
            session_kwargs["aws_session_token"] = self.config.session_token

        if self.config.role_arn:
            # Assume role if specified
            sts_client = boto3.client("sts", **session_kwargs)
            response = sts_client.assume_role(RoleArn=self.config.role_arn, RoleSessionName="HelixSpirals")
            session_kwargs.update(
                {
                    "aws_access_key_id": response["Credentials"]["AccessKeyId"],
                    "aws_secret_access_key": response["Credentials"]["SecretAccessKey"],
                    "aws_session_token": response["Credentials"]["SessionToken"],
                }
            )

        # Initialize service clients
        self._clients["s3"] = boto3.client("s3", **session_kwargs)
        self._clients["ec2"] = boto3.client("ec2", **session_kwargs)
        self._clients["lambda"] = boto3.client("lambda", **session_kwargs)
        self._clients["sqs"] = boto3.client("sqs", **session_kwargs)
        self._clients["sns"] = boto3.client("sns", **session_kwargs)

        logger.info("✅ AWS clients initialized for region %s", self.config.region)

    # ==================== S3 Operations ====================

    async def list_s3_buckets(self) -> list[dict[str, Any]]:
        """List all S3 buckets."""
        try:
            response = self._clients["s3"].list_buckets()
            return response.get("Buckets", [])
        except ClientError as e:
            logger.error("Failed to list S3 buckets: %s", e)
            raise

    async def list_s3_objects(self, bucket: str, prefix: str = "", max_keys: int = 1000) -> list[S3Object]:
        """List objects in an S3 bucket."""
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)

            objects = []
            for obj in response.get("Contents", []):
                objects.append(
                    S3Object(
                        bucket=bucket,
                        key=obj["Key"],
                        size=obj["Size"],
                        last_modified=obj["LastModified"],
                        etag=obj["ETag"].strip('"'),
                        content_type=obj.get("ContentType"),
                    )
                )

            return objects
        except ClientError as e:
            logger.error("Failed to list S3 objects: %s", e)
            raise

    async def upload_s3_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> dict[str, str]:
        """Upload an object to S3."""
        try:
            self.s3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
            return {"bucket": bucket, "key": key, "status": "uploaded"}
        except ClientError as e:
            logger.error("Failed to upload S3 object: %s", e)
            raise

    async def download_s3_object(self, bucket: str, key: str) -> bytes:
        """Download an object from S3."""
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except ClientError as e:
            logger.error("Failed to download S3 object: %s", e)
            raise

    async def delete_s3_object(self, bucket: str, key: str) -> bool:
        """Delete an object from S3."""
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            logger.error("Failed to delete S3 object: %s", e)
            return False

    # ==================== EC2 Operations ====================

    async def describe_instances(self) -> list[EC2Instance]:
        """Describe all EC2 instances."""
        try:
            response = self.ec2_client.describe_instances()

            instances = []
            for reservation in response["Reservations"]:
                for instance in reservation["Instances"]:
                    tags = {tag["Key"]: tag["Value"] for tag in instance.get("Tags", [])}
                    instances.append(
                        EC2Instance(
                            instance_id=instance["InstanceId"],
                            instance_type=instance["InstanceType"],
                            state=instance["State"]["Name"],
                            launch_time=instance["LaunchTime"],
                            private_ip=instance.get("PrivateIpAddress"),
                            public_ip=instance.get("PublicIpAddress"),
                            tags=tags,
                        )
                    )

            return instances
        except ClientError as e:
            logger.error("Failed to describe EC2 instances: %s", e)
            raise

    async def start_instance(self, instance_id: str) -> dict[str, Any]:
        """Start an EC2 instance."""
        try:
            response = self.ec2_client.start_instances(InstanceIds=[instance_id])
            return response
        except ClientError as e:
            logger.error("Failed to start EC2 instance: %s", e)
            raise

    async def stop_instance(self, instance_id: str) -> dict[str, Any]:
        """Stop an EC2 instance."""
        try:
            response = self.ec2_client.stop_instances(InstanceIds=[instance_id])
            return response
        except ClientError as e:
            logger.error("Failed to stop EC2 instance: %s", e)
            raise

    async def reboot_instance(self, instance_id: str) -> dict[str, Any]:
        """Reboot an EC2 instance."""
        try:
            response = self._clients["ec2"].reboot_instances(InstanceIds=[instance_id])
            return response
        except ClientError as e:
            logger.error("Failed to reboot EC2 instance: %s", e)
            raise

    # ==================== Lambda Operations ====================

    async def list_lambda_functions(self) -> list[LambdaFunction]:
        """List all Lambda functions."""
        try:

            response = self._clients["lambda"].list_functions()
            functions = []
            for func in response["Functions"]:
                functions.append(
                    LambdaFunction(
                        function_name=func["FunctionName"],
                        runtime=func["Runtime"],
                        handler=func["Handler"],
                        code_size=func["CodeSize"],
                        timeout=func["Timeout"],
                        memory_size=func["MemorySize"],
                        last_modified=func["LastModified"],
                        state=func.get("State", "Active"),
                    )
                )

            return functions
        except ClientError as e:
            logger.error("Failed to list Lambda functions: %s", e)
            raise

    async def invoke_lambda(
        self,
        function_name: str,
        payload: dict[str, Any],
        invocation_type: str = "RequestResponse",
    ) -> dict[str, Any]:
        """Invoke a Lambda function."""
        try:
            response = self._clients["lambda"].invoke(
                FunctionName=function_name,
                InvocationType=invocation_type,
                Payload=json.dumps(payload),
            )

            if invocation_type == "RequestResponse":
                import json

                return json.loads(response["Payload"].read())

            return {"status": "invoked"}
        except ClientError as e:
            logger.error("Failed to invoke Lambda function: %s", e)
            raise

    async def create_lambda_function(
        self,
        function_name: str,
        runtime: str,
        handler: str,
        code_zip: bytes,
        role_arn: str,
        timeout: int = 30,
        memory_size: int = 128,
    ) -> dict[str, Any]:
        """Create a Lambda function."""
        try:
            response = self.lambda_client.create_function(
                FunctionName=function_name,
                Runtime=runtime,
                Role=role_arn,
                Handler=handler,
                Code={"ZipFile": code_zip},
                Timeout=timeout,
                MemorySize=memory_size,
            )
            return response
        except ClientError as e:
            logger.error("Failed to create Lambda function: %s", e)
            raise

    # ==================== SQS Operations ====================

    async def list_sqs_queues(self) -> list[str]:
        """List all SQS queues."""
        try:
            response = self.sqs_client.list_queues()
            return response.get("QueueUrls", [])
        except ClientError as e:
            logger.error("Failed to list SQS queues: %s", e)
            raise

    async def send_sqs_message(
        self,
        queue_url: str,
        message_body: str,
        message_attributes: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Send a message to an SQS queue."""
        try:
            kwargs = {
                "QueueUrl": queue_url,
                "MessageBody": message_body,
            }

            if message_attributes:
                kwargs["MessageAttributes"] = message_attributes

            response = self._clients["sqs"].send_message(**kwargs)
            return response
        except ClientError as e:
            logger.error("Failed to send SQS message: %s", e)
            raise

    async def receive_sqs_message(
        self, queue_url: str, max_messages: int = 1, wait_time: int = 20
    ) -> list[dict[str, Any]]:
        """Receive messages from an SQS queue."""
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
            )
            return response.get("Messages", [])
        except ClientError as e:
            logger.error("Failed to receive SQS messages: %s", e)
            raise

    # ==================== SNS Operations ====================

    async def list_sns_topics(self) -> list[dict[str, Any]]:
        """List all SNS topics."""
        try:
            response = self.sns_client.list_topics()
            return response.get("Topics", [])
        except ClientError as e:
            logger.error("Failed to list SNS topics: %s", e)
            raise

    async def publish_sns_message(self, topic_arn: str, message: str, subject: str | None = None) -> dict[str, Any]:
        """Publish a message to an SNS topic."""
        try:
            kwargs = {
                "TopicArn": topic_arn,
                "Message": message,
            }

            if subject:
                kwargs["Subject"] = subject

            response = self._clients["sns"].publish(**kwargs)
            return response
        except ClientError as e:
            logger.error("Failed to publish SNS message: %s", e)
            raise

    # ==================== Utility Methods ====================

    async def test_connection(self) -> bool:
        """Test the AWS connection."""
        try:
            self._clients["sts"].get_caller_identity()
            return True
        except (ClientError, NoCredentialsError) as e:
            logger.error("AWS connection test failed: %s", e)
            return False

    def get_supported_services(self) -> list[str]:
        """Get list of supported AWS services."""
        return ["s3", "ec2", "lambda", "sqs", "sns"]
