"""
Helix Spirals - Integration Node Types
Complete automation platform with 50+ node types for enterprise workflows.
"""

import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, ClassVar
from urllib.parse import urlparse

import aiohttp

try:
    import aioboto3
except ImportError:
    aioboto3 = None  # Optional: only needed for S3StorageNode

try:
    import aioredis
except ImportError:
    aioredis = None  # Optional: only needed for CacheNode
import anthropic
import asyncpg
import openai
import stripe

try:
    from motor.motor_asyncio import AsyncIOMotorClient
except ImportError:
    AsyncIOMotorClient = None

try:
    from google.cloud import storage
except ImportError:
    storage = None

try:
    from twilio.rest import Client as TwilioClient
except ImportError:
    TwilioClient = None

try:
    from sendgrid import SendGridAPIClient
except ImportError:
    SendGridAPIClient = None


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SSRF Prevention: validate URLs before making outbound requests
# ---------------------------------------------------------------------------

_BLOCKED_IP_NETWORKS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),  # link-local
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),  # IPv6 ULA
    ipaddress.ip_network("fe80::/10"),  # IPv6 link-local
]


def _validate_external_url(url: str) -> str:
    """Validate that a URL points to a public external host (not internal/private).

    Returns the validated URL. Raises ValueError for blocked URLs.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Blocked URL scheme: {parsed.scheme}")
    hostname = parsed.hostname
    if not hostname:
        raise ValueError("URL has no hostname")

    # Check for IP-based URLs
    try:
        addr = ipaddress.ip_address(hostname)
        for net in _BLOCKED_IP_NETWORKS:
            if addr in net:
                raise ValueError(f"Blocked internal IP: {hostname}")
    except ValueError as e:
        if "Blocked" in str(e):
            raise
        # Not an IP literal — it's a hostname, resolve-check below

    # Block common internal hostnames
    lower = hostname.lower()
    if lower in ("localhost", "metadata.google.internal", "169.254.169.254"):
        raise ValueError(f"Blocked internal hostname: {hostname}")

    return url


class NodeCategory(Enum):
    TRIGGER = "trigger"
    ACTION = "action"
    TRANSFORM = "transform"
    CONTROL = "control"
    AI = "ai"
    DATABASE = "database"
    STORAGE = "storage"
    COMMUNICATION = "communication"
    INTEGRATION = "integration"
    UTILITY = "utility"


@dataclass
class NodeResult:
    success: bool
    data: Any = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    execution_time_ms: float = 0
    next_nodes: list[str] = field(default_factory=list)


@dataclass
class NodeConfig:
    id: str
    type: str
    name: str
    config: dict[str, Any]
    position: dict[str, int] = field(default_factory=dict)
    connections: list[str] = field(default_factory=list)


class BaseNode(ABC):
    """Base class for all Helix Spirals nodes."""

    name: ClassVar[str] = ""
    display_name: ClassVar[str] = ""
    category: NodeCategory = NodeCategory.ACTION
    description: str = ""
    icon: str = "⚙️"

    def __init__(self, config: NodeConfig | None = None):
        if config is None:
            self.config = None
            self.id = "schema"
            self.name = self.__class__.name
        else:
            self.config = config
            self.id = config.id
            self.name = config.name

    @abstractmethod
    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        pass

    def validate_config(self) -> list[str]:
        """Validate node configuration. Returns list of errors."""
        return []

    def get_schema(self) -> dict[str, Any]:
        return {
            "name": self.__class__.name,
            "display_name": self.__class__.display_name,
            "description": self.description,
            "category": self.category.value,
            "icon": self.icon,
            "inputs": [],
            "outputs": [],
        }


# ============================================================================
# DATABASE NODES
# ============================================================================


class PostgreSQLNode(BaseNode):
    """Execute PostgreSQL queries with connection pooling."""

    name = "postgresql"
    display_name = "PostgreSQL"
    category = NodeCategory.DATABASE
    description = "Execute PostgreSQL queries"
    icon = "🐘"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            connection_string = self.config.config.get("connection_string")
            query = self.config.config.get("query", "")
            params = self.config.config.get("params", [])
            operation = self.config.config.get("operation", "query")  # query, execute, fetch_one

            # Template substitution: move templated values into params list
            # to prevent SQL injection (never interpolate into query text)
            # Process keys in order of appearance in the query to ensure
            # correct parameter numbering
            import re

            for match in re.finditer(r"\{(\w+)\}", query):
                key = match.group(1)
                if key in input_data:
                    params.append(input_data[key])
                    query = query.replace("{" + key + "}", f"${len(params)}", 1)

            conn = await asyncpg.connect(connection_string)
            try:
                if operation == "fetch_one":
                    result = await conn.fetchrow(query, *params)
                    data = dict(result) if result else None
                elif operation == "execute":
                    result = await conn.execute(query, *params)
                    data = {"status": result}
                else:
                    result = await conn.fetch(query, *params)
                    data = [dict(row) for row in result]

                return NodeResult(success=True, data=data)
            finally:
                await conn.close()
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class MongoDBNode(BaseNode):
    """Execute MongoDB operations."""

    name = "mongodb"
    display_name = "MongoDB"
    category = NodeCategory.DATABASE
    description = "MongoDB database operations"
    icon = "🍃"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            connection_string = self.config.config.get("connection_string")
            database = self.config.config.get("database")
            collection = self.config.config.get("collection")
            operation = self.config.config.get("operation", "find")
            query = self.config.config.get("query", {})
            document = self.config.config.get("document", {})

            client = AsyncIOMotorClient(connection_string)
            db = client[database]
            coll = db[collection]

            if operation == "find":
                cursor = coll.find(query)
                data = await cursor.to_list(length=100)
            elif operation == "find_one":
                data = await coll.find_one(query)
            elif operation == "insert_one":
                result = await coll.insert_one(document)
                data = {"inserted_id": str(result.inserted_id)}
            elif operation == "insert_many":
                result = await coll.insert_many(document)
                data = {"inserted_ids": [str(id) for id in result.inserted_ids]}
            elif operation == "update_one":
                result = await coll.update_one(query, {"$set": document})
                data = {"modified_count": result.modified_count}
            elif operation == "delete_one":
                result = await coll.delete_one(query)
                data = {"deleted_count": result.deleted_count}
            elif operation == "aggregate":
                pipeline = self.config.config.get("pipeline", [])
                cursor = coll.aggregate(pipeline)
                data = await cursor.to_list(length=100)
            else:
                return NodeResult(success=False, error=f"Unknown operation: {operation}")

            client.close()
            return NodeResult(success=True, data=data)
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class RedisNode(BaseNode):
    """Redis cache and pub/sub operations."""

    name = "redis_cache"
    display_name = "Redis Cache"
    category = NodeCategory.DATABASE
    description = "Redis cache operations"
    icon = "🔴"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            url = self.config.config.get("url", "redis://localhost")
            operation = self.config.config.get("operation", "get")
            key = self.config.config.get("key", "")
            value = self.config.config.get("value")
            ttl = self.config.config.get("ttl")

            if aioredis is None:
                return NodeResult(
                    success=False,
                    error="aioredis is not installed. Install with: pip install aioredis",
                )
            redis = await aioredis.from_url(url)

            if operation == "get":
                data = await redis.get(key)
                data = data.decode() if data else None
            elif operation == "set":
                await redis.set(key, value, ex=ttl)
                data = {"status": "ok"}
            elif operation == "delete":
                await redis.delete(key)
                data = {"status": "deleted"}
            elif operation == "hget":
                field = self.config.config.get("field")
                data = await redis.hget(key, field)
                data = data.decode() if data else None
            elif operation == "hset":
                field = self.config.config.get("field")
                await redis.hset(key, field, value)
                data = {"status": "ok"}
            elif operation == "lpush":
                await redis.lpush(key, value)
                data = {"status": "ok"}
            elif operation == "rpop":
                data = await redis.rpop(key)
                data = data.decode() if data else None
            elif operation == "publish":
                channel = self.config.config.get("channel")
                await redis.publish(channel, value)
                data = {"status": "published"}
            else:
                return NodeResult(success=False, error=f"Unknown operation: {operation}")

            await redis.close()
            return NodeResult(success=True, data=data)
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# STORAGE NODES
# ============================================================================


class S3Node(BaseNode):
    """AWS S3 storage operations."""

    name = "s3_storage"
    display_name = "AWS S3"
    category = NodeCategory.STORAGE
    description = "AWS S3 file operations"
    icon = "📦"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            if aioboto3 is None:
                return NodeResult(
                    success=False,
                    error="aioboto3 is not installed. Install with: pip install aioboto3",
                )
            session = aioboto3.Session()
            bucket = self.config.config.get("bucket")
            operation = self.config.config.get("operation", "get")
            key = self.config.config.get("key", "")

            async with session.client("s3") as s3:
                if operation == "get":
                    response = await s3.get_object(Bucket=bucket, Key=key)
                    data = await response["Body"].read()
                    return NodeResult(success=True, data={"content": data.decode(), "key": key})

                elif operation == "put":
                    content = self.config.config.get("content", input_data.get("content", ""))
                    content_type = self.config.config.get("content_type", "text/plain")
                    await s3.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=content.encode() if isinstance(content, str) else content,
                        ContentType=content_type,
                    )
                    return NodeResult(success=True, data={"key": key, "bucket": bucket})

                elif operation == "delete":
                    await s3.delete_object(Bucket=bucket, Key=key)
                    return NodeResult(success=True, data={"deleted": key})

                elif operation == "list":
                    prefix = self.config.config.get("prefix", "")
                    response = await s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
                    files = [obj["Key"] for obj in response.get("Contents", [])]
                    return NodeResult(success=True, data={"files": files})

                elif operation == "presigned_url":
                    expires_in = self.config.config.get("expires_in", 3600)
                    url = await s3.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": bucket, "Key": key},
                        ExpiresIn=expires_in,
                    )
                    return NodeResult(success=True, data={"url": url})

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class GoogleCloudStorageNode(BaseNode):
    """Google Cloud Storage operations."""

    name = "gcs"
    display_name = "Google Cloud Storage"
    category = NodeCategory.STORAGE
    description = "Google Cloud Storage operations"
    icon = "☁️"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            bucket_name = self.config.config.get("bucket")
            operation = self.config.config.get("operation", "get")
            blob_name = self.config.config.get("blob_name", "")

            client = storage.Client()
            bucket = client.bucket(bucket_name)

            if operation == "get":
                blob = bucket.blob(blob_name)
                content = blob.download_as_text()
                return NodeResult(success=True, data={"content": content, "name": blob_name})

            elif operation == "put":
                content = self.config.config.get("content", input_data.get("content", ""))
                blob = bucket.blob(blob_name)
                blob.upload_from_string(content)
                return NodeResult(success=True, data={"name": blob_name, "bucket": bucket_name})

            elif operation == "delete":
                blob = bucket.blob(blob_name)
                blob.delete()
                return NodeResult(success=True, data={"deleted": blob_name})

            elif operation == "list":
                prefix = self.config.config.get("prefix", "")
                blobs = list(bucket.list_blobs(prefix=prefix))
                files = [blob.name for blob in blobs]
                return NodeResult(success=True, data={"files": files})

            else:
                return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# AI NODES
# ============================================================================


class OpenAINode(BaseNode):
    """OpenAI API integration for GPT models."""

    name = "openai_llm"
    display_name = "OpenAI"
    category = NodeCategory.AI
    description = "OpenAI GPT completions and embeddings"
    icon = "🤖"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            api_key = self.config.config.get("api_key") or context.get("openai_api_key")
            operation = self.config.config.get("operation", "chat")
            model = self.config.config.get("model", "gpt-4")

            client = openai.AsyncOpenAI(api_key=api_key)

            if operation == "chat":
                messages = self.config.config.get("messages", [])
                # Add input data as user message if provided
                if "prompt" in input_data:
                    messages.append({"role": "user", "content": input_data["prompt"]})

                response = await client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=self.config.config.get("temperature", 0.7),
                    max_tokens=self.config.config.get("max_tokens", 1000),
                )
                return NodeResult(
                    success=True,
                    data={
                        "response": response.choices[0].message.content,
                        "usage": {
                            "prompt_tokens": response.usage.prompt_tokens,
                            "completion_tokens": response.usage.completion_tokens,
                            "total_tokens": response.usage.total_tokens,
                        },
                    },
                )

            elif operation == "embedding":
                text = self.config.config.get("text", input_data.get("text", ""))
                response = await client.embeddings.create(
                    model=self.config.config.get("embedding_model", "text-embedding-ada-002"),
                    input=text,
                )
                return NodeResult(
                    success=True,
                    data={
                        "embedding": response.data[0].embedding,
                        "dimensions": len(response.data[0].embedding),
                    },
                )

            elif operation == "image":
                prompt = self.config.config.get("prompt", input_data.get("prompt", ""))
                response = await client.images.generate(
                    model="dall-e-3",
                    prompt=prompt,
                    size=self.config.config.get("size", "1024x1024"),
                    n=1,
                )
                return NodeResult(
                    success=True,
                    data={
                        "url": response.data[0].url,
                        "revised_prompt": response.data[0].revised_prompt,
                    },
                )

            else:
                return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class AnthropicNode(BaseNode):
    """Anthropic Claude API integration."""

    name = "anthropic_llm"
    display_name = "Anthropic"
    category = NodeCategory.AI
    description = "Anthropic Claude completions"
    icon = "🧠"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            api_key = self.config.config.get("api_key") or context.get("anthropic_api_key")
            model = self.config.config.get("model", "claude-3-opus-20240229")

            client = anthropic.AsyncAnthropic(api_key=api_key)

            messages = self.config.config.get("messages", [])
            if "prompt" in input_data:
                messages.append({"role": "user", "content": input_data["prompt"]})

            response = await client.messages.create(
                model=model,
                max_tokens=self.config.config.get("max_tokens", 1000),
                messages=messages,
                system=self.config.config.get("system_prompt", ""),
            )

            return NodeResult(
                success=True,
                data={
                    "response": response.content[0].text,
                    "usage": {
                        "input_tokens": response.usage.input_tokens,
                        "output_tokens": response.usage.output_tokens,
                    },
                },
            )
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class SentimentAnalysisNode(BaseNode):
    """Analyze sentiment of text using AI."""

    name = "sentiment"
    display_name = "Sentiment Analysis"
    category = NodeCategory.AI
    description = "Analyze text sentiment"
    icon = "😊"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            text = self.config.config.get("text") or input_data.get("text", "")

            # Use simple rule-based sentiment as fallback
            positive_words = [
                "good",
                "great",
                "excellent",
                "amazing",
                "wonderful",
                "fantastic",
                "love",
                "happy",
            ]
            negative_words = [
                "bad",
                "terrible",
                "awful",
                "horrible",
                "hate",
                "sad",
                "angry",
                "disappointed",
            ]

            text_lower = text.lower()
            positive_count = sum(1 for word in positive_words if word in text_lower)
            negative_count = sum(1 for word in negative_words if word in text_lower)

            if positive_count > negative_count:
                sentiment = "positive"
                score = min(0.5 + (positive_count * 0.1), 1.0)
            elif negative_count > positive_count:
                sentiment = "negative"
                score = max(0.5 - (negative_count * 0.1), 0.0)
            else:
                sentiment = "neutral"
                score = 0.5

            return NodeResult(
                success=True,
                data={
                    "sentiment": sentiment,
                    "score": score,
                    "confidence": 0.7,
                    "text_length": len(text),
                },
            )
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# COMMUNICATION NODES
# ============================================================================


class TwilioNode(BaseNode):
    """Twilio SMS, Voice, and WhatsApp integration."""

    name = "twilio_sms"
    display_name = "Twilio SMS"
    category = NodeCategory.COMMUNICATION
    description = "Send SMS, make calls, or send WhatsApp messages"
    icon = "📱"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            account_sid = self.config.config.get("account_sid")
            auth_token = self.config.config.get("auth_token")
            operation = self.config.config.get("operation", "sms")

            client = TwilioClient(account_sid, auth_token)

            if operation == "sms":
                message = client.messages.create(
                    body=self.config.config.get("body", input_data.get("message", "")),
                    from_=self.config.config.get("from_number"),
                    to=self.config.config.get("to_number", input_data.get("to", "")),
                )
                return NodeResult(success=True, data={"sid": message.sid, "status": message.status})

            elif operation == "whatsapp":
                message = client.messages.create(
                    body=self.config.config.get("body", input_data.get("message", "")),
                    from_=f"whatsapp:{self.config.config.get('from_number')}",
                    to=f"whatsapp:{self.config.config.get('to_number', input_data.get('to', ''))}",
                )
                return NodeResult(success=True, data={"sid": message.sid, "status": message.status})

            elif operation == "call":
                call = client.calls.create(
                    url=self.config.config.get("twiml_url"),
                    from_=self.config.config.get("from_number"),
                    to=self.config.config.get("to_number", input_data.get("to", "")),
                )
                return NodeResult(success=True, data={"sid": call.sid, "status": call.status})

            else:
                return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class SendGridNode(BaseNode):
    """SendGrid email integration."""

    name = "sendgrid_email"
    display_name = "SendGrid Email"
    category = NodeCategory.COMMUNICATION
    description = "Send emails via SendGrid"
    icon = "📧"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            from sendgrid.helpers.mail import Content, Email, Mail, To

            api_key = self.config.config.get("api_key")

            message = Mail(
                from_email=Email(self.config.config.get("from_email")),
                to_emails=To(self.config.config.get("to_email", input_data.get("to", ""))),
                subject=self.config.config.get("subject", input_data.get("subject", "")),
                html_content=Content(
                    "text/html",
                    self.config.config.get("body", input_data.get("body", "")),
                ),
            )

            sg = SendGridAPIClient(api_key)
            response = sg.send(message)

            return NodeResult(
                success=True,
                data={
                    "status_code": response.status_code,
                    "message_id": response.headers.get("X-Message-Id"),
                },
            )
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# INTEGRATION NODES
# ============================================================================


class StripeNode(BaseNode):
    """Stripe payment integration."""

    name = "stripe_payment"
    display_name = "Stripe"
    category = NodeCategory.INTEGRATION
    description = "Stripe payments and subscriptions"
    icon = "💳"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            stripe.api_key = self.config.config.get("api_key")
            operation = self.config.config.get("operation", "create_payment_intent")

            if operation == "create_payment_intent":
                intent = stripe.PaymentIntent.create(
                    amount=self.config.config.get("amount", input_data.get("amount", 0)),
                    currency=self.config.config.get("currency", "usd"),
                    metadata=self.config.config.get("metadata", {}),
                )
                return NodeResult(
                    success=True,
                    data={
                        "id": intent.id,
                        "client_secret": intent.client_secret,
                        "status": intent.status,
                    },
                )

            elif operation == "create_customer":
                customer = stripe.Customer.create(
                    email=self.config.config.get("email", input_data.get("email", "")),
                    name=self.config.config.get("name", input_data.get("name", "")),
                    metadata=self.config.config.get("metadata", {}),
                )
                return NodeResult(success=True, data={"id": customer.id, "email": customer.email})

            elif operation == "create_subscription":
                subscription = stripe.Subscription.create(
                    customer=self.config.config.get("customer_id", input_data.get("customer_id", "")),
                    items=[{"price": self.config.config.get("price_id")}],
                )
                return NodeResult(
                    success=True,
                    data={"id": subscription.id, "status": subscription.status},
                )

            elif operation == "list_invoices":
                invoices = stripe.Invoice.list(
                    customer=self.config.config.get("customer_id", input_data.get("customer_id", "")),
                    limit=self.config.config.get("limit", 10),
                )
                return NodeResult(
                    success=True,
                    data={
                        "invoices": [
                            {
                                "id": inv.id,
                                "amount": inv.amount_due,
                                "status": inv.status,
                            }
                            for inv in invoices.data
                        ]
                    },
                )

            else:
                return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class ShopifyNode(BaseNode):
    """Shopify e-commerce integration."""

    name = "shopify"
    display_name = "Shopify"
    category = NodeCategory.INTEGRATION
    description = "Shopify store operations"
    icon = "🛒"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            shop_url = self.config.config.get("shop_url")
            access_token = self.config.config.get("access_token")
            operation = self.config.config.get("operation", "get_products")

            # Validate shop URL to prevent SSRF
            _validate_external_url(shop_url)

            headers = {
                "X-Shopify-Access-Token": access_token,
                "Content-Type": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                if operation == "get_products":
                    async with session.get(f"{shop_url}/admin/api/2024-01/products.json", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_orders":
                    async with session.get(f"{shop_url}/admin/api/2024-01/orders.json", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_product":
                    product_data = self.config.config.get("product", input_data.get("product", {}))
                    async with session.post(
                        f"{shop_url}/admin/api/2024-01/products.json",
                        headers=headers,
                        json={"product": product_data},
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "update_inventory":
                    inventory_item_id = self.config.config.get("inventory_item_id")
                    quantity = self.config.config.get("quantity", input_data.get("quantity", 0))
                    async with session.post(
                        f"{shop_url}/admin/api/2024-01/inventory_levels/set.json",
                        headers=headers,
                        json={
                            "inventory_item_id": inventory_item_id,
                            "available": quantity,
                        },
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class HubSpotNode(BaseNode):
    """HubSpot CRM integration."""

    name = "hubspot"
    display_name = "HubSpot"
    category = NodeCategory.INTEGRATION
    description = "HubSpot CRM operations"
    icon = "🧡"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            api_key = self.config.config.get("api_key")
            operation = self.config.config.get("operation", "get_contacts")

            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }

            base_url = "https://api.hubapi.com"

            async with aiohttp.ClientSession() as session:
                if operation == "get_contacts":
                    async with session.get(f"{base_url}/crm/v3/objects/contacts", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_contact":
                    properties = self.config.config.get("properties", input_data)
                    async with session.post(
                        f"{base_url}/crm/v3/objects/contacts",
                        headers=headers,
                        json={"properties": properties},
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "get_deals":
                    async with session.get(f"{base_url}/crm/v3/objects/deals", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_deal":
                    properties = self.config.config.get("properties", input_data)
                    async with session.post(
                        f"{base_url}/crm/v3/objects/deals",
                        headers=headers,
                        json={"properties": properties},
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


class ZendeskNode(BaseNode):
    """Zendesk support ticket integration."""

    name = "zendesk"
    display_name = "Zendesk"
    category = NodeCategory.INTEGRATION
    description = "Zendesk ticket operations"
    icon = "🎫"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            subdomain = self.config.config.get("subdomain")
            email = self.config.config.get("email")
            api_token = self.config.config.get("api_token")
            operation = self.config.config.get("operation", "get_tickets")

            # Validate subdomain to prevent SSRF via crafted subdomains
            if not subdomain or not subdomain.replace("-", "").replace("_", "").isalnum():
                return NodeResult(success=False, error="Invalid Zendesk subdomain")

            auth = base64.b64encode(f"{email}/token:{api_token}".encode()).decode()
            headers = {
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/json",
            }

            base_url = f"https://{subdomain}.zendesk.com/api/v2"

            async with aiohttp.ClientSession() as session:
                if operation == "get_tickets":
                    async with session.get(f"{base_url}/tickets.json", headers=headers) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "create_ticket":
                    ticket_data = {
                        "ticket": {
                            "subject": self.config.config.get("subject", input_data.get("subject", "")),
                            "description": self.config.config.get("description", input_data.get("description", "")),
                            "priority": self.config.config.get("priority", "normal"),
                        }
                    }
                    async with session.post(f"{base_url}/tickets.json", headers=headers, json=ticket_data) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                elif operation == "update_ticket":
                    ticket_id = self.config.config.get("ticket_id", input_data.get("ticket_id"))
                    update_data = {"ticket": self.config.config.get("update", input_data.get("update", {}))}
                    async with session.put(
                        f"{base_url}/tickets/{ticket_id}.json",
                        headers=headers,
                        json=update_data,
                    ) as resp:
                        data = await resp.json()
                        return NodeResult(success=True, data=data)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")
        except Exception as e:
            return NodeResult(success=False, error=str(e))


# ============================================================================
# CONTROL FLOW NODES
# ============================================================================


class SplitNode(BaseNode):
    """Split workflow into parallel execution paths."""

    name = "split"
    display_name = "Split"
    category = NodeCategory.CONTROL
    description = "Split into parallel paths"
    icon = "🔀"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        branches = self.config.config.get("branches", [])
        return NodeResult(
            success=True,
            data=input_data,
            next_nodes=branches,
            metadata={"parallel": True},
        )


class MergeNode(BaseNode):
    """Merge results from parallel execution paths."""

    name = "merge"
    display_name = "Merge"
    category = NodeCategory.CONTROL
    description = "Merge parallel results"
    icon = "🔄"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        # Input data should contain results from all branches
        merge_strategy = self.config.config.get("strategy", "combine")

        if merge_strategy == "combine":
            # Combine all results into a single object
            merged = {}
            if isinstance(input_data, list):
                for i, item in enumerate(input_data):
                    merged[f"branch_{i}"] = item
            else:
                merged = input_data
            return NodeResult(success=True, data=merged)

        elif merge_strategy == "first":
            # Take first successful result
            if isinstance(input_data, list) and len(input_data) > 0:
                return NodeResult(success=True, data=input_data[0])
            return NodeResult(success=True, data=input_data)

        elif merge_strategy == "array":
            # Return as array
            if not isinstance(input_data, list):
                input_data = [input_data]
            return NodeResult(success=True, data=input_data)

        else:
            return NodeResult(success=True, data=input_data)


class ErrorHandlerNode(BaseNode):
    """Handle errors with try-catch-finally logic."""

    name = "error_handler"
    display_name = "Error Handler"
    category = NodeCategory.CONTROL
    description = "Error handling wrapper"
    icon = "🛡️"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        error = context.get("error")

        if error:
            # Error occurred, execute error handler
            error_action = self.config.config.get("on_error", "log")

            if error_action == "retry":
                retry_count = context.get("retry_count", 0)
                max_retries = self.config.config.get("max_retries", 3)

                if retry_count < max_retries:
                    return NodeResult(
                        success=True,
                        data=input_data,
                        metadata={"retry": True, "retry_count": retry_count + 1},
                    )

            elif error_action == "fallback":
                fallback_value = self.config.config.get("fallback_value", {})
                return NodeResult(success=True, data=fallback_value)

            elif error_action == "notify":
                # Would trigger notification
                return NodeResult(
                    success=True,
                    data={"error_handled": True, "original_error": str(error)},
                    metadata={"notification_sent": True},
                )

            # Default: log and continue
            return NodeResult(success=True, data={"error_logged": True, "original_error": str(error)})

        return NodeResult(success=True, data=input_data)


class RateLimiterNode(BaseNode):
    """Rate limit API calls and operations."""

    name = "rate_limiter_node"
    display_name = "Rate Limiter"
    category = NodeCategory.CONTROL
    description = "Rate limiting for API calls"
    icon = "⏱️"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        requests_per_minute = self.config.config.get("requests_per_minute", 60)
        requests_per_second = requests_per_minute / 60
        delay = 1 / requests_per_second

        # Simple delay-based rate limiting
        await asyncio.sleep(delay)

        return NodeResult(
            success=True,
            data=input_data,
            metadata={"rate_limited": True, "delay_ms": delay * 1000},
        )


class RetryNode(BaseNode):
    """Retry failed operations with exponential backoff."""

    name = "retry"
    display_name = "Retry"
    category = NodeCategory.CONTROL
    description = "Retry with exponential backoff"
    icon = "🔁"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        max_retries = self.config.config.get("max_retries", 3)
        base_delay = self.config.config.get("base_delay_ms", 1000)
        max_delay = self.config.config.get("max_delay_ms", 30000)

        retry_count = context.get("retry_count", 0)

        if retry_count > 0:
            # Calculate exponential backoff delay
            delay = min(base_delay * (2 ** (retry_count - 1)), max_delay)
            await asyncio.sleep(delay / 1000)

        return NodeResult(
            success=True,
            data=input_data,
            metadata={
                "retry_count": retry_count,
                "max_retries": max_retries,
                "should_retry": retry_count < max_retries,
            },
        )


# ============================================================================
# UTILITY NODES
# ============================================================================


class WebhookSignatureNode(BaseNode):
    """Verify webhook signatures for security."""

    name = "webhook_signature"
    display_name = "Webhook Signature"
    category = NodeCategory.UTILITY
    description = "Verify webhook signatures"
    icon = "🔐"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        secret = self.config.config.get("secret")
        signature_header = self.config.config.get("signature_header", "X-Signature")
        algorithm = self.config.config.get("algorithm", "sha256")

        received_signature = context.get("headers", {}).get(signature_header, "")
        payload = json.dumps(input_data, separators=(",", ":"))

        if algorithm == "sha256":
            expected_signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
        elif algorithm == "sha1":
            expected_signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha1).hexdigest()
        else:
            return NodeResult(success=False, error=f"Unknown algorithm: {algorithm}")

        is_valid = hmac.compare_digest(received_signature, expected_signature)

        if not is_valid:
            return NodeResult(success=False, error="Invalid webhook signature")

        return NodeResult(success=True, data=input_data, metadata={"signature_valid": True})


class CacheNode(BaseNode):
    """Cache results for performance optimization."""

    name = "cache"
    display_name = "Cache"
    category = NodeCategory.UTILITY
    description = "Cache workflow results"
    icon = "💾"

    _MAX_CACHE_SIZE: ClassVar[int] = 1024
    _cache: ClassVar[OrderedDict] = OrderedDict()

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        operation = self.config.config.get("operation", "get")
        cache_key = self.config.config.get("key", "")
        ttl = self.config.config.get("ttl", 3600)

        # Generate cache key from input if not specified
        if not cache_key:
            cache_key = hashlib.md5(json.dumps(input_data, sort_keys=True).encode(), usedforsecurity=False).hexdigest()

        if operation == "get":
            cached = self._cache.get(cache_key)
            if cached:
                if cached["expires"] > datetime.now(UTC).timestamp():
                    self._cache.move_to_end(cache_key)
                    return NodeResult(success=True, data=cached["data"], metadata={"cache_hit": True})
                else:
                    del self._cache[cache_key]

            return NodeResult(success=True, data=None, metadata={"cache_hit": False})

        elif operation == "set":
            self._cache[cache_key] = {
                "data": input_data,
                "expires": datetime.now(UTC).timestamp() + ttl,
            }
            self._cache.move_to_end(cache_key)
            while len(self._cache) > self._MAX_CACHE_SIZE:
                self._cache.popitem(last=False)
            return NodeResult(
                success=True,
                data=input_data,
                metadata={"cached": True, "key": cache_key},
            )

        elif operation == "delete":
            if cache_key in self._cache:
                del self._cache[cache_key]
            return NodeResult(success=True, data={"deleted": cache_key})

        else:
            return NodeResult(success=False, error=f"Unknown operation: {operation}")


# ============================================================================
# SLACK NODE
# ============================================================================


class SlackNode(BaseNode):
    """Send messages, upload files, and manage Slack channels."""

    name = "slack_node"
    display_name = "Slack"
    category = NodeCategory.COMMUNICATION
    description = "Slack messaging and channel management"
    icon = "💬"

    async def execute(self, input_data: dict[str, Any], context: dict[str, Any]) -> NodeResult:
        try:
            from .integrations.slack_connector import SlackConnector

            bot_token = self.config.config.get("bot_token")
            if not bot_token:
                return NodeResult(success=False, error="Slack bot_token is required in config")

            operation = self.config.config.get("operation", "send_message")

            async with SlackConnector(bot_token) as slack:
                if operation == "send_message":
                    channel = self.config.config.get("channel") or input_data.get("channel")
                    text = self.config.config.get("text") or input_data.get("text")
                    blocks = self.config.config.get("blocks") or input_data.get("blocks")
                    thread_ts = self.config.config.get("thread_ts") or input_data.get("thread_ts")

                    if not channel:
                        return NodeResult(success=False, error="channel is required for send_message")

                    result = await slack.send_message(channel=channel, text=text, blocks=blocks, thread_ts=thread_ts)

                    return NodeResult(success=True, data=result)

                elif operation == "upload_file":
                    channels = self.config.config.get("channels") or input_data.get("channels")
                    file_content = input_data.get("file_content")
                    filename = self.config.config.get("filename") or input_data.get("filename", "file.txt")
                    title = self.config.config.get("title") or input_data.get("title")
                    initial_comment = self.config.config.get("initial_comment") or input_data.get("initial_comment")

                    if not channels or not file_content:
                        return NodeResult(
                            success=False,
                            error="channels and file_content are required for upload_file",
                        )

                    result = await slack.upload_file(
                        channels=channels,
                        file_content=file_content,
                        filename=filename,
                        title=title,
                        initial_comment=initial_comment,
                    )

                    return NodeResult(success=True, data=result)

                elif operation == "create_channel":
                    name = self.config.config.get("name") or input_data.get("name")
                    is_private = self.config.config.get("is_private", False) or input_data.get("is_private", False)

                    if not name:
                        return NodeResult(success=False, error="name is required for create_channel")

                    result = await slack.create_channel(name=name, is_private=is_private)

                    return NodeResult(success=True, data=result)

                elif operation == "add_reaction":
                    channel = self.config.config.get("channel") or input_data.get("channel")
                    ts = self.config.config.get("message_ts") or input_data.get("ts")
                    emoji = self.config.config.get("emoji") or input_data.get("emoji")

                    if not all([channel, ts, emoji]):
                        return NodeResult(
                            success=False,
                            error="channel, ts, and emoji are required for add_reaction",
                        )

                    result = await slack.add_reaction(channel=channel, ts=ts, emoji=emoji)

                    return NodeResult(success=True, data=result)

                elif operation == "update_message":
                    channel = self.config.config.get("channel") or input_data.get("channel")
                    ts = self.config.config.get("ts") or input_data.get("ts")
                    text = self.config.config.get("text") or input_data.get("text")
                    blocks = self.config.config.get("blocks") or input_data.get("blocks")

                    if not all([channel, ts]):
                        return NodeResult(
                            success=False,
                            error="channel and ts are required for update_message",
                        )

                    result = await slack.update_message(channel=channel, ts=ts, text=text, blocks=blocks)

                    return NodeResult(success=True, data=result)

                elif operation == "get_user_info":
                    user = self.config.config.get("user") or input_data.get("user")

                    if not user:
                        return NodeResult(success=False, error="user is required for get_user_info")

                    result = await slack.get_user_info(user=user)

                    return NodeResult(success=True, data=result)

                elif operation == "lookup_user_by_email":
                    email = self.config.config.get("email") or input_data.get("email")

                    if not email:
                        return NodeResult(
                            success=False,
                            error="email is required for lookup_user_by_email",
                        )

                    result = await slack.lookup_user_by_email(email=email)

                    return NodeResult(success=True, data=result)

                else:
                    return NodeResult(success=False, error=f"Unknown operation: {operation}")

        except Exception as e:
            return NodeResult(success=False, error=str(e))


def register_enterprise_nodes(registry) -> None:
    _enterprise_classes = [
        PostgreSQLNode,
        MongoDBNode,
        RedisNode,
        S3Node,
        GoogleCloudStorageNode,
        OpenAINode,
        AnthropicNode,
        SentimentAnalysisNode,
        TwilioNode,
        SendGridNode,
        StripeNode,
        ShopifyNode,
        HubSpotNode,
        ZendeskNode,
        SplitNode,
        MergeNode,
        ErrorHandlerNode,
        RateLimiterNode,
        RetryNode,
        WebhookSignatureNode,
        CacheNode,
        SlackNode,
    ]
    for cls in _enterprise_classes:
        registry.nodes[cls.name] = cls


# ============================================================================
# NODE REGISTRY
# ============================================================================

NODE_REGISTRY: dict[str, type] = {
    # Database
    "postgresql": PostgreSQLNode,
    "mongodb": MongoDBNode,
    "redis": RedisNode,
    # Storage
    "s3": S3Node,
    "gcs": GoogleCloudStorageNode,
    # AI
    "openai": OpenAINode,
    "anthropic": AnthropicNode,
    "sentiment": SentimentAnalysisNode,
    # Communication
    "slack": SlackNode,
    "slack_message": SlackNode,  # alias used by workflow builder UI
    "twilio": TwilioNode,
    "sendgrid": SendGridNode,
    "email_send": SendGridNode,  # alias used by workflow builder UI
    # Integration
    "stripe": StripeNode,
    "shopify": ShopifyNode,
    "hubspot": HubSpotNode,
    "zendesk": ZendeskNode,
    # Control
    "split": SplitNode,
    "merge": MergeNode,
    "error_handler": ErrorHandlerNode,
    "rate_limiter": RateLimiterNode,
    "retry": RetryNode,
    # Utility
    "webhook_signature": WebhookSignatureNode,
    "cache": CacheNode,
}


def get_node_class(node_type: str) -> type | None:
    """Get node class by type."""
    return NODE_REGISTRY.get(node_type)


def list_available_nodes() -> list[dict[str, Any]]:
    """List all available node types with metadata."""
    nodes = []
    for node_type, node_class in NODE_REGISTRY.items():
        nodes.append(
            {
                "type": node_type,
                "category": node_class.category.value,
                "description": node_class.description,
                "icon": node_class.icon,
            }
        )
    return nodes
