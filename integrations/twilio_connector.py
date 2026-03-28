"""
Twilio Integration Connector for Helix Spirals.

Provides comprehensive Twilio API integration for SMS, Voice, WhatsApp,
phone number management, and communication automation.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class TwilioMessageType(Enum):
    """Twilio message types."""

    SMS = "sms"
    WHATSAPP = "whatsapp"
    MMS = "mms"


class TwilioCallStatus(Enum):
    """Twilio call status."""

    QUEUED = "queued"
    RINGING = "ringing"
    IN_PROGRESS = "in-progress"
    COMPLETED = "completed"
    FAILED = "failed"
    BUSY = "busy"
    NO_ANSWER = "no-answer"
    CANCELLED = "cancelled"


class TwilioDirection(Enum):
    """Message/call direction."""

    INBOUND = "inbound"
    OUTBOUND = "outbound"
    OUTBOUND_REPLY = "outbound-reply"
    OUTBOUND_API = "outbound-api"


@dataclass
class TwilioConfig:
    """Configuration for Twilio connector."""

    account_sid: str
    auth_token: str
    timeout: int = 30
    max_retries: int = 3

    @property
    def base_url(self) -> str:
        return f"https://api.twilio.com/2010-04-01/Accounts/{self.account_sid}"


@dataclass
class TwilioMessage:
    """Represents a Twilio message."""

    sid: str
    from_number: str
    to_number: str
    body: str
    direction: TwilioDirection
    status: str
    message_type: TwilioMessageType
    media_urls: list[str] = field(default_factory=list)
    date_created: datetime | None = None
    date_sent: datetime | None = None
    date_updated: datetime | None = None
    price: float | None = None
    price_unit: str | None = None
    segments: int = 0
    error_code: str | None = None
    error_message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TwilioCall:
    """Represents a Twilio call."""

    sid: str
    from_number: str
    to_number: str
    status: TwilioCallStatus
    direction: TwilioDirection
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration: int | None = None
    price: float | None = None
    price_unit: str | None = None
    recording_url: str | None = None
    transcription_url: str | None = None
    date_created: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TwilioPhoneNumber:
    """Represents a Twilio phone number."""

    phone_number: str
    friendly_name: str
    voice_url: str | None = None
    sms_url: str | None = None
    capabilities: dict[str, bool] = field(default_factory=dict)
    date_created: datetime | None = None


class TwilioConnector:
    """
    Comprehensive Twilio API connector for Helix Spirals.

    Provides methods for:
    - SMS sending/receiving
    - Voice calls
    - WhatsApp messaging
    - Phone number management
    - Recording and transcription
    - Media handling
    """

    def __init__(self, config: TwilioConfig):
        self.config = config
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            auth = aiohttp.BasicAuth(self.config.account_sid, self.config.auth_token)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                auth=auth,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, method: str, endpoint: str, data: dict | None = None) -> dict[str, Any]:
        """Make an API request."""
        session = await self._get_session()
        url = f"{self.config.base_url}/{endpoint}"

        # Convert dict to form data
        form_data = None
        if data:
            form_data = {k: str(v) if v is not None else "" for k, v in data.items()}

        async with session.request(method, url, data=form_data) as response:
            if response.status not in [200, 201]:
                error_text = await response.text()
                raise ValueError(f"Twilio API error: {response.status} - {error_text}")

            return await response.json()

    # ==================== SMS Operations ====================

    async def send_sms(
        self,
        to_number: str,
        body: str,
        from_number: str | None = None,
        status_callback: str | None = None,
        media_urls: list[str] | None = None,
    ) -> TwilioMessage:
        """Send an SMS message."""
        data = {
            "To": to_number,
            "Body": body,
            "From": from_number or "",
        }

        if status_callback:
            data["StatusCallback"] = status_callback

        if media_urls:
            for i, url in enumerate(media_urls):
                data[f"MediaUrl[{i}]"] = url

        response = await self._request("POST", "Messages.json", data)
        return self._parse_message(response, TwilioMessageType.SMS)

    async def send_bulk_sms(
        self, messages: list[dict[str, str]], from_number: str | None = None
    ) -> list[TwilioMessage]:
        """Send multiple SMS messages in parallel."""
        tasks = [self.send_sms(to_number=msg["to"], body=msg["body"], from_number=from_number) for msg in messages]
        return await asyncio.gather(*tasks)

    async def get_message(self, message_sid: str) -> TwilioMessage:
        """Get a message by SID."""
        response = await self._request("GET", f"Messages/{message_sid}.json")
        return self._parse_message(response)

    async def list_messages(
        self,
        to_number: str | None = None,
        from_number: str | None = None,
        date_after: datetime | None = None,
        date_before: datetime | None = None,
        limit: int = 50,
    ) -> list[TwilioMessage]:
        """List messages with filtering."""
        params = {"PageSize": str(min(limit, 1000))}

        if to_number:
            params["To"] = to_number
        if from_number:
            params["From"] = from_number
        if date_after:
            params["DateSent>"] = date_after.isoformat()
        if date_before:
            params["DateSent<"] = date_before.isoformat()

        response = await self._request("GET", "Messages.json", params)

        messages = []
        for item in response.get("messages", []):
            messages.append(self._parse_message(item))

        return messages

    # ==================== Voice Operations ====================

    async def make_call(
        self,
        to_number: str,
        from_number: str,
        url: str,
        method: str = "POST",
        status_callback: str | None = None,
        recording_url: str | None = None,
        transcription_url: str | None = None,
        machine_detection: str | None = None,
        timeout: int = 30,
        caller_id: str | None = None,
    ) -> TwilioCall:
        """Make a voice call."""
        data = {
            "To": to_number,
            "From": from_number,
            "Url": url,
            "Method": method,
            "Timeout": str(timeout),
        }

        if status_callback:
            data["StatusCallback"] = status_callback

        if recording_url:
            data["RecordingStatusCallback"] = recording_url

        if transcription_url:
            data["TranscriptionStatusCallback"] = transcription_url

        if machine_detection:
            data["MachineDetection"] = machine_detection

        if caller_id:
            data["CallerId"] = caller_id

        response = await self._request("POST", "Calls.json", data)
        return self._parse_call(response)

    async def get_call(self, call_sid: str) -> TwilioCall:
        """Get a call by SID."""
        response = await self._request("GET", f"Calls/{call_sid}.json")
        return self._parse_call(response)

    async def list_calls(
        self,
        to_number: str | None = None,
        from_number: str | None = None,
        status: str | None = None,
        date_after: datetime | None = None,
        limit: int = 50,
    ) -> list[TwilioCall]:
        """List calls with filtering."""
        params = {"PageSize": str(min(limit, 1000))}

        if to_number:
            params["To"] = to_number
        if from_number:
            params["From"] = from_number
        if status:
            params["Status"] = status
        if date_after:
            params["StartTime>"] = date_after.isoformat()

        response = await self._request("GET", "Calls.json", params)

        calls = []
        for item in response.get("calls", []):
            calls.append(self._parse_call(item))

        return calls

    async def modify_call(
        self,
        call_sid: str,
        url: str | None = None,
        method: str | None = None,
        status: str | None = None,
    ) -> TwilioCall:
        """Modify an in-progress call."""
        data = {}

        if url:
            data["Url"] = url
        if method:
            data["Method"] = method
        if status:
            data["Status"] = status

        response = await self._request("POST", f"Calls/{call_sid}.json", data)
        return self._parse_call(response)

    async def end_call(self, call_sid: str) -> bool:
        """End an in-progress call."""
        await self.modify_call(call_sid, status="completed")
        return True

    # ==================== WhatsApp Operations ====================

    async def send_whatsapp(
        self,
        to_number: str,
        body: str,
        from_number: str | None = None,
        media_url: str | None = None,
        status_callback: str | None = None,
    ) -> TwilioMessage:
        """Send a WhatsApp message."""
        data = {
            "To": f"whatsapp:{to_number}",
            "Body": body,
            "From": f"whatsapp:{from_number}" if from_number else "",
        }

        if media_url:
            data["MediaUrl"] = media_url

        if status_callback:
            data["StatusCallback"] = status_callback

        response = await self._request("POST", "Messages.json", data)
        return self._parse_message(response, TwilioMessageType.WHATSAPP)

    async def send_whatsapp_template(
        self,
        to_number: str,
        template_name: str,
        from_number: str,
        template_params: dict[str, Any] | None = None,
    ) -> TwilioMessage:
        """Send a WhatsApp template message."""
        content_sid = await self._get_template_content_sid(template_name)

        data = {
            "To": f"whatsapp:{to_number}",
            "From": f"whatsapp:{from_number}",
            "ContentSid": content_sid,
        }

        if template_params:
            data["ContentVariables"] = str(template_params)

        response = await self._request("POST", "Messages.json", data)
        return self._parse_message(response, TwilioMessageType.WHATSAPP)

    async def _get_template_content_sid(self, template_name: str) -> str:
        """Get content SID for a WhatsApp template by querying the Content API."""
        try:
            session = await self._get_session()
            url = "https://content.twilio.com/v1/Content"
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    for content in data.get("contents", []):
                        if content.get("friendly_name") == template_name:
                            return content["sid"]
                    raise ValueError(f"WhatsApp template '{template_name}' not found in Twilio Content API")
                else:
                    error_text = await response.text()
                    raise ValueError(f"Twilio Content API error: {response.status} - {error_text}")
        except aiohttp.ClientError as e:
            logger.error("Failed to query Twilio Content API: %s", e)
            raise ValueError(f"Could not resolve template '{template_name}': {e}")

    # ==================== Phone Number Management ====================

    async def get_phone_numbers(
        self,
        phone_number: str | None = None,
        friendly_name: str | None = None,
        limit: int = 50,
    ) -> list[TwilioPhoneNumber]:
        """Get phone numbers in your account."""
        params = {"PageSize": str(min(limit, 1000))}

        if phone_number:
            params["PhoneNumber"] = phone_number
        if friendly_name:
            params["FriendlyName"] = friendly_name

        response = await self._request("GET", "IncomingPhoneNumbers.json", params)

        numbers = []
        for item in response.get("incoming_phone_numbers", []):
            numbers.append(
                TwilioPhoneNumber(
                    phone_number=item["phone_number"],
                    friendly_name=item["friendly_name"],
                    voice_url=item.get("voice_url"),
                    sms_url=item.get("sms_url"),
                    capabilities={
                        "voice": item.get("capabilities", {}).get("voice", False),
                        "sms": item.get("capabilities", {}).get("sms", False),
                        "mms": item.get("capabilities", {}).get("mms", False),
                    },
                    date_created=self._parse_datetime(item.get("date_created")),
                )
            )

        return numbers

    async def buy_phone_number(
        self,
        area_code: str | None = None,
        phone_number: str | None = None,
        friendly_name: str | None = None,
    ) -> TwilioPhoneNumber:
        """Buy a new phone number."""
        data = {}

        if area_code:
            data["AreaCode"] = area_code
        elif phone_number:
            data["PhoneNumber"] = phone_number

        if friendly_name:
            data["FriendlyName"] = friendly_name

        response = await self._request("POST", "IncomingPhoneNumbers.json", data)

        return TwilioPhoneNumber(
            phone_number=response["phone_number"],
            friendly_name=response["friendly_name"],
            voice_url=response.get("voice_url"),
            sms_url=response.get("sms_url"),
            capabilities={
                "voice": response.get("capabilities", {}).get("voice", False),
                "sms": response.get("capabilities", {}).get("sms", False),
                "mms": response.get("capabilities", {}).get("mms", False),
            },
            date_created=self._parse_datetime(response.get("date_created")),
        )

    async def release_phone_number(self, phone_number: str) -> bool:
        """Release a phone number from your account."""
        await self._request("DELETE", f"IncomingPhoneNumbers/{phone_number}.json")
        return True

    # ==================== Recording Operations ====================

    async def get_recordings(
        self,
        call_sid: str | None = None,
        date_after: datetime | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Get recordings."""
        params = {"PageSize": str(min(limit, 1000))}

        if call_sid:
            params["CallSid"] = call_sid
        if date_after:
            params["DateCreated>"] = date_after.isoformat()

        response = await self._request("GET", "Recordings.json", params)
        return response.get("recordings", [])

    async def get_recording(self, recording_sid: str) -> dict[str, Any]:
        """Get a recording."""
        return await self._request("GET", f"Recordings/{recording_sid}.json")

    async def delete_recording(self, recording_sid: str) -> bool:
        """Delete a recording."""
        await self._request("DELETE", f"Recordings/{recording_sid}.json")
        return True

    async def get_transcription(self, transcription_sid: str) -> dict[str, Any]:
        """Get a transcription."""
        return await self._request("GET", f"Transcriptions/{transcription_sid}.json")

    # ==================== Helper Methods ====================

    def _parse_message(
        self, data: dict[str, Any], msg_type: TwilioMessageType = TwilioMessageType.SMS
    ) -> TwilioMessage:
        """Parse message data from API response."""
        # Determine message type
        if "whatsapp" in data.get("from", "").lower():
            msg_type = TwilioMessageType.WHATSAPP
        elif data.get("num_media", 0) > 0:
            msg_type = TwilioMessageType.MMS

        return TwilioMessage(
            sid=data["sid"],
            from_number=data["from"],
            to_number=data["to"],
            body=data.get("body", ""),
            direction=TwilioDirection(data["direction"]),
            status=data["status"],
            message_type=msg_type,
            date_created=self._parse_datetime(data.get("date_created")),
            date_sent=self._parse_datetime(data.get("date_sent")),
            date_updated=self._parse_datetime(data.get("date_updated")),
            price=float(data["price"]) if data.get("price") else None,
            price_unit=data.get("price_unit"),
            segments=int(data["num_segments"] or 0),
            error_code=data.get("error_code"),
            error_message=data.get("error_message"),
            metadata={
                k: v
                for k, v in data.items()
                if k
                not in [
                    "sid",
                    "from",
                    "to",
                    "body",
                    "direction",
                    "status",
                    "date_created",
                    "date_sent",
                    "date_updated",
                    "price",
                    "price_unit",
                    "num_segments",
                    "error_code",
                    "error_message",
                ]
            },
        )

    def _parse_call(self, data: dict[str, Any]) -> TwilioCall:
        """Parse call data from API response."""
        return TwilioCall(
            sid=data["sid"],
            from_number=data["from"],
            to_number=data["to"],
            status=TwilioCallStatus(data["status"]),
            direction=TwilioDirection(data["direction"]),
            start_time=self._parse_datetime(data.get("start_time")),
            end_time=self._parse_datetime(data.get("end_time")),
            duration=int(data["duration"]) if data.get("duration") else None,
            price=float(data["price"]) if data.get("price") else None,
            price_unit=data.get("price_unit"),
            date_created=self._parse_datetime(data.get("date_created")),
            metadata={
                k: v
                for k, v in data.items()
                if k
                not in [
                    "sid",
                    "from",
                    "to",
                    "status",
                    "direction",
                    "start_time",
                    "end_time",
                    "duration",
                    "price",
                    "price_unit",
                    "date_created",
                ]
            },
        )

    def _parse_datetime(self, dt_str: str | None) -> datetime | None:
        """Parse ISO datetime string."""
        if not dt_str:
            return None
        # Twilio uses RFC 2822 format

        try:
            return datetime.strptime(dt_str, "%a, %d %b %Y %H:%M:%S %z")
        except ValueError:
            try:
                return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            except ValueError:
                return None


# ==================== Helix Spirals Node Integration ====================


class TwilioNode:
    """
    Helix Spirals node for Twilio integration.

    Supports operations:
    - send_sms: Send SMS message
    - make_call: Make voice call
    - send_whatsapp: Send WhatsApp message
    - list_messages: List messages
    - list_calls: List calls
    - buy_number: Buy phone number
    - release_number: Release phone number
    """

    def __init__(self, config: TwilioConfig):
        self.connector = TwilioConnector(config)

    async def execute(self, operation: str, params: dict[str, Any]) -> dict[str, Any]:
        """Execute a Twilio operation."""
        operations = {
            "send_sms": self._send_sms,
            "send_bulk_sms": self._send_bulk_sms,
            "make_call": self._make_call,
            "end_call": self._end_call,
            "send_whatsapp": self._send_whatsapp,
            "send_whatsapp_template": self._send_whatsapp_template,
            "list_messages": self._list_messages,
            "list_calls": self._list_calls,
            "buy_number": self._buy_number,
            "release_number": self._release_number,
            "get_messages": self._get_messages,
        }

        if operation not in operations:
            raise ValueError(f"Unknown operation: {operation}")

        return await operations[operation](params)

    async def _send_sms(self, params: dict[str, Any]) -> dict[str, Any]:
        """Send SMS message."""
        message = await self.connector.send_sms(
            to_number=params["to"],
            body=params["body"],
            from_number=params.get("from_number"),
            media_urls=params.get("media_urls"),
        )

        return {
            "success": True,
            "message_sid": message.sid,
            "status": message.status,
            "direction": message.direction.value,
        }

    async def _send_bulk_sms(self, params: dict[str, Any]) -> dict[str, Any]:
        """Send bulk SMS messages."""
        messages = await self.connector.send_bulk_sms(
            messages=params["messages"], from_number=params.get("from_number")
        )

        return {
            "success": True,
            "sent_count": len(messages),
            "message_sids": [m.sid for m in messages],
        }

    async def _make_call(self, params: dict[str, Any]) -> dict[str, Any]:
        """Make voice call."""
        call = await self.connector.make_call(
            to_number=params["to"],
            from_number=params["from"],
            url=params["url"],
            method=params.get("method", "POST"),
            timeout=params.get("timeout", 30),
        )

        return {"success": True, "call_sid": call.sid, "status": call.status.value}

    async def _end_call(self, params: dict[str, Any]) -> dict[str, Any]:
        """End call."""
        await self.connector.end_call(params["call_sid"])
        return {"success": True, "call_sid": params["call_sid"]}

    async def _send_whatsapp(self, params: dict[str, Any]) -> dict[str, Any]:
        """Send WhatsApp message."""
        message = await self.connector.send_whatsapp(
            to_number=params["to"],
            body=params["body"],
            from_number=params.get("from_number"),
            media_url=params.get("media_url"),
        )

        return {"success": True, "message_sid": message.sid, "status": message.status}

    async def _send_whatsapp_template(self, params: dict[str, Any]) -> dict[str, Any]:
        """Send WhatsApp template."""
        message = await self.connector.send_whatsapp_template(
            to_number=params["to"],
            template_name=params["template_name"],
            from_number=params["from"],
            template_params=params.get("params"),
        )

        return {"success": True, "message_sid": message.sid, "status": message.status}

    async def _list_messages(self, params: dict[str, Any]) -> dict[str, Any]:
        """List messages."""
        messages = await self.connector.list_messages(
            to_number=params.get("to_number"),
            from_number=params.get("from_number"),
            limit=params.get("limit", 50),
        )

        return {
            "success": True,
            "messages": [
                {
                    "sid": m.sid,
                    "from": m.from_number,
                    "to": m.to_number,
                    "body": m.body,
                    "status": m.status,
                    "direction": m.direction.value,
                    "created_at": (m.date_created.isoformat() if m.date_created else None),
                }
                for m in messages
            ],
        }

    async def _list_calls(self, params: dict[str, Any]) -> dict[str, Any]:
        """List calls."""
        calls = await self.connector.list_calls(
            to_number=params.get("to_number"),
            from_number=params.get("from_number"),
            status=params.get("status"),
            limit=params.get("limit", 50),
        )

        return {
            "success": True,
            "calls": [
                {
                    "sid": c.sid,
                    "from": c.from_number,
                    "to": c.to_number,
                    "status": c.status.value,
                    "direction": c.direction.value,
                    "duration": c.duration,
                    "start_time": c.start_time.isoformat() if c.start_time else None,
                }
                for c in calls
            ],
        }

    async def _buy_number(self, params: dict[str, Any]) -> dict[str, Any]:
        """Buy phone number."""
        number = await self.connector.buy_phone_number(
            area_code=params.get("area_code"),
            phone_number=params.get("phone_number"),
            friendly_name=params.get("friendly_name"),
        )

        return {
            "success": True,
            "phone_number": number.phone_number,
            "friendly_name": number.friendly_name,
        }

    async def _release_number(self, params: dict[str, Any]) -> dict[str, Any]:
        """Release phone number."""
        await self.connector.release_phone_number(params["phone_number"])
        return {"success": True, "phone_number": params["phone_number"]}

    async def _get_messages(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get message details."""
        message = await self.connector.get_message(params["message_sid"])

        return {
            "success": True,
            "message": {
                "sid": message.sid,
                "from": message.from_number,
                "to": message.to_number,
                "body": message.body,
                "status": message.status,
                "created_at": (message.date_created.isoformat() if message.date_created else None),
            },
        }

    async def close(self):
        """Close the connector."""
        await self.connector.close()
