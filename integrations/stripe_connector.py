"""
Stripe Integration Connector for Helix Spirals.

Provides comprehensive Stripe API integration for payment processing,
subscription management, billing, and customer management.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class StripeChargeStatus(Enum):
    """Stripe charge status."""

    SUCCEEDED = "succeeded"
    PENDING = "pending"
    FAILED = "failed"


class StripeInvoiceStatus(Enum):
    """Stripe invoice status."""

    DRAFT = "draft"
    OPEN = "open"
    PAID = "paid"
    UNCOLLECTIBLE = "uncollectible"
    VOID = "void"


class StripeSubscriptionStatus(Enum):
    """Stripe subscription status."""

    TRIALING = "trialing"
    ACTIVE = "active"
    PAST_DUE = "past_due"
    CANCELED = "canceled"
    UNPAID = "unpaid"


@dataclass
class StripeConfig:
    """Configuration for Stripe connector."""

    api_key: str
    webhook_secret: str | None = None
    timeout: int = 30
    max_retries: int = 3

    @property
    def base_url(self) -> str:
        return "https://api.stripe.com/v1"


@dataclass
class StripeCharge:
    """Represents a Stripe charge."""

    id: str
    amount: int  # In cents
    currency: str
    status: StripeChargeStatus
    customer_id: str | None
    description: str | None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    receipt_url: str | None = None
    payment_method_id: str | None = None
    failure_code: str | None = None
    failure_message: str | None = None


@dataclass
class StripeCustomer:
    """Represents a Stripe customer."""

    id: str
    email: str | None
    name: str | None
    description: str | None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    balance: int = 0
    currency: str | None = None
    default_source: str | None = None


@dataclass
class StripeSubscription:
    """Represents a Stripe subscription."""

    id: str
    customer_id: str
    status: StripeSubscriptionStatus
    current_period_start: datetime
    current_period_end: datetime
    cancel_at_period_end: bool
    created_at: datetime | None = None
    price_id: str | None = None
    quantity: int = 1
    trial_start: datetime | None = None
    trial_end: datetime | None = None


@dataclass
class StripePaymentIntent:
    """Represents a Stripe payment intent."""

    id: str
    amount: int
    currency: str
    status: str
    client_secret: str | None = None
    customer_id: str | None = None
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    payment_method_id: str | None = None


class StripeConnector:
    """
    Comprehensive Stripe API connector for Helix Spirals.

    Provides methods for:
    - Payment processing (charges, payment intents)
    - Customer management
    - Subscription management
    - Invoice management
    - Payment methods
    - Refunds
    - Webhooks
    """

    def __init__(self, config: StripeConfig):
        self.config = config
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(
        self,
        method: str,
        endpoint: str,
        data: dict | None = None,
        params: dict | None = None,
    ) -> dict[str, Any]:
        """Make an API request."""
        session = await self._get_session()
        url = f"{self.config.base_url}/{endpoint}"

        # Convert dict to form data
        form_data = None
        if data:
            form_data = {}
            for k, v in data.items():
                if v is not None:
                    if isinstance(v, bool):
                        form_data[k] = "true" if v else "false"
                    elif isinstance(v, (dict, list)):
                        import json

                        form_data[k] = json.dumps(v)
                    else:
                        form_data[k] = str(v)

        async with session.request(method, url, data=form_data, params=params) as response:
            if response.status not in [200, 201]:
                error_text = await response.text()
                try:
                    error_json = json.loads(error_text)
                    error_message = error_json.get("error", {}).get("message", error_text)
                except (json.JSONDecodeError, ValueError, TypeError, KeyError, AttributeError):
                    error_message = error_text
                raise ValueError(f"Stripe API error: {response.status} - {error_message}")

            return await response.json()

    # ==================== Payment Intents ====================

    async def create_payment_intent(
        self,
        amount: int,
        currency: str,
        customer_id: str | None = None,
        description: str | None = None,
        payment_method_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        confirm: bool = False,
        off_session: bool = False,
    ) -> StripePaymentIntent:
        """Create a payment intent."""
        data = {"amount": amount, "currency": currency}

        if customer_id:
            data["customer"] = customer_id
        if description:
            data["description"] = description
        if payment_method_id:
            data["payment_method"] = payment_method_id
        if metadata:
            data["metadata"] = metadata
        if confirm:
            data["confirm"] = confirm
        if off_session:
            data["off_session"] = off_session

        response = await self._request("POST", "payment_intents", data)

        return StripePaymentIntent(
            id=response["id"],
            amount=response["amount"],
            currency=response["currency"],
            status=response["status"],
            client_secret=response.get("client_secret"),
            customer_id=response.get("customer"),
            description=response.get("description"),
            metadata=response.get("metadata", {}),
            created_at=datetime.fromtimestamp(response["created"]),
            payment_method_id=response.get("payment_method"),
        )

    async def get_payment_intent(self, payment_intent_id: str) -> StripePaymentIntent:
        """Get a payment intent."""
        response = await self._request("GET", f"payment_intents/{payment_intent_id}")

        return StripePaymentIntent(
            id=response["id"],
            amount=response["amount"],
            currency=response["currency"],
            status=response["status"],
            client_secret=response.get("client_secret"),
            customer_id=response.get("customer"),
            description=response.get("description"),
            metadata=response.get("metadata", {}),
            created_at=datetime.fromtimestamp(response["created"]),
            payment_method_id=response.get("payment_method"),
        )

    async def confirm_payment_intent(
        self, payment_intent_id: str, payment_method_id: str | None = None
    ) -> StripePaymentIntent:
        """Confirm a payment intent."""
        data = {}
        if payment_method_id:
            data["payment_method"] = payment_method_id

        response = await self._request("POST", f"payment_intents/{payment_intent_id}/confirm", data)

        return StripePaymentIntent(
            id=response["id"],
            amount=response["amount"],
            currency=response["currency"],
            status=response["status"],
            client_secret=response.get("client_secret"),
            customer_id=response.get("customer"),
            description=response.get("description"),
            metadata=response.get("metadata", {}),
            created_at=datetime.fromtimestamp(response["created"]),
            payment_method_id=response.get("payment_method"),
        )

    async def cancel_payment_intent(self, payment_intent_id: str) -> StripePaymentIntent:
        """Cancel a payment intent."""
        response = await self._request("POST", f"payment_intents/{payment_intent_id}/cancel")

        return StripePaymentIntent(
            id=response["id"],
            amount=response["amount"],
            currency=response["currency"],
            status=response["status"],
            client_secret=response.get("client_secret"),
            customer_id=response.get("customer"),
            description=response.get("description"),
            metadata=response.get("metadata", {}),
            created_at=datetime.fromtimestamp(response["created"]),
        )

    # ==================== Charges ====================

    async def create_charge(
        self,
        amount: int,
        currency: str,
        source: str | None = None,
        customer_id: str | None = None,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StripeCharge:
        """Create a charge (legacy API)."""
        data = {"amount": amount, "currency": currency}

        if source:
            data["source"] = source
        elif customer_id:
            data["customer"] = customer_id
        else:
            raise ValueError("Either source or customer_id must be provided")

        if description:
            data["description"] = description
        if metadata:
            data["metadata"] = metadata

        response = await self._request("POST", "charges", data)

        return StripeCharge(
            id=response["id"],
            amount=response["amount"],
            currency=response["currency"],
            status=StripeChargeStatus(response["status"]),
            customer_id=response.get("customer"),
            description=response.get("description"),
            metadata=response.get("metadata", {}),
            created_at=datetime.fromtimestamp(response["created"]),
            receipt_url=response.get("receipt_url"),
            payment_method_id=response.get("payment_method"),
            failure_code=response.get("failure_code"),
            failure_message=response.get("failure_message"),
        )

    async def get_charge(self, charge_id: str) -> StripeCharge:
        """Get a charge."""
        response = await self._request("GET", f"charges/{charge_id}")

        return StripeCharge(
            id=response["id"],
            amount=response["amount"],
            currency=response["currency"],
            status=StripeChargeStatus(response["status"]),
            customer_id=response.get("customer"),
            description=response.get("description"),
            metadata=response.get("metadata", {}),
            created_at=datetime.fromtimestamp(response["created"]),
            receipt_url=response.get("receipt_url"),
            payment_method_id=response.get("payment_method"),
            failure_code=response.get("failure_code"),
            failure_message=response.get("failure_message"),
        )

    # ==================== Customers ====================

    async def create_customer(
        self,
        email: str | None = None,
        name: str | None = None,
        description: str | None = None,
        payment_method_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StripeCustomer:
        """Create a customer."""
        data = {}

        if email:
            data["email"] = email
        if name:
            data["name"] = name
        if description:
            data["description"] = description
        if metadata:
            data["metadata"] = metadata

        response = await self._request("POST", "customers", data)

        # Set default payment method if provided
        if payment_method_id:
            await self.attach_payment_method_to_customer(payment_method_id, response["id"])
            await self.set_default_payment_method(response["id"], payment_method_id)

        return StripeCustomer(
            id=response["id"],
            email=response.get("email"),
            name=response.get("name"),
            description=response.get("description"),
            metadata=response.get("metadata", {}),
            created_at=datetime.fromtimestamp(response["created"]),
            balance=response.get("balance", 0),
            currency=response.get("currency"),
            default_source=response.get("default_source"),
        )

    async def get_customer(self, customer_id: str) -> StripeCustomer:
        """Get a customer."""
        response = await self._request("GET", f"customers/{customer_id}")

        return StripeCustomer(
            id=response["id"],
            email=response.get("email"),
            name=response.get("name"),
            description=response.get("description"),
            metadata=response.get("metadata", {}),
            created_at=datetime.fromtimestamp(response["created"]),
            balance=response.get("balance", 0),
            currency=response.get("currency"),
            default_source=response.get("default_source"),
        )

    async def update_customer(
        self,
        customer_id: str,
        email: str | None = None,
        name: str | None = None,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StripeCustomer:
        """Update a customer."""
        data = {}

        if email:
            data["email"] = email
        if name:
            data["name"] = name
        if description:
            data["description"] = description
        if metadata:
            data["metadata"] = metadata

        response = await self._request("POST", f"customers/{customer_id}", data)

        return StripeCustomer(
            id=response["id"],
            email=response.get("email"),
            name=response.get("name"),
            description=response.get("description"),
            metadata=response.get("metadata", {}),
            created_at=datetime.fromtimestamp(response["created"]),
            balance=response.get("balance", 0),
            currency=response.get("currency"),
            default_source=response.get("default_source"),
        )

    async def delete_customer(self, customer_id: str) -> bool:
        """Delete a customer."""
        await self._request("DELETE", f"customers/{customer_id}")
        return True

    # ==================== Payment Methods ====================

    async def attach_payment_method_to_customer(self, payment_method_id: str, customer_id: str) -> dict[str, Any]:
        """Attach a payment method to a customer."""
        data = {"customer": customer_id}
        return await self._request("POST", f"payment_methods/{payment_method_id}/attach", data)

    async def set_default_payment_method(self, customer_id: str, payment_method_id: str) -> dict[str, Any]:
        """Set default payment method for a customer."""
        data = {"invoice_settings": {"default_payment_method": payment_method_id}}
        return await self._request("POST", f"customers/{customer_id}", data)

    async def get_payment_methods(self, customer_id: str, type: str = "card") -> list[dict[str, Any]]:
        """Get payment methods for a customer."""
        params = {"customer": customer_id, "type": type}
        response = await self._request("GET", "payment_methods", params=params)
        return response.get("data", [])

    # ==================== Subscriptions ====================

    async def create_subscription(
        self,
        customer_id: str,
        price_id: str,
        quantity: int = 1,
        trial_period_days: int | None = None,
        metadata: dict[str, Any] | None = None,
        payment_behavior: str = "default_incomplete",
    ) -> StripeSubscription:
        """Create a subscription."""
        data = {
            "customer": customer_id,
            "items": [{"price": price_id, "quantity": quantity}],
            "payment_behavior": payment_behavior,
        }

        if trial_period_days:
            data["trial_period_days"] = trial_period_days
        if metadata:
            data["metadata"] = metadata

        response = await self._request("POST", "subscriptions", data)

        return StripeSubscription(
            id=response["id"],
            customer_id=response["customer"],
            status=StripeSubscriptionStatus(response["status"]),
            current_period_start=datetime.fromtimestamp(response["current_period_start"]),
            current_period_end=datetime.fromtimestamp(response["current_period_end"]),
            cancel_at_period_end=response.get("cancel_at_period_end", False),
            created_at=datetime.fromtimestamp(response["created"]),
            price_id=(response["items"]["data"][0]["price"]["id"] if response.get("items") else None),
            quantity=(response["items"]["data"][0]["quantity"] if response.get("items") else 1),
            trial_start=(datetime.fromtimestamp(response["trial_start"]) if response.get("trial_start") else None),
            trial_end=(datetime.fromtimestamp(response["trial_end"]) if response.get("trial_end") else None),
        )

    async def get_subscription(self, subscription_id: str) -> StripeSubscription:
        """Get a subscription."""
        response = await self._request("GET", f"subscriptions/{subscription_id}")

        return StripeSubscription(
            id=response["id"],
            customer_id=response["customer"],
            status=StripeSubscriptionStatus(response["status"]),
            current_period_start=datetime.fromtimestamp(response["current_period_start"]),
            current_period_end=datetime.fromtimestamp(response["current_period_end"]),
            cancel_at_period_end=response.get("cancel_at_period_end", False),
            created_at=datetime.fromtimestamp(response["created"]),
            price_id=(response["items"]["data"][0]["price"]["id"] if response.get("items") else None),
            quantity=(response["items"]["data"][0]["quantity"] if response.get("items") else 1),
            trial_start=(datetime.fromtimestamp(response["trial_start"]) if response.get("trial_start") else None),
            trial_end=(datetime.fromtimestamp(response["trial_end"]) if response.get("trial_end") else None),
        )

    async def cancel_subscription(self, subscription_id: str, at_period_end: bool = True) -> StripeSubscription:
        """Cancel a subscription."""
        data = {"cancel_at_period_end": at_period_end}
        response = await self._request("DELETE", f"subscriptions/{subscription_id}", data)

        return StripeSubscription(
            id=response["id"],
            customer_id=response["customer"],
            status=StripeSubscriptionStatus(response["status"]),
            current_period_start=datetime.fromtimestamp(response["current_period_start"]),
            current_period_end=datetime.fromtimestamp(response["current_period_end"]),
            cancel_at_period_end=response.get("cancel_at_period_end", False),
            created_at=datetime.fromtimestamp(response["created"]),
        )

    # ==================== Invoices ====================

    async def list_invoices(
        self,
        customer_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List invoices."""
        params = {"limit": str(min(limit, 100))}

        if customer_id:
            params["customer"] = customer_id
        if status:
            params["status"] = status

        response = await self._request("GET", "invoices", params=params)
        return response.get("data", [])

    async def get_invoice(self, invoice_id: str) -> dict[str, Any]:
        """Get an invoice."""
        return await self._request("GET", f"invoices/{invoice_id}")

    # ==================== Refunds ====================

    async def create_refund(
        self,
        charge_id: str,
        amount: int | None = None,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a refund."""
        data = {"charge": charge_id}

        if amount:
            data["amount"] = amount
        if reason:
            data["reason"] = reason
        if metadata:
            data["metadata"] = metadata

        return await self._request("POST", "refunds", data)

    # ==================== Products and Prices ====================

    async def create_product(
        self,
        name: str,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a product."""
        data = {"name": name}

        if description:
            data["description"] = description
        if metadata:
            data["metadata"] = metadata

        return await self._request("POST", "products", data)

    async def create_price(
        self,
        product_id: str,
        unit_amount: int,
        currency: str,
        recurring_interval: str = "month",
        recurring_count: int | None = None,
    ) -> dict[str, Any]:
        """Create a price."""
        data = {
            "product": product_id,
            "unit_amount": unit_amount,
            "currency": currency,
            "recurring": {"interval": recurring_interval},
        }

        if recurring_count:
            data["recurring"]["interval_count"] = recurring_count

        return await self._request("POST", "prices", data)


# ==================== Helix Spirals Node Integration ====================


class StripeNode:
    """
    Helix Spirals node for Stripe integration.

    Supports operations:
    - create_payment_intent: Create payment intent
    - create_charge: Create charge
    - create_customer: Create customer
    - create_subscription: Create subscription
    - get_subscription: Get subscription
    - cancel_subscription: Cancel subscription
    - list_invoices: List invoices
    - create_refund: Create refund
    """

    def __init__(self, config: StripeConfig):
        self.connector = StripeConnector(config)

    async def execute(self, operation: str, params: dict[str, Any]) -> dict[str, Any]:
        """Execute a Stripe operation."""
        operations = {
            "create_payment_intent": self._create_payment_intent,
            "confirm_payment_intent": self._confirm_payment_intent,
            "cancel_payment_intent": self._cancel_payment_intent,
            "create_charge": self._create_charge,
            "create_customer": self._create_customer,
            "get_customer": self._get_customer,
            "create_subscription": self._create_subscription,
            "get_subscription": self._get_subscription,
            "cancel_subscription": self._cancel_subscription,
            "list_invoices": self._list_invoices,
            "create_refund": self._create_refund,
            "create_product": self._create_product,
            "create_price": self._create_price,
        }

        if operation not in operations:
            raise ValueError(f"Unknown operation: {operation}")

        return await operations[operation](params)

    async def _create_payment_intent(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create payment intent."""
        intent = await self.connector.create_payment_intent(
            amount=params["amount"],
            currency=params["currency"],
            customer_id=params.get("customer_id"),
            description=params.get("description"),
            payment_method_id=params.get("payment_method_id"),
            metadata=params.get("metadata"),
        )

        return {
            "success": True,
            "intent_id": intent.id,
            "client_secret": intent.client_secret,
            "status": intent.status,
        }

    async def _confirm_payment_intent(self, params: dict[str, Any]) -> dict[str, Any]:
        """Confirm payment intent."""
        intent = await self.connector.confirm_payment_intent(
            payment_intent_id=params["payment_intent_id"],
            payment_method_id=params.get("payment_method_id"),
        )

        return {"success": True, "intent_id": intent.id, "status": intent.status}

    async def _cancel_payment_intent(self, params: dict[str, Any]) -> dict[str, Any]:
        """Cancel payment intent."""
        intent = await self.connector.cancel_payment_intent(params["payment_intent_id"])

        return {"success": True, "intent_id": intent.id, "status": intent.status}

    async def _create_charge(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create charge."""
        charge = await self.connector.create_charge(
            amount=params["amount"],
            currency=params["currency"],
            source=params.get("source"),
            customer_id=params.get("customer_id"),
            description=params.get("description"),
        )

        return {
            "success": True,
            "charge_id": charge.id,
            "status": charge.status.value,
            "receipt_url": charge.receipt_url,
        }

    async def _create_customer(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create customer."""
        customer = await self.connector.create_customer(
            email=params.get("email"),
            name=params.get("name"),
            description=params.get("description"),
            payment_method_id=params.get("payment_method_id"),
        )

        return {"success": True, "customer_id": customer.id, "email": customer.email}

    async def _get_customer(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get customer."""
        customer = await self.connector.get_customer(params["customer_id"])

        return {
            "success": True,
            "customer_id": customer.id,
            "email": customer.email,
            "name": customer.name,
            "default_source": customer.default_source,
        }

    async def _create_subscription(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create subscription."""
        subscription = await self.connector.create_subscription(
            customer_id=params["customer_id"],
            price_id=params["price_id"],
            quantity=params.get("quantity", 1),
            trial_period_days=params.get("trial_period_days"),
        )

        return {
            "success": True,
            "subscription_id": subscription.id,
            "status": subscription.status.value,
            "current_period_end": subscription.current_period_end.isoformat(),
        }

    async def _get_subscription(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get subscription."""
        subscription = await self.connector.get_subscription(params["subscription_id"])

        return {
            "success": True,
            "subscription_id": subscription.id,
            "status": subscription.status.value,
            "current_period_end": subscription.current_period_end.isoformat(),
        }

    async def _cancel_subscription(self, params: dict[str, Any]) -> dict[str, Any]:
        """Cancel subscription."""
        subscription = await self.connector.cancel_subscription(
            subscription_id=params["subscription_id"],
            at_period_end=params.get("at_period_end", True),
        )

        return {
            "success": True,
            "subscription_id": subscription.id,
            "status": subscription.status.value,
        }

    async def _list_invoices(self, params: dict[str, Any]) -> dict[str, Any]:
        """List invoices."""
        invoices = await self.connector.list_invoices(
            customer_id=params.get("customer_id"),
            status=params.get("status"),
            limit=params.get("limit", 100),
        )

        return {"success": True, "invoices": invoices, "count": len(invoices)}

    async def _create_refund(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create refund."""
        refund = await self.connector.create_refund(
            charge_id=params["charge_id"],
            amount=params.get("amount"),
            reason=params.get("reason"),
        )

        return {"success": True, "refund_id": refund["id"], "amount": refund["amount"]}

    async def _create_product(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create product."""
        product = await self.connector.create_product(name=params["name"], description=params.get("description"))

        return {"success": True, "product_id": product["id"], "name": product["name"]}

    async def _create_price(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create price."""
        price = await self.connector.create_price(
            product_id=params["product_id"],
            unit_amount=params["unit_amount"],
            currency=params["currency"],
            recurring_interval=params.get("recurring_interval", "month"),
        )

        return {
            "success": True,
            "price_id": price["id"],
            "unit_amount": price["unit_amount"],
        }

    async def close(self):
        """Close the connector."""
        await self.connector.close()
