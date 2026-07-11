use crate::TINY_VALUE;
use crate::proto::{
    Attribute, CheckoutRequest, CheckoutResponse, LineItem, MetricSample, PaymentMethod, Shipment,
    ShippingAddress, TelemetryEvent, TinyRequest, TinyResponse,
};

#[must_use]
pub fn tiny_request() -> TinyRequest {
    TinyRequest {
        name: TINY_VALUE.to_owned(),
    }
}

#[must_use]
pub fn tiny_response(request: TinyRequest) -> TinyResponse {
    TinyResponse {
        message: request.name,
    }
}

#[must_use]
pub fn telemetry_event() -> TelemetryEvent {
    let samples = (0_u32..128)
        .map(|offset| MetricSample {
            timestamp_ms: 1_718_000_000_000_i64 + i64::from(offset) * 1_000,
            latency_ns: 10_000 + u64::from(offset % 37) * 125,
            bytes: 512 + u64::from(offset % 11) * 32,
        })
        .collect();
    TelemetryEvent {
        id: 42,
        service: "checkout-api".to_owned(),
        tags: owned(["production", "payments", "latency", "serialization"]),
        samples,
        attributes: attributes(),
    }
}

#[must_use]
pub fn checkout_request() -> CheckoutRequest {
    CheckoutRequest {
        request_id: "req-018f7f6a-9c5d-74aa-9b5d-4c53ab4a2d21".to_owned(),
        method: "CheckoutService.PlaceOrder".to_owned(),
        customer_id: 9_223_372_036_854_775,
        session_id: "sess-018f7f68-f87a-7320-bd3f-5d7bb1686f15".to_owned(),
        currency: "USD".to_owned(),
        items: vec![
            line_item("sku-7YQ-441", 2, 12_999, "iad-1", false),
            line_item("sku-2AB-019", 1, 4_999, "iad-2", true),
            line_item("sku-9LM-220", 3, 1_299, "atl-1", false),
        ],
        shipping_address: Some(ShippingAddress {
            recipient: "Alex Rivera".to_owned(),
            line1: "405 Market Street".to_owned(),
            line2: "Suite 900".to_owned(),
            city: "San Francisco".to_owned(),
            region: "CA".to_owned(),
            postal_code: "94105".to_owned(),
            country: "US".to_owned(),
        }),
        payment_method: Some(PaymentMethod {
            token: "tok_live_51P0gxULkdIwHu7ixQK9vj32v".to_owned(),
            network: "visa".to_owned(),
            last_four: "4242".to_owned(),
            billing_postal_code: "94105".to_owned(),
        }),
        coupon_codes: owned(["SUMMER25", "LOYALTY10"]),
        attributes: attributes(),
    }
}

#[must_use]
pub fn checkout_response() -> CheckoutResponse {
    CheckoutResponse {
        request_id: "req-018f7f6a-9c5d-74aa-9b5d-4c53ab4a2d21".to_owned(),
        order_id: "ord-018f7f6b-13bb-7db5-a29f-52f1e9b0d8a4".to_owned(),
        status: "CONFIRMED".to_owned(),
        subtotal_cents: 34_894,
        discount_cents: 2_500,
        tax_cents: 2_631,
        shipping_cents: 799,
        total_cents: 35_824,
        shipments: vec![
            Shipment {
                shipment_id: "ship-018f7f6b-25ef-7f10-a5a4-18b57d8d6391".to_owned(),
                carrier: "UPS".to_owned(),
                service_level: "GROUND".to_owned(),
                estimated_days: 3,
                item_skus: owned(["sku-7YQ-441", "sku-2AB-019"]),
            },
            Shipment {
                shipment_id: "ship-018f7f6b-2b8e-77e7-b0f0-eab83c7c5d7f".to_owned(),
                carrier: "USPS".to_owned(),
                service_level: "PRIORITY".to_owned(),
                estimated_days: 4,
                item_skus: owned(["sku-9LM-220"]),
            },
        ],
        attributes: attributes(),
    }
}

fn line_item(
    sku: &str,
    quantity: u32,
    unit_price_cents: u64,
    warehouse_id: &str,
    gift_wrap: bool,
) -> LineItem {
    LineItem {
        sku: sku.to_owned(),
        quantity,
        unit_price_cents,
        warehouse_id: warehouse_id.to_owned(),
        gift_wrap,
    }
}

fn attributes() -> Vec<Attribute> {
    [
        ("region", "iad"),
        ("runtime", "rust"),
        ("serializer", "serialization-bench"),
        ("schema", "benchmark-event-v1"),
    ]
    .into_iter()
    .map(|(key, value)| Attribute {
        key: key.to_owned(),
        value: value.to_owned(),
    })
    .collect()
}

fn owned<const N: usize>(values: [&str; N]) -> Vec<String> {
    values.into_iter().map(str::to_owned).collect()
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{checkout_request, checkout_response, telemetry_event, tiny_request};

    #[test]
    fn protobuf_fixture_sizes_match_the_canonical_corpus() {
        assert_eq!(tiny_request().encoded_len(), 19);
        assert_eq!(telemetry_event().encoded_len(), 2_079);
        assert_eq!(checkout_request().encoded_len(), 460);
        assert_eq!(checkout_response().encoded_len(), 371);
    }
}
