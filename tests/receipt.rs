use rust_decimal::Decimal;

use polycopy_engine::{OrderReceipt, ReceiptError};

#[test]
fn fak_zero_fill_does_not_become_a_phantom_full_fill() {
    let receipt = OrderReceipt::from_fak_match(qty(10), qty(10), Decimal::ZERO)
        .expect("a zero-fill FAK receipt is valid");

    assert_eq!(receipt.requested_qty(), qty(10));
    assert_eq!(receipt.accepted_qty(), qty(10));
    assert_eq!(receipt.filled_qty(), Decimal::ZERO);
    assert_eq!(receipt.remaining_qty(), Decimal::ZERO);
}

#[test]
fn fak_partial_fill_uses_matched_shares_not_requested_quantity() {
    let receipt = OrderReceipt::from_fak_match(qty(10), qty(10), Decimal::new(325, 2))
        .expect("a partial-fill FAK receipt is valid");

    assert_eq!(receipt.requested_qty(), qty(10));
    assert_eq!(receipt.filled_qty(), Decimal::new(325, 2));
    assert_ne!(receipt.filled_qty(), receipt.requested_qty());
    assert_eq!(receipt.remaining_qty(), Decimal::ZERO);
}

#[test]
fn receipt_rejects_quantities_larger_than_the_request() {
    let error = OrderReceipt::from_fak_match(qty(10), qty(10), qty(11))
        .expect_err("a venue cannot fill more than was requested");

    assert_eq!(
        error,
        ReceiptError::ExceedsRequested {
            field: "filled_qty",
            value: qty(11),
            requested_qty: qty(10),
        }
    );
}

fn qty(value: i64) -> Decimal {
    Decimal::from(value)
}
