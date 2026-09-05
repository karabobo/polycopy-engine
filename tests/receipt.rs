use rust_decimal::Decimal;

use polycopy_engine::{OrderReceipt, ReceiptError};

#[test]
fn fak_zero_fill_does_not_become_a_phantom_full_fill() {
    let receipt = OrderReceipt::from_fak_buy_budget(qty(10), qty(10), Decimal::ZERO)
        .expect("a zero-fill FAK receipt is valid");

    assert_eq!(receipt.requested_qty(), qty(10));
    assert_eq!(receipt.accepted_qty(), qty(10));
    assert_eq!(receipt.filled_qty(), Decimal::ZERO);
    assert_eq!(receipt.remaining_qty(), Decimal::ZERO);
}

#[test]
fn fak_partial_fill_uses_matched_shares_not_requested_quantity() {
    let receipt = OrderReceipt::from_fak_sell_shares(qty(10), qty(10), Decimal::new(325, 2))
        .expect("a partial-fill FAK receipt is valid");

    assert_eq!(receipt.requested_qty(), qty(10));
    assert_eq!(receipt.filled_qty(), Decimal::new(325, 2));
    assert_ne!(receipt.filled_qty(), receipt.requested_qty());
    assert_eq!(receipt.remaining_qty(), Decimal::ZERO);
}

#[test]
fn buy_fak_can_receive_more_shares_than_its_requested_budget() {
    let receipt = OrderReceipt::from_fak_buy_budget(qty(5), qty(5), Decimal::new(5_288_460, 6))
        .expect("a better-priced BUY may receive more shares than its budget value");

    assert_eq!(receipt.requested_qty(), qty(5));
    assert_eq!(receipt.filled_qty(), Decimal::new(5_288_460, 6));
}

#[test]
fn sell_receipt_rejects_shares_larger_than_the_request() {
    let error = OrderReceipt::from_fak_sell_shares(qty(10), qty(10), qty(11))
        .expect_err("a SELL cannot fill more shares than it offered");

    assert_eq!(
        error,
        ReceiptError::ExceedsRequested {
            field: "filled_qty",
            value: qty(11),
            requested_qty: qty(10),
        }
    );
}

#[test]
fn buy_fak_rejects_a_fill_on_a_zero_quantity_request() {
    // P2-7 defense in depth: a corrupted or fail-open-parsed envelope.size
    // (requested == 0) combined with real venue matched-shares must be
    // rejected, never minted into a phantom lot.
    let error = OrderReceipt::from_fak_buy_budget(Decimal::ZERO, Decimal::ZERO, qty(5))
        .expect_err("a zero-budget BUY can never have a real fill");

    assert_eq!(
        error,
        ReceiptError::FillOnZeroRequest {
            field: "filled_qty",
            filled_qty: qty(5),
        }
    );
}

#[test]
fn sell_fak_rejects_a_fill_on_a_zero_quantity_request() {
    let error = OrderReceipt::from_fak_sell_shares(Decimal::ZERO, Decimal::ZERO, qty(5))
        .expect_err("a zero-quantity SELL can never have a real fill");

    assert_eq!(
        error,
        ReceiptError::FillOnZeroRequest {
            field: "filled_qty",
            filled_qty: qty(5),
        }
    );
}

#[test]
fn zero_request_with_zero_fill_is_still_valid() {
    // The guard must reject only the impossible combination
    // (request == 0, fill > 0); a fully-zero receipt remains legal.
    let receipt = OrderReceipt::from_fak_buy_budget(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
        .expect("a fully-zero receipt is valid");

    assert_eq!(receipt.filled_qty(), Decimal::ZERO);
}

#[test]
fn fill_on_zero_request_error_message_is_stable() {
    let error = OrderReceipt::from_fak_sell_shares(Decimal::ZERO, Decimal::ZERO, qty(3))
        .expect_err("error display pin");
    assert_eq!(
        error.to_string(),
        "filled_qty (3) must be zero when requested_qty is zero -- a zero-quantity request can never have a real fill"
    );
}

#[test]
fn buy_fak_rejects_accepted_budget_above_requested_budget() {
    // P2-6 pin: the BUY-side ExceedsRequested branch (accepted_qty >
    // requested_qty). The SELL-side equivalent is already covered by
    // sell_receipt_rejects_shares_larger_than_the_request; BUY's bound
    // semantics differ (matched_shares has no upper bound) but the
    // accepted > requested guard is symmetric and must also fail closed.
    let error = OrderReceipt::from_fak_buy_budget(qty(10), qty(11), qty(5))
        .expect_err("accepted_budget above requested_budget must reject");

    assert_eq!(
        error,
        ReceiptError::ExceedsRequested {
            field: "accepted_qty",
            value: qty(11),
            requested_qty: qty(10),
        }
    );
}

fn qty(value: i64) -> Decimal {
    Decimal::from(value)
}
