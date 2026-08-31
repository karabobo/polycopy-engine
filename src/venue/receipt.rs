use std::{error::Error, fmt};

use rust_decimal::Decimal;

/// A venue receipt whose quantity fields intentionally remain distinct.
///
/// `requested_qty` and `accepted_qty` use the order's request unit. For a SELL
/// that unit is outcome shares; for a BUY it is the CLOB's notional/budget
/// unit. `filled_qty` and `remaining_qty` use outcome shares. They therefore
/// cannot be compared generically: a better-priced BUY can fill more shares
/// than its requested budget value. `filled_qty` is the only quantity that a
/// later lot-accounting layer may apply. For a FAK result, `remaining_qty` is
/// always zero because unmatched quantity expires.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderReceipt {
    requested_qty: Decimal,
    accepted_qty: Decimal,
    filled_qty: Decimal,
    remaining_qty: Decimal,
}

impl OrderReceipt {
    /// Builds a final-or-open venue receipt after checking that all quantities
    /// are non-negative.
    ///
    /// This constructor deliberately does not compare requested and filled
    /// quantities. Use [`Self::from_fak_buy_budget`] or
    /// [`Self::from_fak_sell_shares`] for a FAK, where the order side provides
    /// the unit needed for a meaningful bound.
    pub fn new(
        requested_qty: Decimal,
        accepted_qty: Decimal,
        filled_qty: Decimal,
        remaining_qty: Decimal,
    ) -> Result<Self, ReceiptError> {
        validate_nonnegative("requested_qty", requested_qty)?;
        validate_nonnegative("accepted_qty", accepted_qty)?;
        validate_nonnegative("filled_qty", filled_qty)?;
        validate_nonnegative("remaining_qty", remaining_qty)?;
        Ok(Self {
            requested_qty,
            accepted_qty,
            filled_qty,
            remaining_qty,
        })
    }

    /// Creates a final BUY FAK receipt from the CLOB's actual matched shares.
    ///
    /// `requested_budget` and `accepted_budget` must not be exceeded by one
    /// another, but `matched_shares` intentionally has no such bound: a BUY
    /// filled below its limit can receive more outcome shares than the budget
    /// value in the request. Returning the budget as filled shares would
    /// create a phantom lot for a zero or partial FAK fill.
    pub fn from_fak_buy_budget(
        requested_budget: Decimal,
        accepted_budget: Decimal,
        matched_shares: Decimal,
    ) -> Result<Self, ReceiptError> {
        validate_not_greater_than_requested(
            "accepted_qty",
            accepted_budget,
            requested_budget,
        )?;
        Self::new(
            requested_budget,
            accepted_budget,
            matched_shares,
            Decimal::ZERO,
        )
    }

    /// Creates a final SELL FAK receipt, for which every quantity is outcome
    /// shares and therefore an actual fill must not exceed the requested sell
    /// quantity.
    pub fn from_fak_sell_shares(
        requested_shares: Decimal,
        accepted_shares: Decimal,
        matched_shares: Decimal,
    ) -> Result<Self, ReceiptError> {
        validate_not_greater_than_requested(
            "accepted_qty",
            accepted_shares,
            requested_shares,
        )?;
        validate_not_greater_than_requested(
            "filled_qty",
            matched_shares,
            requested_shares,
        )?;
        Self::new(
            requested_shares,
            accepted_shares,
            matched_shares,
            Decimal::ZERO,
        )
    }

    pub fn requested_qty(&self) -> Decimal {
        self.requested_qty
    }

    pub fn accepted_qty(&self) -> Decimal {
        self.accepted_qty
    }

    pub fn filled_qty(&self) -> Decimal {
        self.filled_qty
    }

    pub fn remaining_qty(&self) -> Decimal {
        self.remaining_qty
    }
}

fn validate_nonnegative(field: &'static str, value: Decimal) -> Result<(), ReceiptError> {
    if value < Decimal::ZERO {
        return Err(ReceiptError::NegativeQuantity { field, value });
    }
    Ok(())
}

fn validate_not_greater_than_requested(
    field: &'static str,
    value: Decimal,
    requested_qty: Decimal,
) -> Result<(), ReceiptError> {
    if value > requested_qty {
        return Err(ReceiptError::ExceedsRequested {
            field,
            value,
            requested_qty,
        });
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReceiptError {
    NegativeQuantity {
        field: &'static str,
        value: Decimal,
    },
    ExceedsRequested {
        field: &'static str,
        value: Decimal,
        requested_qty: Decimal,
    },
}

impl fmt::Display for ReceiptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeQuantity { field, value } => {
                write!(formatter, "{field} must not be negative, received {value}")
            }
            Self::ExceedsRequested {
                field,
                value,
                requested_qty,
            } => write!(
                formatter,
                "{field} ({value}) must not exceed requested_qty ({requested_qty})"
            ),
        }
    }
}

impl Error for ReceiptError {}
