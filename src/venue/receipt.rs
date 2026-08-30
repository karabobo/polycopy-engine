use std::{error::Error, fmt};

use rust_decimal::Decimal;

/// A venue receipt whose quantity fields intentionally remain distinct.
///
/// `requested_qty` is the original logical order size. `accepted_qty` is the
/// size acknowledged by the venue. `filled_qty` is the actual matched size and
/// is the only quantity that a later lot-accounting layer may apply. For a FAK
/// result, `remaining_qty` is always zero because unmatched quantity expires.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderReceipt {
    requested_qty: Decimal,
    accepted_qty: Decimal,
    filled_qty: Decimal,
    remaining_qty: Decimal,
}

impl OrderReceipt {
    /// Builds a final-or-open venue receipt after checking basic quantity bounds.
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
        validate_not_greater_than_requested("accepted_qty", accepted_qty, requested_qty)?;
        validate_not_greater_than_requested("filled_qty", filled_qty, requested_qty)?;
        validate_not_greater_than_requested("remaining_qty", remaining_qty, requested_qty)?;

        Ok(Self {
            requested_qty,
            accepted_qty,
            filled_qty,
            remaining_qty,
        })
    }

    /// Creates a final FAK receipt from the venue's actual matched shares.
    ///
    /// This deliberately accepts `matched_shares` separately from
    /// `requested_qty`: returning the requested size here would create a
    /// phantom fill for a zero-fill or partial-fill FAK order.
    pub fn from_fak_match(
        requested_qty: Decimal,
        accepted_qty: Decimal,
        matched_shares: Decimal,
    ) -> Result<Self, ReceiptError> {
        Self::new(requested_qty, accepted_qty, matched_shares, Decimal::ZERO)
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
