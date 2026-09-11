#![cfg(all(feature = "execute", feature = "intl_clob"))]

use polycopy_engine::{
    canary::{CanaryOrderSpec, CanarySide},
    canary_run,
    copytrading::{execute::SizedDecision, prepare},
    venue::execution_contract::{ClobOrderAmount, Side},
};
use rust_decimal::Decimal;

fn production_buy(budget: Decimal) -> SizedDecision {
    SizedDecision {
        intent_id: 1,
        token_id: "123456".to_owned(),
        side: Side::Buy,
        // This deliberately cannot be multiplied by price into exact cents.
        // The builder contract must use the budget instead of this estimate.
        qty: Decimal::new(17241, 4),
        limit_price: Decimal::new(58, 2),
        buy_budget: Some(budget),
    }
}

#[test]
fn buy_canary_and_production_agree_on_maker_usdc_not_independent_shares() {
    let canary = CanaryOrderSpec::new(
        "123456".to_owned(),
        CanarySide::Buy,
        Decimal::new(58, 2),
        Decimal::ONE,
    )
    .expect("valid BUY canary");

    assert_eq!(
        canary_run::construction_amount(&canary).expect("canary amount"),
        ClobOrderAmount::BuyMakerUsdc(Decimal::ONE),
    );
    assert_eq!(
        prepare::construction_amount(&production_buy(Decimal::ONE)).expect("production amount"),
        ClobOrderAmount::BuyMakerUsdc(Decimal::ONE),
    );
}

#[test]
fn buy_canary_rejects_a_non_cent_maker_budget() {
    let canary = CanaryOrderSpec::new(
        "123456".to_owned(),
        CanarySide::Buy,
        Decimal::new(58, 2),
        Decimal::new(1001, 3),
    )
    .expect("size itself is positive");

    assert!(canary_run::construction_amount(&canary).is_err());
    assert!(prepare::construction_amount(&production_buy(Decimal::new(1001, 3))).is_err());
}

#[test]
fn sell_canary_and_production_keep_share_amounts() {
    let canary = CanaryOrderSpec::new(
        "123456".to_owned(),
        CanarySide::Sell,
        Decimal::new(58, 2),
        Decimal::new(17241, 4),
    )
    .expect("valid SELL canary");
    let production = SizedDecision {
        intent_id: 1,
        token_id: "123456".to_owned(),
        side: Side::Sell,
        qty: Decimal::new(17241, 4),
        limit_price: Decimal::new(58, 2),
        buy_budget: None,
    };

    let expected = ClobOrderAmount::SellMakerShares(Decimal::new(17241, 4));
    assert_eq!(canary_run::construction_amount(&canary).unwrap(), expected);
    assert_eq!(prepare::construction_amount(&production).unwrap(), expected);
}
