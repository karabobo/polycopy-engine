//! Chinese names, explanations and "does this actually apply right now"
//! notes for every per-leader parameter.
//!
//! Several stored fields are silently overridden by others (a ratio beats a
//! fixed share count, the larger of two tolerances wins, maker-only prices on
//! the market's own tick). Showing raw columns side by side made the owner
//! read a leader as "fixed 10 shares, max 10 USDC" when it was really
//! "20% of the leader's shares, capped at 10 USDC, resting maker orders only".
//! The notes below mirror `execute.rs`'s sizing precedence; keep them in step
//! with it.

use chrono::{DateTime, Local, TimeZone};
use rust_decimal::Decimal;

use super::live_config::LivePolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// Governs orders in the leader's current mode.
    Active,
    /// Stored but overridden in the current mode.
    Inactive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldRow {
    pub name: &'static str,
    pub value: String,
    pub note: String,
    pub effect: Effect,
}

fn row(name: &'static str, value: impl Into<String>, note: impl Into<String>, effect: Effect) -> FieldRow {
    FieldRow {
        name,
        value: value.into(),
        note: note.into(),
        effect,
    }
}

fn decimal(raw: &str) -> Option<Decimal> {
    raw.trim().parse().ok()
}

fn percent(raw: &str) -> String {
    match decimal(raw) {
        Some(ratio) => format!("{}%", (ratio * Decimal::ONE_HUNDRED).normalize()),
        None => raw.to_owned(),
    }
}

pub fn yes_no(value: bool) -> &'static str {
    if value {
        "是"
    } else {
        "否"
    }
}

/// `0x1234…abcd`: enough to recognise an address without putting whole
/// wallet addresses on screen for anyone looking over the owner's shoulder.
pub fn short_address(address: &str) -> String {
    let address = address.trim();
    if address.len() <= 12 || !address.is_ascii() {
        return address.to_owned();
    }
    format!("{}…{}", &address[..6], &address[address.len() - 4..])
}

pub fn duration_zh(seconds: i64) -> String {
    match seconds {
        s if s > 0 && s % 86_400 == 0 => format!("{} 天", s / 86_400),
        s if s > 0 && s % 3_600 == 0 => format!("{} 小时", s / 3_600),
        s if s > 0 && s % 60 == 0 => format!("{} 分钟", s / 60),
        s => format!("{s} 秒"),
    }
}

/// A ledger timestamp (RFC 3339, stored in UTC) as `MM-DD HH:MM:SS` in
/// `zone`. Unparseable text is shown as stored.
pub fn ledger_time_in<Tz: TimeZone>(stored: &str, zone: &Tz) -> String
where
    Tz::Offset: std::fmt::Display,
{
    DateTime::parse_from_rfc3339(stored)
        .map(|at| at.with_timezone(zone).format("%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|_| stored.to_owned())
}

/// [`ledger_time_in`] the server's local zone, the one the header clock and
/// the journal use (Beijing time on the production server).
pub fn ledger_time(stored: &str) -> String {
    ledger_time_in(stored, &Local)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizingMode {
    Ratio,
    FixedShares,
    Notional,
}

pub fn sizing_mode(policy: &LivePolicy) -> SizingMode {
    if policy.size_ratio.is_some() {
        SizingMode::Ratio
    } else if policy.max_order_shares.is_some() {
        SizingMode::FixedShares
    } else {
        SizingMode::Notional
    }
}

/// One sentence describing how this leader's BUYs are sized and placed.
pub fn strategy_summary(policy: &LivePolicy) -> String {
    let sizing = match sizing_mode(policy) {
        SizingMode::Ratio => format!(
            "按 leader 份数的 {} 下单,单笔不超过 {} USDC",
            percent(policy.size_ratio.as_deref().unwrap_or_default()),
            policy.max_order_notional
        ),
        SizingMode::FixedShares => format!(
            "每次固定买 {} 份",
            policy.max_order_shares.as_deref().unwrap_or_default()
        ),
        SizingMode::Notional => format!("每次最多 {} USDC", policy.max_order_notional),
    };
    let placement = if policy.maker_only {
        "只挂 maker 单(不吃单)"
    } else {
        "先吃单(FAK),吃不到再挂单"
    };
    format!("{sizing};{placement}")
}

/// The tolerance the engine actually uses at a given leader price: the larger
/// of the absolute and the basis-point tolerance.
pub fn effective_tolerance_note(policy: &LivePolicy) -> String {
    let abs = decimal(&policy.price_tolerance_abs).unwrap_or(Decimal::ZERO);
    match (abs > Decimal::ZERO, policy.price_tolerance_bps > 0) {
        (true, false) => format!("最多比 leader 成交价高 {}", abs.normalize()),
        (false, true) => format!(
            "最多比 leader 成交价高 {}%",
            (Decimal::from(policy.price_tolerance_bps) / Decimal::ONE_HUNDRED).normalize()
        ),
        (true, true) => format!(
            "取两者较大:{} 或成交价的 {}%",
            abs.normalize(),
            (Decimal::from(policy.price_tolerance_bps) / Decimal::ONE_HUNDRED).normalize()
        ),
        (false, false) => "不允许比 leader 成交价高".to_owned(),
    }
}

/// Every stored parameter, in the order the owner reasons about them.
pub fn leader_field_rows(policy: &LivePolicy) -> Vec<FieldRow> {
    use Effect::{Active, Inactive};
    let mode = sizing_mode(policy);
    let mut rows = Vec::new();

    rows.push(match &policy.size_ratio {
        Some(ratio) => row(
            "跟单比例",
            percent(ratio),
            format!("leader 买 100 份,我们买 {}", (decimal(ratio).unwrap_or_default() * Decimal::ONE_HUNDRED).normalize()),
            Active,
        ),
        None => row("跟单比例", "未设置", "不按比例下单", Inactive),
    });

    rows.push(match (&policy.max_order_shares, mode) {
        (Some(shares), SizingMode::FixedShares) => {
            row("固定下单份数", format!("{shares} 份"), "每次固定买这么多份", Active)
        }
        (Some(shares), _) => row(
            "固定下单份数",
            format!("{shares} 份"),
            "未生效:已被跟单比例覆盖",
            Inactive,
        ),
        (None, _) => row("固定下单份数", "未设置", "", Inactive),
    });

    rows.push(match mode {
        SizingMode::Ratio => row(
            "单笔上限",
            format!("{} USDC", policy.max_order_notional),
            "按比例算出的金额超过它时,把份数压到上限以内",
            Active,
        ),
        SizingMode::Notional => row(
            "单笔上限",
            format!("{} USDC", policy.max_order_notional),
            "每次最多花这么多",
            Active,
        ),
        SizingMode::FixedShares => row(
            "单笔上限",
            format!("{} USDC", policy.max_order_notional),
            "未生效:固定份数模式不按金额下单(仍受账户单笔上限约束)",
            Inactive,
        ),
    });

    rows.push(row(
        "只挂 maker 单",
        yes_no(policy.maker_only),
        if policy.maker_only {
            "挂单价取 leader价+容差 与 卖一价−一格 的较低者"
        } else {
            "先按市价吃单,吃不到再挂单"
        },
        Active,
    ));

    rows.push(row(
        "允许同方向连续加仓",
        yes_no(policy.allow_repeated_market_direction),
        if policy.allow_repeated_market_direction {
            "同一市场同一方向 leader 连续买,我们也连续跟"
        } else {
            "同一市场同一方向只跟第一笔"
        },
        Active,
    ));

    let abs = decimal(&policy.price_tolerance_abs).unwrap_or(Decimal::ZERO);
    rows.push(row(
        "价格容差(绝对值)",
        abs.normalize().to_string(),
        effective_tolerance_note(policy),
        if abs > Decimal::ZERO { Active } else { Inactive },
    ));
    rows.push(row(
        "价格容差(比例)",
        format!("{} bps", policy.price_tolerance_bps),
        "1 bps = 0.01%;与绝对值容差取较大者",
        if policy.price_tolerance_bps > 0 { Active } else { Inactive },
    ));

    rows.push(row(
        "价格区间",
        format!("{} ~ {}", policy.min_price, policy.max_price),
        "只跟 leader 成交价落在这个区间内的单",
        Active,
    ));

    let min_size = decimal(&policy.min_leader_trade_size).unwrap_or(Decimal::ZERO);
    rows.push(row(
        "最小跟单门槛",
        format!("{} 份", min_size.normalize()),
        if min_size > Decimal::ZERO {
            "leader 单笔少于这么多份就不跟"
        } else {
            "0 = 不限制;太小的单会在下单前被拒"
        },
        if min_size > Decimal::ZERO { Active } else { Inactive },
    ));

    rows.push(match (&policy.rolling_budget_usdc, policy.budget_window_seconds) {
        (Some(budget), Some(window)) => row(
            "leader 滚动预算",
            format!("{budget} USDC / {}", duration_zh(window)),
            "这段时间内最多花这么多,超出的信号被拒",
            Active,
        ),
        (Some(budget), None) => row(
            "leader 滚动预算",
            format!("{budget} USDC"),
            "窗口沿用账户预算窗口",
            Active,
        ),
        _ => row("leader 滚动预算", "未设置", "只受账户滚动预算限制", Inactive),
    });

    rows.push(row(
        "信号时效",
        format!("{} 秒", policy.max_signal_age_seconds),
        "leader 成交后超过这么久才收到的信号不跟",
        Active,
    ));
    rows.push(row(
        "决策期限",
        format!("{} 秒", policy.decision_window_seconds),
        "生成跟单指令后这么久还没下出去就放弃",
        Active,
    ));

    rows.push(row(
        "同市场对冲",
        yes_no(policy.balance_within_market),
        if policy.balance_within_market {
            "已持有同一市场另一边时,按持有量下单"
        } else {
            "不做特殊处理"
        },
        if policy.balance_within_market { Active } else { Inactive },
    ));

    rows.push(row(
        "价格最小变动",
        policy.tick_size.clone(),
        if policy.maker_only {
            "未生效:只挂 maker 单时改用市场自己的最小价位"
        } else {
            "报价按这个步长取整"
        },
        if policy.maker_only { Inactive } else { Active },
    ));

    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ratio_maker() -> LivePolicy {
        LivePolicy {
            max_signal_age_seconds: 30,
            decision_window_seconds: 20,
            price_tolerance_bps: 0,
            tick_size: "0.01".into(),
            min_price: "0.01".into(),
            max_price: "0.99".into(),
            max_order_notional: "10".into(),
            min_leader_trade_size: "0".into(),
            rolling_budget_usdc: Some("30".into()),
            budget_window_seconds: Some(600),
            max_order_shares: Some("10".into()),
            balance_within_market: false,
            price_tolerance_abs: "0.02".into(),
            size_ratio: Some("0.2".into()),
            allow_repeated_market_direction: true,
            maker_only: true,
        }
    }

    fn find<'a>(rows: &'a [FieldRow], name: &str) -> &'a FieldRow {
        rows.iter().find(|r| r.name == name).unwrap()
    }

    #[test]
    fn ratio_mode_marks_fixed_shares_overridden_and_explains_the_cap() {
        let rows = leader_field_rows(&ratio_maker());
        assert_eq!(find(&rows, "跟单比例").value, "20%");
        assert_eq!(find(&rows, "固定下单份数").effect, Effect::Inactive);
        assert!(find(&rows, "固定下单份数").note.contains("跟单比例覆盖"));
        assert_eq!(find(&rows, "单笔上限").effect, Effect::Active);
        assert_eq!(find(&rows, "价格最小变动").effect, Effect::Inactive);
        assert_eq!(find(&rows, "leader 滚动预算").value, "30 USDC / 10 分钟");
        assert_eq!(
            strategy_summary(&ratio_maker()),
            "按 leader 份数的 20% 下单,单笔不超过 10 USDC;只挂 maker 单(不吃单)"
        );
    }

    #[test]
    fn fixed_shares_mode_marks_notional_overridden() {
        let mut policy = ratio_maker();
        policy.size_ratio = None;
        policy.maker_only = false;
        let rows = leader_field_rows(&policy);
        assert_eq!(find(&rows, "固定下单份数").effect, Effect::Active);
        assert_eq!(find(&rows, "单笔上限").effect, Effect::Inactive);
        assert_eq!(find(&rows, "价格最小变动").effect, Effect::Active);
    }

    #[test]
    fn tolerance_note_reflects_which_tolerance_applies() {
        let mut policy = ratio_maker();
        assert_eq!(effective_tolerance_note(&policy), "最多比 leader 成交价高 0.02");
        policy.price_tolerance_bps = 300;
        assert!(effective_tolerance_note(&policy).starts_with("取两者较大"));
        policy.price_tolerance_abs = "0".into();
        assert_eq!(effective_tolerance_note(&policy), "最多比 leader 成交价高 3%");
    }

    #[test]
    fn helpers_format_addresses_and_durations() {
        assert_eq!(
            short_address("0x00000000000000000000000000000000000000bb"),
            "0x0000…00bb"
        );
        assert_eq!(short_address("0x12"), "0x12");
        assert_eq!(duration_zh(86_400), "1 天");
        assert_eq!(duration_zh(600), "10 分钟");
        assert_eq!(duration_zh(45), "45 秒");
    }

    #[test]
    fn ledger_times_are_shown_in_the_local_zone() {
        let beijing = chrono::FixedOffset::east_opt(8 * 3_600).unwrap();
        // 21:33 UTC is already the next morning in Beijing.
        assert_eq!(ledger_time_in("2026-09-28T21:33:52.123Z", &beijing), "09-29 05:33:52");
        assert_eq!(ledger_time_in("2026-09-25", &beijing), "2026-09-25");
    }
}
