# 跟单操作面板(ops_panel)实现计划

面向用户的中文操作面板,在服务器上运行:`ssh -t <host> /opt/polycopy-engine/current/target/release/ops_panel`(需 root)。
只调用 systemctl / journalctl / 只读数据库 / 现有 `copy_config_apply` 与 `persistent_control`;不含任何下单逻辑。

## 已定决策(业主 2026-09-29)
- 只手改一个文件 `/etc/polycopy-engine/trading-config.json`:新增 `runtime` 段(账户级单笔上限、滚动预算、预算窗口、轮询间隔、补抓间隔)和每个 leader 的 `display_name`。`copy_config_apply` 忽略未知字段,向后兼容。
- `persistent-public.env` 与数据库 `persistent_execution_config` 由面板从该文件生成/同步(`persistent_control reconfigure`),文件头标"由面板生成,请勿手改"。私钥仍只在 `credentials/copy-secrets.env`,面板只显示"已配置"。
- leader2 显示名 `leader2-ratio-maker`;清空 leader2 的 `max_order_shares`(首次正式使用应用流程时执行)。
- 服务器上现有 trading-config.json(2026-09-19)已过时,修改前必须先从数据库重新生成。

## 进度
- **第一阶段已完成(未提交)**:总览(服务状态、启动/停止/重启带二次确认与后果提示、保险丝、未结 case、配置检查汇总、磁盘、最近 24 小时统计)、日志(journald 实时、切换服务、只看重要、暂停、翻看)、配置详情(每个 leader 全部参数的中文名/当前值/说明/是否生效、账户与运行参数、与 `copy_persistent` 启动校验完全一致的一致性检查)。24 个单元测试;`--all-features` 全量 422 个库测试通过,严格 clippy 通过。
- **发布前提**:配置页"单笔上限"的说明描述的是方案 B(Codex 的 89117c7,比例单超限时压缩份数)上线后的行为,面板必须与它一起或在它之后发布。
- **第二阶段已完成(未提交)**:配置页按 e 进入修改流程。面板从数据库重新生成 JSON,用 $EDITOR 编辑;拼错的字段名会报错,不会被悄悄忽略;在数据库副本上用真实的 `apply_trading_config` 和 `reconfigure_config` 试运行;用中文列出改动,停用 leader 用红字标出;然后依次备份、停服务、`copy_config_apply`、重新生成 env、`persistent_control reconfigure`、复查、确认启动;出错可以按 r 回滚。只改显示名时不停服务。子进程里才加载私钥,面板进程本身不读。新增 11 个测试(共 35 个);`--all-features` 全量 433 个库测试通过,严格 clippy 通过。使用说明见 `docs/OPS_PANEL.md`。
- **2026-09-29 已上线(a2e0fd4)**,服务器上总览和日志页显示正常。首次查看后的显示修正:日志去掉主机名/进程号前缀并自动换行;数据库时间(最新信号、保险丝、异常单)换算成北京时间;总览表格在窄窗口里隐藏"说明"列、启动时间缩短为 `09-29 06:04`。
- 还没有在真实服务器上跑过应用流程。第一次建议先按 e,看完预览后按 n 放弃,零风险;第一次正式使用,就执行已经定好的两项改动(leader2 显示名、清空 max_order_shares)。

## 模块(src/copytrading/ops/,feature `ops_panel = ["execute", "dep:ratatui"]`;复用 `persistent::PersistentRuntimeConfig::from_values` 使一致性检查与启动校验一致)
1. `live_config.rs`:从数据库读 accounts、leader_config、启用的 leader_wallet_aliases、leader_policy 全部列、persistent_execution_config;导出为统一 JSON。**必须能原样回灌**:用导出的 JSON 调 `apply_trading_config` 结果应全部 `Unchanged`(写测试,复用 setup.rs 测试里的 TestDb 模式)。回灌要点(见 setup.rs `normalize_policy`):
   - 数值字段以 `Decimal::to_string()` 存储;`price_tolerance_abs` 缺省存 "0";`size_ratio` 以 `normalize()` 存储;
   - `budget_window_seconds` 只能与 `rolling_budget_usdc` 同时出现;
   - `max_order_notional` 受 `max_notional_ceiling` 约束,应用时由 runtime 段账户上限自动传入 `POLYCOPY_CONFIG_MAX_NOTIONAL_CEILING`。
2. `checks.rs`:一致性检查(中文结果):env 允许的 leader = 启用的 leader = persistent_execution_config.allowed_leader_ids;env 账户级额度/窗口/间隔 = persistent_execution_config;env 签名类型/funder = accounts;各启用 leader 的 max_order_notional ≤ 账户上限(否则运行时 ConfigMismatch);执行开关;私钥文件存在。
3. `labels.rs`:每个字段的中文名、说明、"当前模式下是否生效"(比例模式下 max_order_shares、max_order_notional 目前不生效 —— 待 Codex 的方案 B 修复后 max_order_notional 生效;实际容差 = max(bps×价格, abs);maker-only 下 tick_size 字段不用)。
4. `services.rs`:解析 `systemctl show -p ActiveState,SubState,ActiveEnterTimestamp,MainPID,ExecMainStatus`(纯函数可测);`/proc/<pid>/exe` 推运行版本;启动/停止/重启;`journalctl -u <unit> -f -n 200 -o short-iso` 子进程逐行读入通道;"只看重要"过滤(成交、拒单、熔断、报错,去掉 WS 重连噪音)。
5. `stats.rs`:最近 24 小时信号数、成交、未成交(GTD 过期)、被拒、超时;保险丝;未结 case;最新信号时间;磁盘剩余。
6. `ui.rs`:AppState + 三个页面(①总览 ②日志 ③配置)+ 二次确认弹窗 + 底部按键说明,纯函数用 ratatui TestBackend 测试(沿用 dashboard.rs 做法)。
7. `src/bin/ops_panel.rs`:事件循环;数字键切页;总览页选服务后 启动/停止/重启 需确认,保险丝未清时提示"启动会立即退出";配置页 ↑↓ 选 leader 看全部参数。

## 第二阶段:修改配置流程
从数据库重新生成 JSON → nano 编辑 → 中文逐项预览变更(停用 leader 时红字警告其持仓/未结 case)+ 一致性检查 → 备份数据库(SQLite `.backup`)与旧 JSON(带时间戳归档)→ 停交易服务 → `copy_config_apply` → 生成 env → `persistent_control reconfigure` → 确认 → 启动并检查。

## 相关待办(不在本分支)
- Codex:比例模式超上限按方案 B 压缩下单量;账户级单笔 BudgetExceeded 改为只拒该笔(说明已给业主)。
- Codex:部署脚本自动清理旧版本、删除编译中间产物、编译前检查磁盘。
- `tools/notify/notify.py` 使用 display_name。
- leader2 小额单回报分析,决定 min_leader_trade_size(暂不改)。
- 本地另一份旧的 trading-config.json 副本同样过时,待业主决定是否删除。
