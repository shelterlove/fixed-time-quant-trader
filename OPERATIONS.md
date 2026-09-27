# 测试网运维

## 首次部署

1. 复制 `.env.example` 为 `.env`，填写测试网 API key/secret。
2. 保持 `TRADING_ENABLED=false`，执行 `docker compose build` 和 `docker compose run --rm --no-deps trader python -m fixed_time.cli live-check --root /app`。
3. 确认账户为 Hedge Mode、Single-Asset Mode，且持仓使用 isolated 2x。
4. 设置 `TRADING_ENABLED=true`，执行 `./deploy.sh`。
5. `docker compose ps` 应显示 trader、dashboard 和 dashboard_https 运行正常；公网面板位于 `https://141-98-199-119.sslip.io:8444/`。

`deploy.sh` 沿用现有 Docker Compose，记录 Git revision，构建并运行隔离测试，检查账户，停止旧 trader，通过 SQLite backup API 备份数据库，再更新容器并等待健康检查。测试与第一次预检失败时旧容器继续运行；停机后的预检或备份失败会尝试重启旧容器。

不自动安装 Docker，不生成 API 密钥，不自动改变账户的 Hedge/Single-Asset 模式。缺 `.env` 时只创建模板并提示填写。开仓时自动设置交易币对为 isolated 2x。

代码需要先在本地提交并推送到 GitHub。VPS 第一次取得新版脚本：

```bash
cd /你的仓库目录
git pull --ff-only
sh deploy.sh
```

之后执行 `sh deploy.sh --pull` 即可同步并更新；要求 VPS 工作区干净且分支已设置上游。现有 `.env` 和 `runtime/` 沿用。不要清空数据库来绕过对账问题。

## 升级旧测试网

首次从旧版跨到 v2，先让旧策略已有交易正常结束，确认账户无仓位、无挂单；脚本会校验。旧表保留，不自动接管无法证明归属的旧订单。已有 v2 运行数据则自动沿用并进行对账恢复。

备份位于数据库旁的 `backups/`，默认 `runtime/backups/`。SQLite backup API 会包含 WAL 中尚未写回主文件的数据。更新失败后查看日志，不要直接用旧备份覆盖已经产生新成交的数据库。

## 常用命令

持仓手动操作启用时，在 VPS 的 `.env` 中设置操作密码的 SHA-256 十六进制摘要为 `DASHBOARD_CONTROL_TOKEN`，再运行 `sh deploy.sh`。面板只在 HTTPS 或本地 SSH 隧道中显示“提前卖出/平仓”和“延长／设价”；用户在页面输入操作密码，页面只在当前打开期间保留密码。连续 10 次输错后，同一来源 5 分钟内拒绝操作请求。指令写入账本队列，trader 复核交易所持仓后执行。提前平仓针对所选批次全部剩余数量；手动延长每批次仅一次，从当前计划退出时间增加指定的 1–168 个整数小时，留空为 4 小时。可单独填写该合约的止盈和止损 USDT 价格，未填的一侧沿用原策略；填写的一侧替换该批次对应的交易所条件保护单。引擎会拒绝位于当前市价错误方向、可能立即触发的价格。页面的“指令已入队”表示等待执行，应继续查看持仓状态、保护价格及最近退出。若 trader 停止，排队指令会在重启后重新核对；过期延长会被拒绝。

公网面板地址为 `https://141-98-199-119.sslip.io:8444/`，使用 Caddy 自动签发和续期证书。HTTP 80 仅用于证书验证和跳转；8080 只绑定 VPS 本机。VPS 的 443 已由其他服务占用，因此面板使用 8444。交易操作仍需输入设定的操作密码。

```bash
python -m fixed_time.cli strategy-check --root .
python -m fixed_time.cli live-check --root .
python -m fixed_time.cli live-reconcile --root .
python -m fixed_time.cli live-health --root .
docker compose logs -f trader
```

`live-check` 只读交易所，可与 trader 同时执行。`live-reconcile` 会修改订单和账本、获取运行锁，不能与 trader 同时运行。dashboard 和 `live-health` 只读数据库；交易健康需要近期心跳、权益记录且无未解决事件。面板 HTTP `/healthz` 只表示面板服务可响应，交易健康在 `/api/status` 的 `healthy` 字段。

面板权益曲线可切换 1 天、7 天、近 30 天，`/api/status?period=1d|7d|30d` 按时段分桶返回采样点；区间变化包含出入金影响。当前持仓的“入场均价”来自成交账本，“信号参考价”来自生成候选时的价格，“当前价格”来自交易所测试网公开报价，约 10 秒缓存，报价不可用时显示空值。最近退出按退出订单计算估算毛收益，不含手续费和资金费。测试网订单回执的 `avgPrice` 有时为零；引擎以该订单的逐笔成交价和数量回补实际均价，历史缺价订单也会逐步回补。仍无法取得成交明细的旧记录保留“—”。

面板内部端口只绑定 VPS 的 `127.0.0.1:8080`，公网访问由 dashboard_https 转发。

## 事故口径

`SIGNALS` 出现 `Temporary failure in name resolution` 时，先检查 VPS 宿主机和 trader 容器对 `fapi.binance.com`、`demo-fapi.binance.com` 的解析，再在容器中访问两个 `/fapi/v1/time` 接口。新版错误会写明失败主机；宿主机成功而容器失败时检查 Docker DNS。恢复后用 `live-check` 核对账户和保护单，并确认事件已解除。错过 180 秒截止线的决策不补开仓。

- `UNKNOWN_EXCHANGE_POSITION`：交易所有本地无法解释的方向仓位；禁止新开仓。
- `POSITION_QUANTITY_MISMATCH`：交易所方向总量与各 lot 合计不同；禁止新开仓。
- `UNKNOWN_EXCHANGE_ORDER/ALGO`：存在非本策略订单；禁止新开仓。
- `UNCONFIRMED_EXIT:<lot>`：交易所仓位已归零但成交不足。保留待核对记录、暂停新增仓位，权益仍更新；后续获得成交会自动恢复，始终查不到时需人工核对。
- `EXCHANGE_ADL`：交易所自动减仓成交。对账会核对强制订单、普通订单回执和逐笔成交；单个 lot 的数量可精确归属时自动扣减，全部平仓会记录实际退出时间，部分减仓会调整保护单数量。若同方向同币有多个 lot、成交数量不能精确对应或交易所历史记录不可用，继续阻断新开仓并保留账本供人工核对。
- `E0_RECOVERED_LATE`：当日零点后首次启动且没有零点权益快照，使用首次可用权益建立当日 E0；事件会保留。

旧版出现 `HTTP 400 / -1021` 时，表示签名请求时间戳领先币安服务器。新版启动时先校准服务器时间，签名时间戳额外向后留出 250ms，并在首次 `-1021` 后重新校准、重签名且只重试一次。若新版仍连续报告该错误，应先检查 VPS 的 NTP/chrony 状态和到币安测试网的网络延迟；不要通过扩大决策期限补做已经错过的候选。

旧版的 `EQUITY_SNAPSHOT · cannot value equity while exchange quantities are unresolved` 是账户对账阻断的下游结果，可能由 `ACCOUNT_RECONCILIATION`、未知交易所仓位或仓位数量不一致触发。新版直接采用交易所账户总钱包余额与未实现盈亏记录权益，但仓位归属或数量没有核对一致时仍会禁止新开仓。升级前必须确认旧账户无仓位、无挂单；不要删除数据库来解除阻断。

## 停止

正常停止使用 `docker compose stop trader`。存在持仓时不要撤销 API key 或只停止 trader 后长期离线；交易所硬止损和多头 +400% 条件单仍在，但计划退出、延长和 +300% 后的保护线切换需要引擎运行。
