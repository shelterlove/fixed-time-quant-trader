# 测试网运维

## 首次部署

1. 复制 `.env.example` 为 `.env`，填写测试网 API key/secret。
2. 保持 `TRADING_ENABLED=false`，执行 `docker compose build` 和 `docker compose run --rm --no-deps trader python -m fixed_time.cli live-check --root /app`。
3. 确认账户为 Hedge Mode、Single-Asset Mode，且持仓使用 isolated 2x。
4. 设置 `TRADING_ENABLED=true`，执行 `./deploy.sh`。
5. `docker compose ps` 应显示 trader 和 dashboard healthy；面板位于 `http://127.0.0.1:8080`。

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

```bash
python -m fixed_time.cli strategy-check --root .
python -m fixed_time.cli live-check --root .
python -m fixed_time.cli live-reconcile --root .
python -m fixed_time.cli live-health --root .
docker compose logs -f trader
```

`live-check` 只读交易所，可与 trader 同时执行。`live-reconcile` 会修改订单和账本、获取运行锁，不能与 trader 同时运行。dashboard 和 `live-health` 只读数据库；交易健康需要近期心跳、权益记录且无未解决事件。面板 HTTP `/healthz` 只表示面板服务可响应，交易健康在 `/api/status` 的 `healthy` 字段。

面板默认只绑定 VPS 的 `127.0.0.1:8080`。本地可用 `ssh -L 8080:127.0.0.1:8080 用户@VPS` 后访问；已有反向代理可继续连接该端口。

## 事故口径

- `UNKNOWN_EXCHANGE_POSITION`：交易所有本地无法解释的方向仓位；禁止新开仓。
- `POSITION_QUANTITY_MISMATCH`：交易所方向总量与各 lot 合计不同；禁止新开仓。
- `UNKNOWN_EXCHANGE_ORDER/ALGO`：存在非本策略订单；禁止新开仓。
- `UNCONFIRMED_EXIT:<lot>`：交易所仓位已归零但成交不足。保留待核对记录、暂停新增仓位，权益仍更新；后续获得成交会自动恢复，始终查不到时需人工核对。
- `E0_RECOVERED_LATE`：当日零点后首次启动且没有零点权益快照，使用首次可用权益建立当日 E0；事件会保留。

旧版出现 `HTTP 400 / -1021` 时，表示签名请求时间戳领先币安服务器。新版启动时先校准服务器时间，签名时间戳额外向后留出 250ms，并在首次 `-1021` 后重新校准、重签名且只重试一次。若新版仍连续报告该错误，应先检查 VPS 的 NTP/chrony 状态和到币安测试网的网络延迟；不要通过扩大决策期限补做已经错过的候选。

旧版的 `EQUITY_SNAPSHOT · cannot value equity while exchange quantities are unresolved` 是账户对账阻断的下游结果，可能由 `ACCOUNT_RECONCILIATION`、未知交易所仓位或仓位数量不一致触发。新版直接采用交易所账户总钱包余额与未实现盈亏记录权益，但仓位归属或数量没有核对一致时仍会禁止新开仓。升级前必须确认旧账户无仓位、无挂单；不要删除数据库来解除阻断。

## 停止

正常停止使用 `docker compose stop trader`。存在持仓时不要撤销 API key 或只停止 trader 后长期离线；交易所硬止损和多头 +400% 条件单仍在，但计划退出、延长和 +300% 后的保护线切换需要引擎运行。
