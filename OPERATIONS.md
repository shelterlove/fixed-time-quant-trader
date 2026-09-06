# 测试网运行手册

本手册只覆盖当前仓库的 Binance USDⓈ-M Futures 测试网部署。交易程序只支持 Hedge Mode、逐仓、受限 2x、单资产模式；订单名义金额由策略的 1.0--1.30 倍阶梯规则限定，不会因交易所可用余额增加而再次放大。签名请求被代码固定为测试网地址。

## 首次部署

在 VPS 上安装 Docker Engine 与 Docker Compose plugin 后：

```bash
git clone https://github.com/shelterlove/fixed-time-quant-trader.git
cd fixed-time-quant-trader
cp .env.example .env
chmod 600 .env
```

编辑 `.env`，填写专用 Binance 测试网 API 密钥。首次保持：

```dotenv
TRADING_ENABLED=false
```

然后执行：

```bash
./deploy.sh
```

首次部署脚本会构建镜像、运行只读账户检查、预热 P90 历史、启动 `trader` 与 `dashboard`，最后显示容器状态。确认账户与运行状态正确后，将 `.env` 中的 `TRADING_ENABLED` 改为 `true`，再运行一次：

```bash
./deploy.sh
```

监控页默认地址为 `http://VPS_IP:8080`。它显示账户净值、累计与当日净收益、回撤和新仓倍率、当前持仓保护、历史成交净收益、最近决策、当前开仓阻断与运行事件。累计收益从本地首个完整分钟净值开始，并扣除已同步的账户转入转出；旧成交尚未同步完整手续费时明确显示“待同步”，不会按零处理。进程在线、允许新仓和仓位保护是三个独立状态。

## 日常升级

选择非决策分钟升级。常规升级不重新预热 P90，也不会启动额外的 `live-seed` 写入进程：

```bash
cd ~/fixed-time-quant-trader
git pull --ff-only
git describe --tags --always
./deploy.sh
docker compose ps
```

若 `trader` 正在运行，脚本会执行受控的 `docker compose up -d --build --wait` 升级并跳过预热。若没有运行中的 `trader`，脚本按首次启动流程检查并预热后再启动。

升级包含账本迁移的版本前，先停止交易进程并使用 SQLite 备份接口保存状态（包含 WAL 中已提交的数据）：

```bash
docker compose stop trader
docker compose run --rm --no-deps trader python -c 'import sqlite3; from datetime import datetime, timezone; from pathlib import Path; p=Path("/app/runtime/backups"); p.mkdir(exist_ok=True); s=sqlite3.connect("file:/app/runtime/testnet.sqlite3?mode=ro",uri=True); d=sqlite3.connect(p/(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")+".sqlite3")); s.backup(d); d.close(); s.close()'
git pull --ff-only
./deploy.sh
```

新交易进程会在持有运行锁时迁移状态、补种有候选标识的规范影子记录。已有 1x 仓位不会被自动改杠杆；新版本会继续处理其保护与退出，但在它们平仓前阻断新开仓。空仓后，新仓使用 `exchange-5s-v2`、逐仓 2x 和持久化的一分钟盯市净值。完整差异见 [`REPAIR_REVIEW.md`](REPAIR_REVIEW.md)。

回滚至旧程序前，必须先确认交易所空仓且无挂单。新版本仓位和未决订单需要由新版本完成对账与退出；不要在有仓位时恢复旧数据库。空仓后可备份当前账本，切回旧代码并使用对应的空仓状态库，再执行部署。

## 监控与常用命令

```bash
# 容器与健康状态
docker compose ps

# 最近交易程序日志；正常运行时没有日志输出也是正常的
docker compose logs --tail=100 trader

# 跟随日志
docker compose logs -f trader

# 检查心跳是否在 30 秒内
docker compose exec trader python -m fixed_time.cli live-health --root /app

# 只读检查测试网账户模式、余额、仓位与订单
docker compose run --rm trader python -m fixed_time.cli live-check --root /app
```

`trader` 通常每 5 秒轮询账户与持仓，每 60 秒安排一次完整对账和成交/资金流水增量同步。网络延迟会延后实际执行。信号、影子与持仓行情在后台读取，主线程串行写库和下单；决策时点最多允许 120 秒开仓，账户阻断或超期不会丢弃已经形成的影子候选。仪表盘前台每 5 秒刷新，后台标签降为 30 秒；所有浏览器共享服务端短缓存，页面不会直接请求交易所。

仪表盘容器不注入交易所密钥，并以只读方式挂载 `runtime/`。镜像只复制运行所需源码、策略配置和种子数据，`research/`、`data/` 与结果目录不会进入构建上下文。

实时信号排名和多头 P90 影子历史使用完整正式网 USDT 永续行情宇宙。候选生成后，测试网未上线或暂停的正式网合约会被排除在容量分配与下单之外，不会发送无效订单，也不会因测试网合约差异改变其他币种的横截面排名或 P90 历史。决策详情会以 `testnet_eligible: false` 标记这类候选。

## 单实例与状态文件

`live-run`、`live-seed` 与 `live-smoke` 会对 `runtime/testnet.sqlite3` 取得进程级独占锁。同一运行数据库已被交易程序占用时，后两个写入命令会直接失败，不会与交易程序并发操作。

锁文件位于 `runtime/testnet.sqlite3.lock`。文件本身在正常退出后可能保留；锁由操作系统持有，进程退出或崩溃后会自动释放。因此不要通过删除锁文件来处理运行问题。

`live-check` 仅访问交易所账户，不提交订单，可在交易程序运行时使用。

## 停止与恢复

```bash
# 停止容器；主机 runtime/ 目录和 SQLite 状态会保留
docker compose down

# 再次启动；deploy.sh 会先检查是否存在运行中的 trader
./deploy.sh
```

若需要禁止任何新的测试网下单，将 `.env` 中的 `TRADING_ENABLED` 设为 `false` 后再部署。不要在仍有持仓时这样做：市价退出和缺失硬止损后的恢复平仓同样需要交易权限；交易所已托管的硬止损仍由交易所执行。

## 异常处理边界

- 发现未知交易所持仓、未知订单/算法单、数量不一致或无法确认交易所止损时，程序记录对账阻断并停止运行。
- 已知持仓缺少硬止损时，程序尝试补挂；仍无法保护时以 `UNPROTECTED_RECOVERY` 市价退出。
- GET 请求按配置有限重试；写请求不直接重发。网络错误记录到运行状态，交易循环继续恢复未决订单。不能确认前不创建替代退出单；交易所已挂保护单继续有效。
- 影子缺口记录在 `shadow_tasks.last_error`，保留增量进度后再尝试；不因影子失败终止持仓保护。`P90_FALLBACK` 事件表示可用规范样本不足，采用配置中的回退阈值。
- `live-smoke` 会实际开平最小名义金额仓位，且要求专用测试网账户完全空仓、无普通订单、无算法单。不得在运行策略时执行。

当前阶段没有外部告警。新仓已有的硬止损与盈利保护均可在 VPS 离线时由交易所触发；继续抬高保护价、时间退出和旧仓分钟保护需要进程在线。
