# 测试网运行手册

本手册只覆盖当前仓库的 Binance USDⓈ-M Futures 测试网部署。交易程序只支持 Hedge Mode、逐仓、1x、单资产模式；签名请求被代码固定为测试网地址。

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

监控页默认地址为 `http://VPS_IP:8080`。它显示运行心跳、可用 USDT、持仓、最近决策、订单和对账事件。

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

`trader` 每 5 秒轮询账户与持仓；至少每 60 秒进行一次完整对账。决策时点允许最多 120 秒完成固定的全市场小时行情读取；仪表盘每 5 秒刷新，健康检查要求心跳不超过 30 秒。

实时信号只从“正式网可读取行情且测试网实际支持下单”的 USDT 永续合约交集生成。测试网未上线或暂停的正式网合约会在候选生成前排除，不会发送无效订单。

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
- 网络类 Binance 错误会有限次数重试；连续五次错误后交易进程退出，由 Compose 的 `unless-stopped` 策略重启。
- `live-smoke` 会实际开平最小名义金额仓位，且要求专用测试网账户完全空仓、无普通订单、无算法单。不得在运行策略时执行。

当前阶段没有外部告警或 VPS 宕机后的交易所外退出机制；交易所硬止损是唯一的进程外保护。
