# 动态 Top100 成员诊断（只读）

范围仅限两个决策时点：AKROUSDT `2022-04-01 06:00 UTC` 与 NAORISUSDT `2026-04-30 06:00 UTC`。旧侧仅扫描了对应时点的 `legacy_research/hourly_features.parquet` 和 `legacy_research/2026H1_hourly_features.parquet` 各一次；没有下载数据、运行回测或修改策略。

新侧采用指定的影子有效性规则：连续 `T-25h…T-1h`、`qv24=Σ(T-24h…T-1h)`、`median(T-25h…T-2h)>0`、`Σ(T-3h…T-1h)>0`；未使用单独的最新小时成交额或成交笔数门槛。当前与 `T-4h` 截面均使用该规则。

## 结论

两处目标排名差异均由旧侧保留已暂停交易的合约造成，而非新旧官方行情修订、并列排序或本地下载遗漏。

- AKRO：旧侧 93、新侧影子 92、共同 92。旧侧多出的 BTCSTUSDT 使 AKRO `r4_rank` 从新侧 88 增至旧侧 89，继而使 `r4_rank_change` 从 -85 变为 -86、二次 ordinal 从 90 变为 91。
- NAORIS：旧侧 100、新侧影子 100、共同 98。旧侧多出的 DAMUSDT 与 ZKJUSDT 的 `r24` 都高于 NAORIS，恰好使其 `r24_rank` 从新侧 10 增至旧侧 12。旧侧/新侧的 `r4_rank_change` 均为 -99；旧侧二次 ordinal 为 94、新侧为 95，是同一成员差异在历史排名变化横截面的传播。

共同成员的 `visible_close`、r1/r4/r24、v1/v4/v24 在两侧仅有二进制求和量级差异（最大约 `4.8e-7`，相对量级约机器精度），没有可导致排名变化的行情修订证据。

## 成员外连接与因果状态

| 时点 | 仅旧成员 | 仅新影子成员 | 关键因子影响 | 本地因果诊断 | 归因 |
| --- | --- | --- | --- | --- | --- |
| AKRO 2022-04-01 06:00 | BTCSTUSDT | — | `r4=0.0`，旧 r4 rank=15，高于 AKRO 的 -0.04907241，直接多占一位 | 连续 25 根；qv24=0、prior median=0、recent qv3=0、最新 qv=0、trade_count=0 | 旧侧有效成员规则不同：把停牌零成交合约留在截面 |
| NAORIS 2026-04-30 06:00 | DAMUSDT | HBARUSDT | DAM `r24=0.75305466`、旧 rank=1，高于 NAORIS | DAM 连续 25 根；qv24=35,169,003.05286，但 prior median=0、recent qv3=0、最新 qv=0、trade_count=0 | 旧侧有效成员规则不同 |
| NAORIS 2026-04-30 06:00 | ZKJUSDT | PIEVERSEUSDT | ZKJ `r24=0.15299793`、旧 rank=6，高于 NAORIS | ZKJ 连续 25 根；qv24=30,238,072.149963，但 prior median=0、recent qv3=0、最新 qv=0、trade_count=0 | 旧侧有效成员规则不同 |
| NAORIS 2026-04-30 06:00 | — | HBARUSDT | `r24=-0.02791316`，新 rank=59，不影响 NAORIS 的前置两位 | 连续 25 根；qv24=25,868,168.70502、prior median=1,001,549.72604、recent qv3=2,404,346.48789、最新 qv=497,621.98684、trade_count=4,890 | 在影子规则下是有效成员；旧侧被零成交成员挤出 |
| NAORIS 2026-04-30 06:00 | — | PIEVERSEUSDT | `r24=-0.05406488`，新 rank=77，不影响 NAORIS 的前置两位 | 连续 25 根；qv24=25,944,580.91370、prior median=963,037.64075、recent qv3=2,438,135.55840、最新 qv=840,474.16560、trade_count=16,323 | 在影子规则下是有效成员；旧侧被零成交成员挤出 |

## 排名核对

| 目标 | 旧侧 | 新侧影子 | 直接原因 |
| --- | ---: | ---: | --- |
| AKRO r4 rank | 89 | 88 | BTCSTUSDT 的 `r4=0` 高于 AKRO |
| AKRO r4 rank change | -86 | -85 | 上述当前 r4 排名一位差 |
| AKRO 二次 ordinal | 91 | 90 | 上述变化值一位差 |
| NAORIS r24 rank | 12 | 10 | DAMUSDT、ZKJUSDT 均高于 NAORIS |
| NAORIS r4 rank change | -99 | -99 | 相同 |
| NAORIS 二次 ordinal | 94 | 95 | 成员差异传播到排名变化横截面 |

## 建议

不应为匹配旧结果而保留 `BTCST`、`DAM` 或 `ZKJ` 这类最近三小时无成交、此前中位成交额为零的停牌成员。指定影子规则对这三者均明确排除，且同时保留 HBAR 与 PIEVERSE 等有因果可见交易活动的成员。

因此，若要采纳该因果澄清，应使用给定的 25 小时连续 / qv24 / prior median / recent qv3 规则统一替换当前成员过滤，并离线重建研究窗口；但这不应以旧侧 AKRO、NAORIS 的排名为验收目标，因为旧目标正是由失效成员造成的。
