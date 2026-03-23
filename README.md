# Liquid Glass Flow Desk 苹果UI版
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/b6fabbbd-efb6-4162-9948-1c220112f55e" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/8ecf0174-329d-4178-9c61-944c948c3b8c" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/3c21cca8-fd82-4854-ab6d-1322a61d8541" />

<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/83c508e6-58d4-4579-8f23-f9f99559036f" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/4ed457be-7cca-4665-9e40-678e30a3db77" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/3f5e3cf2-5ba5-4f6d-8f9d-64e8651e6f82" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/dc3c62a1-6dd1-43a9-ac32-d7277f0e4163" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/7b7701a9-3f7b-4c4b-9178-a9c52ce904fe" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/a987e8e0-b65d-4a98-be68-aae1c39debf8" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/f2d6e5eb-6527-46e7-9488-d2683cb0c413" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/3e5913f2-740b-45d7-8df6-f69573a29ed0" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/80304624-a758-4c25-85ce-d7123208c405" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/22d9d5c0-4c83-4d1d-9322-9519d7e3b3c6" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/00c7a1eb-24bf-4eeb-a20b-a44bdae376dc" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/35ddfd75-431d-48e2-9259-190d973885ac" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/89bcc333-7e20-4611-9d85-28a4d941a768" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/612dbfe5-b147-467a-8a50-2657ea2a89c0" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/16fdd2f9-b255-4887-8594-c9d3af071c1f" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/0a82a6f8-881e-4740-89c5-268cd3ba41e9" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/340d99c5-23a8-4d48-916d-fd2b526af34a" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/8658d4be-f7ae-4dcd-9914-4f84426c6558" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/c0f341f6-6a25-4b14-97be-b3573e0a62ec" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/f28a9013-9bd6-42a4-8e9c-c6190983464b" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/894fb7c9-1ff2-4422-ac9b-a0eb27086c09" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/9c008b6a-fa82-482a-a3a9-13f488ae544b" />
<img width="281" height="314" alt="image" src="https://github.com/user-attachments/assets/4d3b89de-e5fc-4676-9e36-51e5a5fe72e7" />
<img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/1d2eb493-715c-4878-8e28-7a5e30a0d7ac" />
 <img width="1920" height="925" alt="image" src="https://github.com/user-attachments/assets/3ad8af4e-b4ef-4803-81f6-1336528e5c71" />

多交易所现货 / 合约参考终端。

核心目标是把：

- 多交易所实时行情
- 现货 / 合约对照
- OI / Funding / 多空比 / 爆仓 / 大额成交
- 本地历史、事件回放、告警解释

放到同一套可交互终端里。

## 这是什么架构

当前项目是 **双栈并存架构**：

- 旧栈：`Streamlit` 单体应用
- 新栈：`FastAPI + 本地运行时管理器 + 原生前端 SPA`

更准确地说，它是一个：

- **本地状态型后端**
- **带共享实时层的多交易所数据终端**
- **带 SQLite 历史库和本地归档的分析系统**

而不是纯静态网站，也不是标准无状态 BFF。

## 核心组成

### 1. API / Web 入口

- [api_server.py](/E:/Codex/ex/api_server.py)
  FastAPI 入口，负责：
  - HTTP API
  - SSE / WebSocket 推送
  - 静态前端挂载
  - runtime manager 启动与关闭

- [web/index.html](/E:/Codex/ex/web/index.html)
- [web/app_main.js](/E:/Codex/ex/web/app_main.js)
- [web/app_shared.js](/E:/Codex/ex/web/app_shared.js)
- [web/app_views.js](/E:/Codex/ex/web/app_views.js)
- [web/styles.css](/E:/Codex/ex/web/styles.css)

这是现在主要使用的新前端。

### 2. 运行时中枢

- [market_runtime.py](/E:/Codex/ex/market_runtime.py)

这是项目的中台。它负责：

- coin session 生命周期
- payload 生成
- 本地缓存
- 预热 / 预计算
- 参考层 / 监控层 / 告警层 / 执行层
- 历史读取与回退

主要对象：

- `MarketRuntimeSession`
- `MarketRuntimeManager`

### 3. 数据源层

- [exchanges.py](/E:/Codex/ex/exchanges.py)

统一封装各交易所 REST：

- Binance
- Bybit
- OKX
- Hyperliquid
- Bitget
- Gate
- HTX

负责：

- snapshot
- candles
- trades
- OI
- liquidation
- 多空比 / 情绪类公开接口
- 交易所请求健康、冷却、退避

### 4. 实时层

- [realtime.py](/E:/Codex/ex/realtime.py)

负责：

- WebSocket
- sampler
- 共享实时 hub
- 逐笔成交 / 爆仓 / 深度 / 快照
- 地址模式流

当前已经是“**共享连接 + 部分共享结构化解析**”架构，不再是每个 session 各起一套完整实时链路。

### 5. 分析层

- [analytics.py](/E:/Codex/ex/analytics.py)

负责：

- DataFrame 组装
- Plotly 图表
- 指标、评分、参考层、结论层
- 多空比、情绪、执行质量、市场结构分析

### 6. 历史与归档层

- [storage.py](/E:/Codex/ex/storage.py)

负责：

- SQLite 本地历史库
- archive 归档
- 热冷分层
- 聚合副表
- 历史查询 facade

本地数据目录：

- [`.terminal_data/terminal_history.sqlite3`](/E:/Codex/ex/.terminal_data/terminal_history.sqlite3)
- [`.terminal_data/archive`](/E:/Codex/ex/.terminal_data/archive)
- [`.terminal_data/liquidations`](/E:/Codex/ex/.terminal_data/liquidations)

### 7. 旧版页面

- [app.py](/E:/Codex/ex/app.py)

这是旧版 Streamlit 单体工作台，仍然可运行，但现在项目主线已经偏向 FastAPI + SPA。

## 当前数据流

可以把整套系统理解成这条链：

1. 前端发起 `overview / monitor / alerts / execution / history` 请求
2. `api_server.py` 把请求交给 `MarketRuntimeManager`
3. `MarketRuntimeSession` 优先读取：
   - 实时层共享状态
   - 本地 cache
   - 本地历史
4. 缺口部分再由后台预热或交易所 adapter 补齐
5. `analytics.py` 负责把原始数据变成 frame / figure / card
6. `storage.py` 把快照、事件、质量点、OI、bars、transport 写入本地
7. SSE / WebSocket 只负责把 payload 推到前端

## 当前主要能力

### 市场支持

- 合约
- 现货
- 现货 / 合约对照
- 多交易所对照
- 多币种对照

### 数据能力

- Snapshot
- Depth / Orderbook
- Trades / Tape
- Liquidations
- OI
- Funding
- Basis
- 多空比
- 合约情绪
- 现货参考层 / 合约参考层 / 综合结论层

### 工作台能力

- 总览
- 信息榜
- 监控
- 多币种
- 执行层
- 告警中心
- 历史
- 调试 / 健康页
- AI 副驾驶与单事件 drill-down

### 本地持久化能力

- market snapshots
- market events
- orderbook quality points
- signal events
- oi points
- market bars 1m
- transport state
- liquidation archive

## 项目结构

```text
E:\Codex\ex
├─ api_server.py        # FastAPI API + SSE/WS + 静态前端挂载
├─ market_runtime.py    # 运行时中枢、payload、预热、缓存
├─ realtime.py          # 共享实时层、WS、sampler、地址流
├─ exchanges.py         # 交易所 REST 适配
├─ analytics.py         # 指标、图表、DataFrame、评分
├─ storage.py           # SQLite / archive / 聚合 / 热冷分层
├─ api_models.py        # FastAPI response model
├─ models.py            # 数据模型
├─ legacy_history.py    # 旧历史页辅助
├─ legacy_health.py     # 旧健康页辅助
├─ legacy_address.py    # 旧地址页辅助
├─ app.py               # 旧版 Streamlit 单体应用
├─ web/
│  ├─ index.html
│  ├─ app_main.js
│  ├─ app_shared.js
│  ├─ app_views.js
│  └─ styles.css
└─ .terminal_data/      # 本地数据库、归档、事件文件
```

## 运行方式

### 依赖

```bash
pip install -r requirements.txt
```

当前核心依赖：

- `fastapi`
- `uvicorn`
- `requests`
- `pandas`
- `plotly`
- `websocket-client`
- `streamlit`

### 启动新前端 + API

```bash
uvicorn api_server:app --host 0.0.0.0 --port 8000
```

访问：

- [http://127.0.0.1:8000](http://127.0.0.1:8000)

### 启动旧版 Streamlit

```bash
streamlit run app.py
```

## 本地状态与数据

这个项目**不是无状态后端**。

它会在本地维护：

- runtime cache
- 共享实时状态
- 后台预热线程
- SQLite 历史库
- parquet / csv.gz 归档
- UI preferences

因此它非常适合：

- 本地桌面工作台
- 自己的服务器 / VPS
- 常驻进程

而不适合“短生命周期、频繁冷启动”的 serverless 运行方式。

## 能不能直接上 Vercel

### 结论

**不能直接把这整套系统原样上 Vercel。**

### 原因

因为这套项目依赖以下能力，而它们都不适合 Vercel 的典型 serverless 形态：

1. **长连接和常驻状态**

- WebSocket / SSE
- shared realtime hub
- runtime session
- 后台 precompute worker

2. **本地持久化**

- SQLite
- 本地 archive
- liquidation 文件归档
- UI preferences

3. **长生命周期内存状态**

- cache
- source revision
- fanout topic
- background warm jobs

4. **本地线程与后台任务**

- ThreadPoolExecutor
- precompute thread
- monitor fetch executor
- cache warm jobs

Vercel 更适合：

- 静态前端
- 轻 API
- 无状态函数

而你这个项目是：

- 有状态
- 本地落盘
- 长连接
- 后台线程
- 长期运行

### 哪些部分可以上 Vercel

如果你只是想“挂一个网页壳子”，有两种可行拆法：

#### 方案 A：只把前端静态页放 Vercel

- `web/` 前端静态文件可部署
- 但必须把 API 改成独立后端
- 前端通过公网访问你自己的 FastAPI 服务

这个方案的前提是：

- 后端改成单独部署在 VPS / 云主机 / Docker / Railway / Fly.io / Render / 自建服务器

#### 方案 B：前后端彻底拆分

把当前系统拆成：

- `frontend`
- `stateful backend`
- `market collector / realtime worker`
- `database / archive`

这就不是“直接上 Vercel”，而是一次结构性改造。

### 更合适的部署方式

如果你想线上常驻运行，优先建议：

- Windows / Linux 自建机器
- VPS
- Docker + 持久卷
- 一台常驻云主机

最适合的形态是：

- `uvicorn + systemd/supervisor/pm2/nssm`
- 本地磁盘保留 `.terminal_data`

## 当前架构优点

- 本地状态丰富，适合做交易工作台
- 共享实时层已经有明显性能优化
- 有历史库和归档，支持复盘
- 前后端已经分离，新栈比旧 Streamlit 更容易继续扩展

## 当前架构边界

- `market_runtime.py` 仍然偏大
- fanout 还不是纯事件总线最终形态
- realtime 共享层还没做到所有交易所、所有消息全覆盖
- 全站参数 schema 和字段 schema 还在收口中

## 如果继续演进，最值得做什么

当前最值钱的后续方向是：

1. 把 fanout 继续推进成真正的数据驱动推送
2. 把 realtime 剩余未共享 handler 继续并入共享结构化总线
3. 继续收紧参数 schema 和 canonical 字段
4. 继续把历史图板切到“聚合秒开 + 细节按需回读”

## License

All rights reserved.

This repository is proprietary unless you explicitly relicense it.
