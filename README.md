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

# Liquid Glass Flow Desk

# Liquid Glass Flow Desk

一个面向加密市场研究、监控与交易观察的本地终端，聚合多交易所现货 / 合约数据，并提供 OI、Funding、爆仓、盘口、逐笔成交、多空情绪、预警与历史回放能力。

当前主线架构：

- `FastAPI + 原生前端 SPA + 本地 runtime manager`
- 保留 `Streamlit` 旧版工作台作为 legacy 入口

适合的使用场景：

- 多交易所市场监控
- 多币种横向对比
- OI / Funding / 爆仓 / 流动性研究
- 本地量化研究工作台
- 盘中交易辅助观察

---

## 功能概览

支持的核心能力：

- 多交易所行情聚合：Binance、Bybit、OKX、Hyperliquid、Bitget、Gate、HTX
- 现货 / 合约双市场联动观察
- OI、Funding、Basis、Lead/Lag、多空比、盘口质量、逐笔成交
- 实时爆仓流、爆仓价带图、清算热力图、联动观察
- 总览、监控台、多币种工作台、执行台、预警中心、盘口中心、历史回放
- 本地 SQLite 历史存储与归档

主要页面：

- 总览
- 信息榜
- 监控台
- 多币种
- 执行台
- 现货/合约
- 深度
- 爆仓
- 预警
- 盘口
- 地址
- 历史
- 实验室
- 调试
- 健康
- 旧版工作台

---

## 项目截图

当前仓库里还没有正式截图资源，建议后续将截图放到：

- `docs/images/overview.png`
- `docs/images/monitor.png`
- `docs/images/liquidations.png`
- `docs/images/multicoin.png`

推荐在 GitHub README 中展示这几类画面：

1. 总览首页
   展示滚动摘要、参考层、市场概览卡片。
2. 监控台
   展示大额成交、OI 异动、价格异动、交易所分区。
3. 爆仓 / 清算热力图
   展示模型图表、时间窗口切换、交易所筛选。
4. 多币种工作台
   展示 Funding、OI、情绪评分、多空比对比。

示例占位：

```md
![Overview](docs/images/overview.png)
![Monitor](docs/images/monitor.png)
![Liquidations](docs/images/liquidations.png)
![Multicoin](docs/images/multicoin.png)
```

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

如果你之前遇到 `numpy 2.x` 与 `pandas / pyarrow` 的兼容问题，请直接使用仓库内的依赖版本约束重新安装。

### 2. 启动新版前端 + API

```bash
uvicorn api_server:app --host 0.0.0.0 --port 8000
```

打开：

```text
http://127.0.0.1:8000
```

### 3. 启动旧版 Streamlit

```bash
streamlit run app.py
```

### 4. 基础校验

```bash
python -m py_compile api_server.py market_runtime.py realtime.py exchanges.py storage.py
node --check web/app.js
node --check web/app_main.js
node --check web/app_shared.js
node --check web/app_views.js
```

---

## 架构说明

### 1. API 与前端入口

- `api_server.py`
  FastAPI 入口，负责 HTTP API、SSE / WebSocket、静态资源挂载、runtime 生命周期管理。
- `web/index.html`
- `web/app.js`
- `web/app_main.js`
- `web/app_shared.js`
- `web/app_views.js`
- `web/styles.css`

### 2. 运行时核心

- `market_runtime.py`

项目核心文件，负责：

- 单币种 runtime session
- 多币种 manager 协调
- payload 组装
- 缓存与预热
- 总览 / 监控 / 爆仓 / 预警 / 执行台等主要业务逻辑

### 3. 交易所与实时层

- `exchanges.py`
  统一封装各交易所 REST 接口。
- `realtime.py`
  实时流、共享 hub、逐笔成交、爆仓、盘口与采样状态。

### 4. 分析与存储

- `analytics.py`
  DataFrame、评分、图表和分析逻辑。
- `storage.py`
  SQLite 历史库、归档、事件落地和历史查询。

### 5. 旧版工作台

- `app.py`

保留旧版 Streamlit 入口，方便对照和兼容使用，但当前主线已转向 FastAPI + SPA。

---

## 数据流

系统主要数据流如下：

1. 前端请求 `overview / monitor / multicoin / execution / liquidations / alerts`
2. `api_server.py` 将请求转交给 `MarketRuntimeManager`
3. `market_runtime.py` 优先读取：
   - 会话内实时状态
   - 本地缓存
   - 本地历史
4. 缺口数据再由：
   - `realtime.py`
   - `exchanges.py`
   - 本地归档 / SQLite
   回补
5. `analytics.py` 把原始数据转成卡片、表格、图表和面板
6. 结果通过 HTTP / SSE / WebSocket 推给前端

---

## 目录结构

```text
.
├── api_server.py
├── market_runtime.py
├── realtime.py
├── exchanges.py
├── analytics.py
├── storage.py
├── api_models.py
├── models.py
├── request_schema.py
├── app.py
├── tests/
├── web/
│   ├── index.html
│   ├── app.js
│   ├── app_main.js
│   ├── app_shared.js
│   ├── app_views.js
│   └── styles.css
└── .terminal_data/
```

---

## 环境变量说明

当前代码里已经使用到的环境变量主要有这些：

### API / 前端服务

- `LEGACY_STREAMLIT_PORT`
  旧版 Streamlit 端口，默认 `8501`
- `LEGACY_STREAMLIT_AUTOSTART`
  是否自动拉起 legacy Streamlit，默认 `0`

### 预热与后台任务

- `LGFD_PRECOMPUTE_INTERVAL_SECONDS`
  后台预计算周期，默认 `30`
- `LGFD_PRECOMPUTE_WORKERS`
  预计算 worker 数量，默认 `2`
- `LGFD_ENABLE_PRECOMPUTE_WORKER`
  是否启用预计算线程，默认 `1`
- `LGFD_PRECOMPUTE_BOOT_DELAY_SECONDS`
  启动后延迟多久再进入预热，默认 `45`
- `LGFD_HOT_RUNTIME_COINS`
  热币 runtime 列表，默认 `BTC,ETH,SOL,XRP`
- `LGFD_FANOUT_SOURCE_POLL_SECONDS`
  fanout source 轮询间隔，默认 `3.0`

### runtime / 存储阈值

- `LGFD_SESSION_SAMPLE_SECONDS`
  会话级采样秒数
- `LGFD_TAPE_MIN_NOTIONAL_FLOOR`
  逐笔成交最小金额地板
- `LGFD_PERSISTED_EVENT_MIN_NOTIONAL`
  事件持久化最小金额
- `LGFD_PERSISTED_QUALITY_MIN_NOTIONAL`
  盘口质量持久化最小金额
- `LGFD_PERSISTED_QUALITY_IMBALANCE_PCT`
  盘口失衡事件阈值
- `LGFD_PERSISTED_QUALITY_HOT_RETENTION_HOURS`
  盘口质量热数据保留时长
- `LGFD_PERSISTED_QUALITY_EVENT_RETENTION_HOURS`
  盘口质量事件保留时长
- `LGFD_MONITOR_SCAN_CHUNK`
  全局 monitor 扫描块大小
- `LGFD_MONITOR_ROW_TTL_SECONDS`
  monitor 行缓存 TTL

### 通知相关

- `LGFD_TELEGRAM_BOT_TOKEN`
  Telegram bot token
- `LGFD_TELEGRAM_CHAT_ID`
  Telegram chat id

### 存储相关

- `LGFD_ENABLE_SQLITE`
  是否启用 SQLite 历史存储

示例：

```bash
set LGFD_HOT_RUNTIME_COINS=BTC,ETH,SOL,XRP,BNB
set LGFD_ENABLE_PRECOMPUTE_WORKER=1
set LGFD_PRECOMPUTE_BOOT_DELAY_SECONDS=30
set LGFD_TELEGRAM_BOT_TOKEN=xxxx
set LGFD_TELEGRAM_CHAT_ID=xxxx
```

---

## 部署说明

### 本地开发部署

```bash
pip install -r requirements.txt
uvicorn api_server:app --host 0.0.0.0 --port 8000
```

### Windows 常驻运行

适合：

- 本地桌面工作站
- 内网研究终端
- 个人交易观察机

建议：

- 使用固定 Python 环境
- 将 `.terminal_data/` 放在稳定磁盘位置
- 通过任务计划程序或 NSSM 做常驻服务

### Linux / VPS 部署

建议使用：

- `python -m venv .venv`
- `systemd`
- `nginx` 反代

示例流程：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn api_server:app --host 127.0.0.1 --port 8000
```

反代时注意：

- WebSocket 需要透传
- SSE 需要关闭不必要的缓冲
- 如果要长期运行，建议单独挂日志与数据目录

### 生产部署建议

- 仅将服务暴露在受信任网络或加认证的网关后
- 定期清理 `.terminal_data/archive`
- 关注交易所公开接口限流
- 热门币种可预热，长尾币种按需拉取

---

## 常用接口

主要 API：

- `/api/overview`
- `/api/overview-rich`
- `/api/monitor`
- `/api/multicoin`
- `/api/execution`
- `/api/liquidations`
- `/api/alerts`
- `/api/orderbook-center`
- `/api/health`
- `/api/debug/runtime-manager`

实时推送：

- `/api/stream/liquidations`
- `/api/stream/alerts`
- `/api/stream/monitor/*`
- `/ws/overview`
- `/ws/liquidations`
- `/ws/alerts`

---

## 本地存储

项目不是无状态后端。

它会在本地维护：

- runtime cache
- 会话内实时状态
- 预热缓存
- SQLite 历史库
- 爆仓归档
- UI 偏好与面板布局

常见本地目录：

- `.terminal_data/`
- `.terminal_data/archive/`
- `.terminal_data/liquidations/`

---

## 开发说明

建议优先关注这些文件：

- `market_runtime.py`
- `api_server.py`
- `realtime.py`
- `web/app_views.js`
- `web/app_shared.js`

如果你要新增或修复功能，通常入口可以这样找：

- 页面表现问题：`web/`
- 接口返回问题：`api_server.py`
- 数据聚合 / 面板内容问题：`market_runtime.py`
- 交易所抓数问题：`exchanges.py`
- 实时流问题：`realtime.py`

---

## FAQ

### 1. 为什么启动后首包比较慢？

可能原因：

- 冷启动还在预热 runtime
- 多交易所 REST 回补尚未完成
- 本地缓存为空
- 首次构建 overview / multicoin / liquidations 较重

建议：

- 等待首包完成后再观察二次请求耗时
- 调整 `LGFD_PRECOMPUTE_BOOT_DELAY_SECONDS`
- 预热热点币种 `LGFD_HOT_RUNTIME_COINS`

### 2. 为什么有些交易所或币种会显示样本不足？

可能原因：

- 交易所公开接口本身字段不足
- 当前时间窗口样本偏少
- 某些功能优先依赖实时流，刚启动时还没积累足够样本

### 3. Funding 为什么看起来和交易所页面数值不一样？

不同页面可能展示：

- 原始费率值
- 百分比风格显示
- 内部风控口径

如果你面向用户展示，建议统一成交易所常见的费率值风格，例如：

- `0.00389`
- `-0.00400`

### 4. 为什么爆仓 / 热力图在某些币种下看起来样本很少？

可能原因：

- 当前窗口太短
- 对应币种公开爆仓样本少
- 交易所支持程度不同
- 当前图在用推断层而不是真实事件层

### 5. 为什么终端里 README 看起来像乱码？

大多数情况是 PowerShell 显示编码问题，不是文件本身损坏。  
GitHub 按 UTF-8 渲染时通常会正常显示。

### 6. 能直接部署到公网吗？

可以，但不建议裸露暴露。

更推荐：

- 放在 nginx / Caddy / Traefik 后面
- 仅对可信网络开放
- 加认证或网关访问控制

---

## 注意事项

- 部分交易所公开接口存在限流、样本不完整或字段口径差异
- Funding、OI、多空比等指标在不同交易所口径并不完全一致，项目内部会做归一和代理补全
- 某些面板在冷启动时可能先进入预热状态，随后再补齐数据
- Hyperliquid 等来源的部分爆仓 / 情绪数据能力受官方公开接口限制

---

## 测试与校验

仓库包含：

- `tests/`
- 多个 `smoke_*` 回归脚本

基础校验示例：

```bash
python -m py_compile api_server.py market_runtime.py realtime.py exchanges.py storage.py
node --check web/app.js
node --check web/app_main.js
node --check web/app_shared.js
node --check web/app_views.js
```

---

## 许可与说明

本仓库更偏向个人研究终端 / 本地工作台，不构成投资建议。

如果你准备继续扩展本项目，推荐下一步补齐：

- `docs/images/` 正式截图
- 环境变量示例文件
- 部署脚本
- API 参数手册
- 交易所口径说明

## License

All rights reserved.

This repository is proprietary unless you explicitly relicense it.
