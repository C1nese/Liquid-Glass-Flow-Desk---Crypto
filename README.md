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

多交易所加密市场终端，聚合现货、合约、OI、Funding、爆仓、盘口、逐笔成交和多空情绪数据，用于本地研究、监控和交易观察。

当前主线架构为：

- `FastAPI + 原生前端 SPA + 本地 runtime manager`
- 保留 `Streamlit` 旧版工作台作为 legacy 入口

适合的使用场景：

- 多交易所市场监控
- 多币种对比研究
- 爆仓 / 流动性 / OI / Funding 观察
- 本地量化研究台与交易辅助工作台

## 功能概览

支持的核心能力：

- 多交易所行情聚合：Binance、Bybit、OKX、Hyperliquid、Bitget、Gate、HTX
- 现货 / 合约双市场观察
- OI、Funding、Basis、Lead/Lag、多空比、盘口质量、逐笔成交
- 实时爆仓流、爆仓价带图、热力图、联动观察
- 监控台、多币种工作台、执行台、预警中心、盘口中心、历史回放
- 本地 SQLite 历史存储与归档

主要视图：

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
- 监控 / 总览 / 爆仓 / 预警 / 执行台等主要业务逻辑

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

## 环境要求

建议环境：

- Python 3.9+
- Windows / Linux / VPS 均可

安装依赖：

```bash
pip install -r requirements.txt
```

如果你之前遇到 `numpy 2.x` 与 `pandas / pyarrow` 的兼容问题，请直接使用仓库里的依赖版本约束重新安装。

## 启动方式

### 启动新版前端 + API

```bash
uvicorn api_server:app --host 0.0.0.0 --port 8000
```

打开：

```text
http://127.0.0.1:8000
```

### 启动旧版 Streamlit

```bash
streamlit run app.py
```

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

## 注意事项

- 部分交易所公开接口存在限流、样本不完整或字段口径差异。
- Funding、OI、多空比等指标在不同交易所口径并不完全一致，项目内部会做归一和代理补全。
- 某些面板在冷启动时可能先进入预热状态，随后再补齐数据。
- Hyperliquid 等来源的部分爆仓 / 情绪数据能力受官方公开接口限制。

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

## 许可与说明

本仓库更偏向个人研究终端 / 本地工作台，不构成投资建议。

如果你准备继续扩展本项目，推荐下一步补齐：

- 部署说明
- 环境变量说明
- 截图与 GIF 演示
- API 参数表
- 交易所数据口径说明


## License

All rights reserved.

This repository is proprietary unless you explicitly relicense it.
