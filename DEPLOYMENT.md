# Deployment Guide

这个项目不是纯静态网站，而是一个带本地状态的交易终端后端。

它依赖：

- FastAPI
- WebSocket / SSE
- 本地 SQLite
- 本地 archive 文件
- 后台预热线程
- 共享实时状态

所以部署时最重要的不是“能不能跑起来”，而是：

- 进程要常驻
- 磁盘要持久化
- `.terminal_data` 不能丢
- 不要用频繁冷启动的 serverless 形态

下面按 4 种方式给出推荐方案。

## 1. 本地部署

最适合：

- 自己本机盯盘
- Windows 桌面常驻
- Linux 本地工作站

### 目录要求

项目根目录：

- [api_server.py](/E:/Codex/ex/api_server.py)
- [market_runtime.py](/E:/Codex/ex/market_runtime.py)
- [storage.py](/E:/Codex/ex/storage.py)
- [web/index.html](/E:/Codex/ex/web/index.html)

本地状态目录：

- [`.terminal_data`](/E:/Codex/ex/.terminal_data)
- [`.terminal_ui_preferences.json`](/E:/Codex/ex/.terminal_ui_preferences.json)

### 安装

```bash
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
pip install -r requirements.txt
```

Linux / macOS:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### 启动新前端

```bash
uvicorn api_server:app --host 0.0.0.0 --port 8000
```

访问：

- [http://127.0.0.1:8000](http://127.0.0.1:8000)

### 启动旧版 Streamlit

```bash
streamlit run app.py
```

### 本地部署建议

- 首次启动后，等 1 到 3 分钟让实时缓存和预热起来
- 不要随手删 `.terminal_data`
- 如果长期运行，建议定期备份：
  - [`.terminal_data/terminal_history.sqlite3`](/E:/Codex/ex/.terminal_data/terminal_history.sqlite3)
  - [`.terminal_data/archive`](/E:/Codex/ex/.terminal_data/archive)

### Windows 常驻建议

可以用：

- `nssm`
- `Task Scheduler`
- `pm2-windows-service`

最简单就是把命令做成常驻任务：

```bash
uvicorn api_server:app --host 0.0.0.0 --port 8000
```

## 2. 服务器部署

最适合：

- VPS
- 云主机
- 自建 Linux 服务器

这是最推荐的线上方案。

### 推荐结构

```text
/opt/liquid-glass-flow-desk
├─ current/
├─ venv/
└─ data/
   ├─ .terminal_data/
   └─ .terminal_ui_preferences.json
```

建议把运行目录和数据目录分开。

### Python 环境

```bash
cd /opt/liquid-glass-flow-desk/current
python3 -m venv /opt/liquid-glass-flow-desk/venv
source /opt/liquid-glass-flow-desk/venv/bin/activate
pip install -r requirements.txt
```

### 启动命令

```bash
source /opt/liquid-glass-flow-desk/venv/bin/activate
uvicorn api_server:app --host 0.0.0.0 --port 8000
```

### 反向代理

推荐：

- Nginx
- Caddy

Nginx 反代要注意：

- `proxy_read_timeout`
- `Upgrade / Connection`
- `WebSocket`
- `SSE` 不要被缓冲

### systemd 示例

可新建：

`/etc/systemd/system/liquid-glass-flow-desk.service`

```ini
[Unit]
Description=Liquid Glass Flow Desk
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/opt/liquid-glass-flow-desk/current
Environment=PYTHONUNBUFFERED=1
Environment=LGFD_HOT_RUNTIME_COINS=BTC,ETH,SOL,XRP
Environment=LGFD_PRECOMPUTE_INTERVAL_SECONDS=30
ExecStart=/opt/liquid-glass-flow-desk/venv/bin/uvicorn api_server:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

启用：

```bash
sudo systemctl daemon-reload
sudo systemctl enable liquid-glass-flow-desk
sudo systemctl start liquid-glass-flow-desk
sudo systemctl status liquid-glass-flow-desk
```

### 服务器部署建议

- 给 `.terminal_data` 单独挂持久磁盘
- 给日志做轮转
- 建议至少 2 核 / 4G 内存起步
- 如果你要长期跑多币、多视图、多客户端，建议 4 核更稳

## 3. Docker 部署

最适合：

- 单机容器化
- Docker Compose
- 需要迁移方便，但仍然是有状态运行

注意：

**Docker 可以用，但一定要挂持久卷。**

如果不挂卷：

- SQLite 会丢
- archive 会丢
- 偏好会丢

### 推荐 Dockerfile

当前仓库里还没有现成 `Dockerfile`，建议用下面这版。

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8000

CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8000"]
```

### 构建

```bash
docker build -t liquid-glass-flow-desk .
```

### 直接运行

```bash
docker run -d \
  --name lgfd \
  -p 8000:8000 \
  -v /data/lgfd/.terminal_data:/app/.terminal_data \
  -v /data/lgfd/.terminal_ui_preferences.json:/app/.terminal_ui_preferences.json \
  -e LGFD_HOT_RUNTIME_COINS=BTC,ETH,SOL,XRP \
  -e LGFD_PRECOMPUTE_INTERVAL_SECONDS=30 \
  liquid-glass-flow-desk
```

如果宿主机上还没有 `.terminal_ui_preferences.json`，可以只先挂目录，不挂这个文件。

### Docker Compose 示例

```yaml
version: "3.9"

services:
  lgfd:
    build: .
    container_name: lgfd
    ports:
      - "8000:8000"
    environment:
      LGFD_HOT_RUNTIME_COINS: BTC,ETH,SOL,XRP
      LGFD_PRECOMPUTE_INTERVAL_SECONDS: "30"
    volumes:
      - ./data/.terminal_data:/app/.terminal_data
      - ./data/.terminal_ui_preferences.json:/app/.terminal_ui_preferences.json
    restart: unless-stopped
```

### Docker 部署建议

- `restart: unless-stopped`
- 持久卷必须挂
- 不建议跑在会频繁缩容/重启的容器平台
- 如果平台对 WebSocket / 长连接限制很多，不建议使用

## 4. 前后端拆分部署

最适合：

- 想把网页前端托管到 CDN / Vercel
- 想把后端放到 VPS / Docker / 自建机

这是你现在最接近“上 Vercel”的正确方式。

### 推荐拆法

#### 前端

可部署部分：

- [web/index.html](/E:/Codex/ex/web/index.html)
- [web/app_main.js](/E:/Codex/ex/web/app_main.js)
- [web/app_shared.js](/E:/Codex/ex/web/app_shared.js)
- [web/app_views.js](/E:/Codex/ex/web/app_views.js)
- [web/styles.css](/E:/Codex/ex/web/styles.css)

这部分可以放：

- Vercel
- Netlify
- 静态 CDN

#### 后端

必须独立部署：

- [api_server.py](/E:/Codex/ex/api_server.py)
- [market_runtime.py](/E:/Codex/ex/market_runtime.py)
- [realtime.py](/E:/Codex/ex/realtime.py)
- [storage.py](/E:/Codex/ex/storage.py)
- [exchanges.py](/E:/Codex/ex/exchanges.py)
- [analytics.py](/E:/Codex/ex/analytics.py)

后端更适合放：

- VPS
- Docker 主机
- 自建服务器

### 你需要额外改的地方

因为当前前端默认和后端同域部署，拆分后通常还要补：

1. API Base URL

前端 fetch 需要支持：

- `https://api.example.com`

而不是默认当前域。

2. WebSocket / SSE 地址

当前流接口也需要改成可配置 base URL。

3. CORS

后端要允许前端域名访问。

4. 反向代理和 HTTPS

要正确转发：

- `/api/*`
- `/ws/*`
- `/api/stream/*`

### 前后端拆分后的推荐结构

```text
frontend
└─ web/*

backend
├─ api_server.py
├─ market_runtime.py
├─ realtime.py
├─ storage.py
└─ .terminal_data/
```

### 是否适合直接用 Vercel

#### 可以放到 Vercel 的部分

- 静态前端

#### 不适合直接放到 Vercel 的部分

- 整个 FastAPI runtime
- SQLite
- 本地 archive
- 长连接状态
- 后台预热线程
- shared realtime hub

所以结论是：

**可以“前端上 Vercel + 后端独立部署”，但不能“整套系统直接原样上 Vercel”。**

## 环境变量建议

常用变量：

```bash
LGFD_HOT_RUNTIME_COINS=BTC,ETH,SOL,XRP
LGFD_PRECOMPUTE_INTERVAL_SECONDS=30
LGFD_PRECOMPUTE_WORKERS=2
LGFD_ENABLE_PRECOMPUTE_WORKER=1
LEGACY_STREAMLIT_AUTOSTART=0
LGFD_TAPE_MIN_NOTIONAL_FLOOR=5000
```

如果你部署到服务器，建议把这些变量显式写进：

- `systemd`
- `docker-compose.yml`
- `.env`

## 推荐结论

如果你问“哪种最适合这套项目”：

1. **服务器部署**
   最稳，最适合长期运行
2. **Docker 部署**
   适合标准化运维，但一定要挂持久卷
3. **本地部署**
   最适合自己盯盘
4. **前后端拆分部署**
   适合你想把前端挂公网，但后端仍需独立常驻

如果你问“哪种最不推荐”：

- 直接把整套系统原样塞进 Vercel / 纯 serverless

因为它会和这套项目的状态型设计天然冲突。
