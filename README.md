# MinerU Service Adapter

一个将上游 FastAPI 主后端与 MinerU 本地解析服务解耦的中间层，负责进程管理、任务调度、MinIO 文件流转及回调通知。

## 功能特性
- 启动/管理本地 `mineru-api` Worker，多 GPU/多进程并行（轮询分配）。
- 接收 MinIO 路径，下载 PDF，调用 MinerU 解析，上传 Markdown（图片内嵌 base64）。
- 回调通知：成功/失败/超时都会向业务后端回调，支持默认回调地址。
- 健康检查与 Worker 自动重启：连接失败或超时会触发重启，避免显存泄漏。

## 环境要求
- Python 3.10
- CUDA 12.9  Driver Version: 576.52（目前CUDA 12系列与对应适配的驱动应该没有问题，需自行测试）。
- NVIDIA GPU 与 CUDA 驱动可选（若使用 `CUDA_VISIBLE_DEVICES`，需安装 nvidia-container-toolkit）。
- 可访问的 MinIO/S3 兼容存储。

## 快速开始
### 1) 准备环境变量
```bash
cp .env.example .env
# 根据实际环境修改 .env
```

### 2) 安装依赖
```bash
pip install -r requirements.txt
```

### 3) 启动服务（本地）
```bash
python main.py
# 默认监听 0.0.0.0:${CONTROLLER_PORT:-58001}
```

### 4) Docker Compose（如需容器化）
仓库包含 `Dockerfile` 和 `docker-compose.yml`，可根据需要调整挂载的模型、MinIO 端口及 GPU 透传参数后执行：
```bash
docker-compose up --build -d
```

### 5) 试跑一次
```bash
curl -X POST http://localhost:58001/parse \
  -H "Content-Type: application/json" \
  -d '{
    "task_id": "task_123",
    "paper_id": "paper_abc",
    "input_pdf": "pdf-raw/paper_abc.pdf",
    "output_md": "pdf-parsed/paper_abc.md",
    "callback_url": "http://host.docker.internal:58000/api/v1/mineru/callback"
  }'
```

## 目录结构
```
mineru/
├── Dockerfile
├── docker-compose.yml
├── main.py                 # 控制器：子进程管理、任务调度、回调
├── mineru_config.json      # MinerU 配置（可挂载）
├── requirements.txt
├── .env.example            # 环境变量模板
└── README.md
```

## 环境变量说明
在 `.env` 中配置，`.env` 已加入 gitignore。

必填/常用：
- `MINERU_WORKER_COUNT`：启动的 Worker 数量（显存不够请设 1-2）。
- `START_PORT`：Worker 起始端口（默认 6000，依次递增）。
- `CUDA_VISIBLE_DEVICES`：GPU ID（本地/容器需匹配宿主 GPU）。
- `CONTROLLER_PORT`：控制器端口（默认 58001）。
- `TASK_TIMEOUT_SECONDS`：单任务超时，超时触发回调并标记需重启。
- `MINIO_ENDPOINT`：MinIO 地址，支持 http/https，形如 `minio:59000` 或 `localhost:9000`。
- `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` / `MINIO_SECURE`：MinIO 凭证与安全选项。
- `MINERU_INTERNAL_TOKEN`：回调到业务后端时携带的 Bearer Token。
- `MINERU_DEFAULT_CALLBACK_URL`：当请求未提供 `callback_url` 时的兜底地址。

可选（本地 MinIO 引导）：
- `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`

## API 概览
- `POST /parse`：提交异步解析任务（需要 MinIO 路径）。系统满载时返回 503。
- `POST /test/direct_parse`：调试用，直接上传 PDF 返回 ZIP（包含 md 与资源）。
- `GET /health`：健康检查，返回 Worker 状态、进程 PID、空闲数等。

### /parse 请求字段
- `task_id` (string, required)
- `paper_id` (string, required)
- `input_pdf` (string, required) — `bucket/path.pdf`
- `output_md` (string, required) — `bucket/path.md`
- `callback_url` (string, optional) — 若为空将使用 `MINERU_DEFAULT_CALLBACK_URL`
- `parse_method` (string, optional, default `auto`)

### 回调示例
成功：
```json
{
  "task_id": "task_123",
  "paper_id": "paper_abc",
  "status": "SUCCESS",
  "md_key": "pdf-parsed/paper_abc.md",
  "execution_time_ms": 5000
}
```
失败/超时：
```json
{
  "task_id": "task_123",
  "paper_id": "paper_abc",
  "status": "FAILED",
  "error_message": "Worker Connection Error",
  "execution_time_ms": 30000
}
```

## 运行与调优建议
- 显存：优先降低 `MINERU_WORKER_COUNT`，单卡 24GB 建议 3-4 个 Worker。
- 超时：`TASK_TIMEOUT_SECONDS` 过短可能导致大文件频繁超时；过长会拖延重启。
- 端口：默认 Worker 从 6000 起连续占用，控制器端口 58001。
- 日志：`logs/controller_*.log`，包括 Worker 启停、重启、错误堆栈。
- 临时目录：`/tmp/{task_id}` 自动清理。

## 常见故障排查
- MinIO 连接失败：检查 `MINIO_ENDPOINT`/凭证/网络；确保去掉协议或使用正确端口。
- Worker 启动失败/显存不足：调低 `MINERU_WORKER_COUNT`，检查 GPU 驱动与权限。
- 回调失败：确认业务后端可达；设置 `MINERU_INTERNAL_TOKEN` 与回调鉴权一致。
- 解析超时：增大 `TASK_TIMEOUT_SECONDS`，或检查输入 PDF 体积与硬件资源。

## 贡献与许可证
- 欢迎 PR / Issue（建议附环境与复现步骤）。
