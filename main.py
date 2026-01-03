import os
import time
import subprocess
import shutil
import asyncio
import signal
import sys
import psutil
import httpx
import base64
import re
import traceback
from typing import Optional, Dict
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks, HTTPException, File, UploadFile
from fastapi.responses import Response, PlainTextResponse
from urllib.parse import quote
from pydantic import BaseModel
from minio import Minio
from loguru import logger
from functools import partial

# ================= Configuration =================
MINERU_WORKER_COUNT = int(os.getenv("MINERU_WORKER_COUNT", "1"))
START_PORT = int(os.getenv("START_PORT", "6000"))
GPU_IDS_STR = os.getenv("CUDA_VISIBLE_DEVICES", "0")
GPU_IDS = [g.strip() for g in GPU_IDS_STR.split(",") if g.strip()]
CONTROLLER_PORT = int(os.getenv("CONTROLLER_PORT", "58001"))
TASK_TIMEOUT_SECONDS = int(os.getenv("TASK_TIMEOUT_SECONDS", "300"))
# 首次模型下载/初始化很慢：单独给 warmup 更长超时与更宽松的健康监控窗口
WARMUP_TIMEOUT_SECONDS = int(os.getenv("WARMUP_TIMEOUT_SECONDS", "360"))  # 30min
WARMUP_GRACE_SECONDS = int(os.getenv("WARMUP_GRACE_SECONDS", "180"))       # 10min（不做“无活动重启”判定）

# MinIO Config
MINIO_ENDPOINT_RAW = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"
MINERU_DEFAULT_CALLBACK_URL = os.getenv("MINERU_DEFAULT_CALLBACK_URL", "")
MINIO_ENDPOINT = MINIO_ENDPOINT_RAW.replace("http://", "").replace("https://", "")

# Logging
os.makedirs("logs", exist_ok=True)
logger.add("logs/controller_{time}.log", rotation="100 MB", retention="7 days", level="INFO")

# ================= Global State =================
worker_queue: asyncio.Queue = asyncio.Queue()
worker_registry: Dict[str, dict] = {}
_restart_locks: Dict[str, asyncio.Lock] = {}
_inactivity_counters: Dict[str, int] = {}  # url -> consecutive inactive checks
_cpu_primed_pids: set[int] = set()

minio_client = None
try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
except Exception as e:
    logger.warning(f"MinIO initialization failed: {e}")

# ================= Async Helper Functions (关键优化) =================

async def run_in_thread(func, *args, **kwargs):
    """
    将阻塞的同步 IO/CPU 操作放入线程池执行，防止卡死 asyncio 事件循环
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))

def kill_process_tree(pid: int):
    """暴力清理进程，增加 SIGKILL"""
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        for child in children:
            try:
                child.terminate() # 先礼
            except psutil.NoSuchProcess:
                pass
        parent.terminate()
        
        # 给一点时间让它们死
        gone, alive = psutil.wait_procs(children + [parent], timeout=3)
        
        # 后兵：强制 kill
        for p in alive:
            try:
                p.kill()
                logger.warning(f"Force killed process {p.pid}")
            except Exception:
                pass
    except psutil.NoSuchProcess:
        pass
    except Exception as e:
        logger.error(f"Failed to kill process {pid}: {e}")

async def start_worker_process(port: int, worker_index: int = 0) -> str:
    cmd = [
        "mineru-api",
        "--host", "127.0.0.1",
        "--port", str(port),
        "--device", "cuda"
    ]
    env = os.environ.copy()
    if GPU_IDS:
        gpu_id = GPU_IDS[worker_index % len(GPU_IDS)]
        env["CUDA_VISIBLE_DEVICES"] = gpu_id
        logger.info(f"Worker {worker_index} assigned to GPU {gpu_id}")
    
    # 每个 worker 单独落盘日志（PIPE 很容易吞掉关键报错，甚至输出太多会阻塞导致断连）
    os.makedirs("logs/workers", exist_ok=True)
    stdout_path = f"logs/workers/worker_{port}.stdout.log"
    stderr_path = f"logs/workers/worker_{port}.stderr.log"
    stdout_f = open(stdout_path, "ab", buffering=0)
    stderr_f = open(stderr_path, "ab", buffering=0)

    # 使用 process group，方便后续整组杀掉
    if sys.platform != "win32":
        proc = subprocess.Popen(cmd, env=env, stdout=stdout_f, stderr=stderr_f, preexec_fn=os.setsid)
    else:
        proc = subprocess.Popen(cmd, env=env, stdout=stdout_f, stderr=stderr_f)

    url = f"http://127.0.0.1:{port}"
    worker_registry[url] = {
        "proc": proc,
        "port": port,
        "status": "starting",
        "gpu_id": gpu_id if GPU_IDS else "0",
        "worker_index": worker_index,
        "busy_since": None,
        "current_task_id": None,
        "last_activity_ts": None,
        "inactive_checks": 0,
        "warmed_up": False,
        "stdout_path": stdout_path,
        "stderr_path": stderr_path,
        "stdout_f": stdout_f,
        "stderr_f": stderr_f,
    }
    _inactivity_counters[url] = 0
    return url

async def wait_for_worker_ready(url: str, timeout: int = 120) -> bool:
    start_ts = time.time()
    async with httpx.AsyncClient(timeout=1.0) as client: # 短超时，快速失败重试
        while time.time() - start_ts < timeout:
            try:
                resp = await client.get(f"{url}/docs")
                if resp.status_code == 200:
                    return True
            except:
                pass
            await asyncio.sleep(1) # 没准备好就等1秒
    return False

async def restart_worker(url: str):
    if url not in _restart_locks:
        _restart_locks[url] = asyncio.Lock()
    
    # 如果锁被占用，说明正在重启，直接返回
    if _restart_locks[url].locked():
        return

    async with _restart_locks[url]:
        info = worker_registry.get(url)
        if not info: return

        logger.warning(f"🔄 RESTARTING worker: {url}...")
        info["status"] = "restarting"
        
        # 1. 杀旧进程
        if info.get("proc"):
            await run_in_thread(kill_process_tree, info["proc"].pid)
        # 关闭旧日志句柄
        try:
            if info.get("stdout_f"):
                info["stdout_f"].close()
            if info.get("stderr_f"):
                info["stderr_f"].close()
        except Exception:
            pass
        
        # 2. 稍作等待释放端口
        await asyncio.sleep(2)
        
        # 3. 启动新进程
        try:
            await start_worker_process(info["port"], info["worker_index"])
            if await wait_for_worker_ready(url):
                logger.info(f"✅ Worker {url} recovered.")
                worker_registry[url]["status"] = "idle"
                worker_registry[url]["busy_since"] = None
                worker_registry[url]["current_task_id"] = None
                # 重新加入队列
                worker_queue.put_nowait(url)
            else:
                logger.error(f"❌ Worker {url} failed to start.")
                worker_registry[url]["status"] = "dead"
                # 延迟重试
                asyncio.create_task(_delayed_retry_restart(url))
        except Exception as e:
            logger.error(f"Restart exception: {e}")
            asyncio.create_task(_delayed_retry_restart(url))

async def _delayed_retry_restart(url: str, delay: int = 30):
    await asyncio.sleep(delay)
    await restart_worker(url)

# ================= Business Logic Logic =================

def get_image_mime_type(image_path: str) -> str:
    ext = Path(image_path).suffix.lower()
    return {'.jpg': 'image/jpeg', '.png': 'image/png', '.webp': 'image/webp'}.get(ext, 'image/jpeg')

def _sync_embed_images(md_content: str, extract_path: str, md_file_path: str) -> str:
    """同步版本的图片嵌入逻辑，将被放入线程池运行"""
    pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
    md_dir = os.path.dirname(md_file_path)
    
    def replace_image(match):
        alt, img_path = match.group(1), match.group(2)
        if img_path.startswith('data:image/'): return match.group(0)
        
        full_path = None
        # 搜索逻辑
        candidates = [
            os.path.join(md_dir, img_path),
            os.path.join(extract_path, img_path),
            os.path.join(extract_path, os.path.basename(img_path))
        ]
        for p in candidates:
            if os.path.exists(p) and os.path.isfile(p):
                full_path = p
                break
        
        if not full_path: return match.group(0)

        try:
            with open(full_path, 'rb') as f:
                b64 = base64.b64encode(f.read()).decode('utf-8')
                mime = get_image_mime_type(full_path)
                return f"![{alt}](data:{mime};base64,{b64})"
        except:
            return match.group(0)

    return re.sub(pattern, replace_image, md_content)

# ================= Models =================
class ParseRequest(BaseModel):
    task_id: str
    paper_id: str
    input_pdf: str
    output_md: str
    callback_url: Optional[str] = None
    parse_method: str = "auto"

class MinerUCallback(BaseModel):
    task_id: str
    paper_id: str
    status: str
    md_key: Optional[str] = None
    error_message: Optional[str] = None
    execution_time_ms: int = 0

async def send_callback(url: str, payload: MinerUCallback):
    if not url: return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # 后端期望 "Bearer <token>" 格式
            token = os.getenv("MINERU_INTERNAL_TOKEN", "")
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            resp = await client.post(url, json=payload.model_dump(), headers=headers)
            if resp.status_code != 200:
                logger.warning(f"Callback returned {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Callback failed: {e}")

# ================= Core Pipeline =================
async def run_mineru_pipeline(req: ParseRequest, worker_url: str):
    start_time = time.time()
    tmp_dir = f"/tmp/{req.task_id}"
    need_restart = False
    
    # 标记状态
    worker_registry[worker_url].update({
        "status": f"busy: {req.task_id}",
        "busy_since": start_time,
        "current_task_id": req.task_id,
        "last_activity_ts": start_time,
        "inactive_checks": 0,
    })
    _inactivity_counters[worker_url] = 0

    try:
        # 0. 准备目录 (同步操作放入线程池)
        await run_in_thread(os.makedirs, tmp_dir, exist_ok=True)
        local_pdf_path = os.path.join(tmp_dir, "input.pdf")

        # 1. MinIO 下载 (IO密集，放入线程池)
        if not minio_client: raise Exception("MinIO unavailable")
        
        bucket, obj_name = "rag", req.input_pdf
        if "/" in req.input_pdf:
            bucket, obj_name = req.input_pdf.split("/", 1)
        if not obj_name.startswith("raw/"): obj_name = f"raw/{obj_name}"

        await run_in_thread(minio_client.fget_object, bucket, obj_name, local_pdf_path)

        # 2. 调用 MinerU API
        # 注意：这里是大文件上传，如果不使用 stream 可能内存占用高，但在 AsyncClient 中通常还好
        # 关键是 timeout 要足够长，但不应无限长
        # 首次 warmup 任务允许更长超时（模型下载/初始化）
        warmed_up = bool(worker_registry.get(worker_url, {}).get("warmed_up"))
        effective_timeout = float(TASK_TIMEOUT_SECONDS if warmed_up else max(TASK_TIMEOUT_SECONDS, WARMUP_TIMEOUT_SECONDS))

        async with httpx.AsyncClient(timeout=effective_timeout) as client:
            # 为了防止 open() 阻塞，这里快速读取
            files = {"files": (f"{req.paper_id}.pdf", open(local_pdf_path, "rb"), "application/pdf")}
            data = {"parse_method": req.parse_method, "return_md": "true", "response_format_zip": "true", "return_images": "true"}
            
            logger.info(f"Sending task {req.task_id} to {worker_url}")
            response = await client.post(f"{worker_url}/file_parse", files=files, data=data)
            files["files"][1].close() # 显式关闭
            
            if response.status_code != 200:
                raise Exception(f"Worker Error {response.status_code}")

            # 3. 处理结果 (包含解压、读取、Base64转换 - 全部放入线程池)
            zip_path = os.path.join(tmp_dir, "output.zip")
            # 写入文件是同步IO
            await run_in_thread(lambda: open(zip_path, "wb").write(response.content))
            
            extract_path = os.path.join(tmp_dir, "extracted")
            # 解压是重IO+CPU
            await run_in_thread(shutil.unpack_archive, zip_path, extract_path)
            
            # 查找MD
            md_file_path = None
            for root, _, files in os.walk(extract_path):
                for f in files:
                    if f.endswith(".md"):
                        md_file_path = os.path.join(root, f)
                        break
            
            if not md_file_path: raise Exception("No markdown found")

            # 读取内容
            md_content = await run_in_thread(lambda: open(md_file_path, 'r', encoding='utf-8').read())
            
            # 图片 Base64 处理 (最耗时步骤，必须异步)
            md_content = await run_in_thread(_sync_embed_images, md_content, extract_path, md_file_path)

            # 上传 MinIO
            processed_path = os.path.join(tmp_dir, "processed.md")
            await run_in_thread(lambda: open(processed_path, 'w', encoding='utf-8').write(md_content))
            
            target_bucket, target_path = "rag", req.output_md
            if "/" in req.output_md: target_bucket, target_path = req.output_md.split("/", 1)
            if not target_path.startswith("parsed/"): target_path = f"parsed/{target_path}"
            
            await run_in_thread(minio_client.fput_object, target_bucket, target_path, processed_path)

        # 标记 warmup 完成：至少成功跑通一次 pipeline
        try:
            worker_registry[worker_url]["warmed_up"] = True
        except Exception:
            pass

        # 4. 成功回调
        await send_callback(req.callback_url, MinerUCallback(
            task_id=req.task_id, paper_id=req.paper_id, status="SUCCESS", md_key=req.output_md,
            execution_time_ms=int((time.time() - start_time) * 1000)
        ))

    except (httpx.ConnectError, httpx.ReadError):
        logger.error(f"Task {req.task_id}: Connection failed. Worker died?")
        need_restart = True
        await send_callback(req.callback_url, MinerUCallback(
            task_id=req.task_id, paper_id=req.paper_id, status="FAILED", error_message="Worker Crash"
        ))
    except httpx.TimeoutException:
        logger.error(f"Task {req.task_id}: Timeout.")
        need_restart = True
        await send_callback(req.callback_url, MinerUCallback(
            task_id=req.task_id, paper_id=req.paper_id, status="TIMEOUT", error_message="Timeout"
        ))
    except Exception as e:
        logger.error(f"Task {req.task_id} Failed: {traceback.format_exc()}")
        await send_callback(req.callback_url, MinerUCallback(
            task_id=req.task_id, paper_id=req.paper_id, status="FAILED", error_message=str(e)
        ))
    finally:
        # 清理 (放入线程池)
        if os.path.exists(tmp_dir):
            await run_in_thread(shutil.rmtree, tmp_dir, ignore_errors=True)
        
        if need_restart:
            asyncio.create_task(restart_worker(worker_url))
        else:
            # 重置状态
            worker_registry[worker_url]["status"] = "idle"
            worker_registry[worker_url]["busy_since"] = None
            worker_registry[worker_url]["current_task_id"] = None
            worker_registry[worker_url]["last_activity_ts"] = time.time()
            worker_registry[worker_url]["inactive_checks"] = 0
            _inactivity_counters[worker_url] = 0
            worker_queue.put_nowait(worker_url)

# ================= GPU Activity Detection =================

def _check_gpu_activity(gpu_id: str) -> dict:
    """
    检查指定 GPU 的活动状态
    
    Returns:
        dict: {
            "utilization": int (0-100),
            "memory_used_mb": int,
            "memory_total_mb": int,
            "is_active": bool,  # GPU 利用率 > 5% 视为活跃
            "error": Optional[str]
        }
    """
    try:
        # 使用 nvidia-smi 查询 GPU 状态
        result = subprocess.run(
            [
                "nvidia-smi",
                f"--id={gpu_id}",
                "--query-gpu=utilization.gpu,memory.used,memory.total",
                "--format=csv,noheader,nounits"
            ],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode != 0:
            return {"error": f"nvidia-smi failed: {result.stderr}", "is_active": True}
        
        # 解析输出: "45, 8192, 16384"
        parts = result.stdout.strip().split(",")
        if len(parts) >= 3:
            utilization = int(parts[0].strip())
            memory_used = int(parts[1].strip())
            memory_total = int(parts[2].strip())
            
            return {
                "utilization": utilization,
                "memory_used_mb": memory_used,
                "memory_total_mb": memory_total,
                "is_active": utilization > 5,  # GPU 利用率 > 5% 视为活跃
                "error": None
            }
    except subprocess.TimeoutExpired:
        return {"error": "nvidia-smi timeout", "is_active": True}  # 保守起见，假设活跃
    except FileNotFoundError:
        return {"error": "nvidia-smi not found", "is_active": True}
    except Exception as e:
        return {"error": str(e), "is_active": True}
    
    return {"error": "Unknown error", "is_active": True}


def _check_process_activity(pid: int) -> dict:
    """
    检查进程的活动状态
    
    Returns:
        dict: {
            "cpu_percent": float,
            "memory_mb": float,
            "is_active": bool,  # CPU > 1% 或有子进程活跃
            "child_count": int,
            "error": Optional[str]
        }
    """
    try:
        proc = psutil.Process(pid)

        # psutil 的 cpu_percent 需要“先 prime 一次”，否则会返回 0，导致误判“无活动”
        if pid not in _cpu_primed_pids:
            proc.cpu_percent(interval=None)
            for c in proc.children(recursive=True):
                try:
                    c.cpu_percent(interval=None)
                except Exception:
                    pass
            _cpu_primed_pids.add(pid)
            # 第一次采样不可靠：保守认为活跃，避免被 monitor 误杀
            return {
                "cpu_percent": 0.0,
                "memory_mb": proc.memory_info().rss / (1024 * 1024),
                "is_active": True,
                "child_count": len(proc.children(recursive=True)),
                "error": None,
                "note": "cpu_percent primed",
            }

        # 非阻塞读取（返回自上次调用以来的占比）
        cpu_percent = proc.cpu_percent(interval=0.0)
        memory_mb = proc.memory_info().rss / (1024 * 1024)
        
        # 检查子进程
        children = proc.children(recursive=True)
        child_active = any(
            (c.cpu_percent(interval=0.0) > 1)
            for c in children
            if c.is_running()
        )
        
        is_active = cpu_percent > 1 or child_active
        
        return {
            "cpu_percent": cpu_percent,
            "memory_mb": memory_mb,
            "is_active": is_active,
            "child_count": len(children),
            "error": None
        }
    except psutil.NoSuchProcess:
        return {"error": "Process not found", "is_active": False}
    except Exception as e:
        return {"error": str(e), "is_active": True}


# ================= Improved Health Monitor =================

async def _check_single_worker(url: str, info: dict):
    """
    单独检查一个 worker，智能判断是否真的卡死
    
    改进：
    1. 检查 GPU 利用率
    2. 检查进程 CPU 活动
    3. 只有当 GPU 和进程都不活跃时才判定为卡死
    """
    status = info.get("status", "unknown")
    current_time = time.time()
    
    # 1. 检查进程是否存在
    proc = info.get("proc")
    if proc and not psutil.pid_exists(proc.pid):
        logger.error(f"💀 Worker {url} process vanished!")
        await restart_worker(url)
        return

    # 2. 检查 Idle 状态
    if status == "idle":
        try:
            # 极短超时 ping
            async with httpx.AsyncClient(timeout=3.0) as client:
                await client.get(f"{url}/docs")
        except:
            # 即使是 idle，如果不响应，说明可能卡死或显存泄露
            logger.warning(f"⚠️ Idle worker {url} unresponsive. Restarting.")
            await restart_worker(url)
            return

    # 3. 检查 Busy 状态 (智能超时判断)
    elif status.startswith("busy"):
        busy_since = info.get("busy_since")
        task_id = info.get("current_task_id")
        gpu_id = info.get("gpu_id", "0")
        proc = info.get("proc")
        warmed_up = bool(info.get("warmed_up"))

        # 0) 自愈：busy 但缺少关键字段，说明状态不同步，直接回收
        if not busy_since or not task_id:
            logger.warning(
                f"🧹 Inconsistent busy state for {url}: busy_since={busy_since}, task_id={task_id}. "
                f"Recovering to idle."
            )
            info["status"] = "idle"
            info["busy_since"] = None
            info["current_task_id"] = None
            info["inactive_checks"] = 0
            info["last_activity_ts"] = time.time()
            _inactivity_counters[url] = 0
            worker_queue.put_nowait(url)
            return
        
        if busy_since:
            duration = current_time - busy_since
            
            # 软超时：任务超时时间后开始检查
            SOFT_LIMIT = TASK_TIMEOUT_SECONDS
            # 硬超时：无论如何都要杀（防止无限等待）
            HARD_LIMIT = TASK_TIMEOUT_SECONDS * 3  # 3 倍超时时间

            # 额外：如果长期无活动（GPU/CPU 都低），认为是假 busy 或卡死（更早恢复）
            # 每次 monitor loop 是 10s，因此 6 次约 60s
            INACTIVITY_GRACE_SECONDS = 60
            INACTIVE_CHECKS_THRESHOLD = max(1, int(INACTIVITY_GRACE_SECONDS / 10))
            
            # 异步执行检查（避免阻塞）
            gpu_status = await run_in_thread(_check_gpu_activity, gpu_id)
            proc_status = await run_in_thread(_check_process_activity, proc.pid) if proc else {"is_active": False}
            gpu_active = gpu_status.get("is_active", True)
            proc_active = proc_status.get("is_active", True)

            # 更新活动时间与连续不活跃计数
            if gpu_active or proc_active:
                info["last_activity_ts"] = time.time()
                info["inactive_checks"] = 0
                _inactivity_counters[url] = 0
            else:
                info["inactive_checks"] = int(info.get("inactive_checks") or 0) + 1
                _inactivity_counters[url] = info["inactive_checks"]

            logger.info(
                f"⏱️ Worker {url} task {task_id} duration={duration:.0f}s "
                f"GPU={gpu_status.get('utilization', '?')}% (active={gpu_active}) "
                f"CPU={proc_status.get('cpu_percent', 0.0):.1f}% (active={proc_active}) "
                f"inactive_checks={info['inactive_checks']}"
            )

            # 1) 连续无活动 => 认为是假 busy 或卡死，优先恢复（避免一直 busy）
            #    但首次 warmup 阶段（模型下载/初始化）可能表现为 GPU=0 且 CPU 不高，先给更长宽限期
            if (not warmed_up) and duration < WARMUP_GRACE_SECONDS:
                return

            if info["inactive_checks"] >= INACTIVE_CHECKS_THRESHOLD and duration > INACTIVITY_GRACE_SECONDS:
                logger.error(
                    f"🧊 Worker {url} has no GPU/CPU activity for ~{INACTIVITY_GRACE_SECONDS}s "
                    f"while marked busy on task {task_id}. Restarting worker."
                )
                await restart_worker(url)
                return

            # 2) 超过软超时：只有在无活动时才杀；有活动则继续等，直到硬超时
            if duration > SOFT_LIMIT:
                if gpu_active or proc_active:
                    if duration > HARD_LIMIT:
                        logger.error(
                            f"🔥 Worker {url} exceeded hard limit ({duration:.0f}s > {HARD_LIMIT}s) "
                            f"despite activity. Force killing."
                        )
                        await restart_worker(url)
                else:
                    logger.error(
                        f"💀 Worker {url} exceeded soft limit and is inactive; restarting."
                    )
                    await restart_worker(url)

async def _health_monitor_loop():
    logger.info("Starting Parallel Health Monitor...")
    while True:
        try:
            await asyncio.sleep(10) # 每10秒检查一次
            
            # 使用 gather 并行检查所有 worker，互不阻塞
            tasks = []
            for url, info in list(worker_registry.items()):
                # 忽略正在启动或重启中的
                if info["status"] in ["starting", "restarting"]:
                    continue
                tasks.append(_check_single_worker(url, info))
            
            if tasks:
                await asyncio.gather(*tasks)
                
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Health Monitor Error: {e}")

# ================= Lifecycle =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info(f"Spawning {MINERU_WORKER_COUNT} workers...")
    await asyncio.gather(*[start_worker_process(START_PORT + i, i) for i in range(MINERU_WORKER_COUNT)])
    
    # Wait for ready
    async def wait_single(url):
        if await wait_for_worker_ready(url):
            worker_registry[url]["status"] = "idle"
            worker_queue.put_nowait(url)
            logger.info(f"✅ {url} Ready")
        else:
            worker_registry[url]["status"] = "dead"
            logger.error(f"❌ {url} Failed Start")

    await asyncio.gather(*[wait_single(u) for u in worker_registry])
    
    monitor_task = asyncio.create_task(_health_monitor_loop())
    yield
    # Shutdown
    monitor_task.cancel()
    for info in worker_registry.values():
        if info.get("proc"): kill_process_tree(info["proc"].pid)

app = FastAPI(lifespan=lifespan)

# ================= Routes =================

@app.post("/parse", status_code=202)
async def submit_job(req: ParseRequest, background_tasks: BackgroundTasks):
    callback_url = req.callback_url or MINERU_DEFAULT_CALLBACK_URL
    if not callback_url:
        raise HTTPException(status_code=400, detail="callback_url missing")
    req.callback_url = callback_url

    try:
        worker_url = worker_queue.get_nowait()
    except asyncio.QueueEmpty:
        raise HTTPException(status_code=503, detail="System overload")
    
    # 这里不需要 run_in_thread，因为 run_mineru_pipeline 内部已经优化了
    background_tasks.add_task(run_mineru_pipeline, req, worker_url)
    return {"message": "Accepted", "task_id": req.task_id}

@app.post("/parse_md")
async def parse_pdf_to_markdown(file: UploadFile = File(...), parse_method: str = "auto"):
    """直接解析接口 - 同样必须使用异步IO优化，否则会卡死整个服务"""
    try:
        worker_url = worker_queue.get_nowait()
    except asyncio.QueueEmpty:
        raise HTTPException(status_code=503, detail="System busy")

    temp_id = f"direct_{int(time.time())}"
    tmp_dir = f"/tmp/{temp_id}"
    worker_registry[worker_url]["status"] = f"busy: {temp_id}"
    worker_registry[worker_url]["busy_since"] = time.time()
    
    need_restart = False
    
    try:
        await run_in_thread(os.makedirs, tmp_dir, exist_ok=True)
        local_path = os.path.join(tmp_dir, "input.pdf")
        
        # 异步写文件
        content = await file.read()
        await run_in_thread(lambda: open(local_path, "wb").write(content))
        
        async with httpx.AsyncClient(timeout=float(TASK_TIMEOUT_SECONDS)) as client:
            files = {"files": ("input.pdf", open(local_path, "rb"), "application/pdf")}
            data = {"parse_method": parse_method, "return_md": "true", "response_format_zip": "true", "return_images": "true"}
            
            resp = await client.post(f"{worker_url}/file_parse", files=files, data=data)
            files["files"][1].close()
            
            if resp.status_code != 200: raise HTTPException(500, f"Worker Error: {resp.text}")
            
            # 处理 ZIP (复用异步逻辑)
            zip_path = os.path.join(tmp_dir, "output.zip")
            await run_in_thread(lambda: open(zip_path, "wb").write(resp.content))
            extract_path = os.path.join(tmp_dir, "ext")
            await run_in_thread(shutil.unpack_archive, zip_path, extract_path)
            
            md_path = None
            for r, _, f in os.walk(extract_path):
                for x in f:
                    if x.endswith(".md"): md_path = os.path.join(r, x)
            
            if not md_path: raise HTTPException(500, "No MD found")
            
            raw_md = await run_in_thread(lambda: open(md_path, 'r', encoding='utf-8').read())
            final_md = await run_in_thread(_sync_embed_images, raw_md, extract_path, md_path)
            
            return PlainTextResponse(final_md, media_type="text/markdown; charset=utf-8")

    except Exception as e:
        logger.error(f"Direct parse failed: {e}")
        need_restart = True
        raise HTTPException(500, str(e))
    finally:
        await run_in_thread(shutil.rmtree, tmp_dir, ignore_errors=True)
        if need_restart:
            asyncio.create_task(restart_worker(worker_url))
        else:
            worker_registry[worker_url]["status"] = "idle"
            worker_registry[worker_url]["busy_since"] = None
            worker_queue.put_nowait(worker_url)

@app.get("/health")
def health_check(detailed: bool = False):
    """
    健康检查端点 - 返回所有 Worker 的状态
    
    Args:
        detailed: 是否返回详细的 GPU/进程活动信息（会增加延迟）
    """
    stats = []
    for url, info in worker_registry.items():
        pid = info["proc"].pid if info.get("proc") else None
        is_alive = pid and psutil.pid_exists(pid) if pid else False
        gpu_id = info.get("gpu_id", "0")
        
        worker_stat = {
            "url": url,
            "pid": pid,
            "gpu_id": gpu_id,
            "status": info.get("status", "unknown"),
            "system_alive": is_alive,
            "busy_since": info.get("busy_since"),
            "current_task_id": info.get("current_task_id"),
            "inactive_checks": info.get("inactive_checks", 0),
            "stdout_path": info.get("stdout_path"),
            "stderr_path": info.get("stderr_path"),
        }
        
        # 如果任务正在运行且需要详细信息，添加活动状态
        if detailed and info.get("status", "").startswith("busy") and pid:
            worker_stat["gpu_activity"] = _check_gpu_activity(gpu_id)
            worker_stat["process_activity"] = _check_process_activity(pid)
            
            # 计算运行时长
            busy_since = info.get("busy_since")
            if busy_since:
                worker_stat["running_seconds"] = int(time.time() - busy_since)
        
        stats.append(worker_stat)
    
    return {
        "status": "running",
        "free_workers": worker_queue.qsize(),
        "total_workers": MINERU_WORKER_COUNT,
        "details": stats
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=CONTROLLER_PORT)