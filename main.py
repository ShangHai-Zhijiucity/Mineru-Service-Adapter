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
from typing import Optional, Dict
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks, HTTPException, File, UploadFile
from fastapi.responses import Response
from urllib.parse import quote
from pydantic import BaseModel
from minio import Minio
from loguru import logger

# ================= Configuration =================
# 基础配置
MINERU_WORKER_COUNT = int(os.getenv("MINERU_WORKER_COUNT", "1"))
START_PORT = int(os.getenv("START_PORT", "6000"))
GPU_ID = os.getenv("CUDA_VISIBLE_DEVICES", "0")
CONTROLLER_PORT = int(os.getenv("CONTROLLER_PORT", "58001"))

# 任务超时设置 (秒) - 超过此时限任务会被标记失败，且 Worker 可能会被强制重启
TASK_TIMEOUT_SECONDS = int(os.getenv("TASK_TIMEOUT_SECONDS", "300"))

# MinIO 配置
MINIO_ENDPOINT_RAW = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"

# 回调地址（可选默认值，用于未传入 callback_url 时的兜底）
MINERU_DEFAULT_CALLBACK_URL = os.getenv("MINERU_DEFAULT_CALLBACK_URL", "")

# 处理 MINIO_ENDPOINT：移除协议前缀（http:// 或 https://）
# MinIO 客户端需要 host:port 格式，不包含协议
MINIO_ENDPOINT = MINIO_ENDPOINT_RAW.replace("http://", "").replace("https://", "")

# 日志配置
os.makedirs("logs", exist_ok=True)
logger.add("logs/controller_{time}.log", rotation="100 MB", retention="7 days", level="INFO")

# ================= Global State =================
# 资源池：存放可用的 Worker URL
worker_queue: asyncio.Queue = asyncio.Queue()

# 注册表：存放 Worker 的元数据 (PID, Port, Process Object)
# 结构: { "http://127.0.0.1:6000": { "proc": subprocess.Popen, "port": 6000, "status": "idle" } }
worker_registry: Dict[str, dict] = {}

# MinIO 客户端
try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
except Exception as e:
    logger.error(f"MinIO init failed immediately: {e}")
    minio_client = None

# ================= Helper Functions =================

def kill_process_tree(pid: int):
    """
    递归杀死进程树，确保 CUDA 子进程被彻底清理
    """
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        for child in children:
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        parent.kill()
        logger.info(f"Killed process tree for PID {pid}")
    except psutil.NoSuchProcess:
        pass
    except Exception as e:
        logger.error(f"Failed to kill process {pid}: {e}")

async def start_worker_process(port: int) -> str:
    """
    启动单个 MinerU Worker 进程
    """
    cmd = [
        "mineru-api",
        "--host", "127.0.0.1",
        "--port", str(port),
        "--device", "cuda"
    ]
    # 继承当前环境变量，确保 CUDA_VISIBLE_DEVICES 传递下去
    env = os.environ.copy()
    
    # 使用 subprocess 启动
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    url = f"http://127.0.0.1:{port}"
    
    # 注册到全局状态
    worker_registry[url] = {
        "proc": proc,
        "port": port,
        "status": "starting"
    }
    logger.info(f"Worker started at {url} (PID: {proc.pid})")
    return url

async def wait_for_worker_ready(url: str, timeout: int = 60) -> bool:
    """
    轮询等待 Worker 启动完成
    """
    async with httpx.AsyncClient() as client:
        start_ts = time.time()
        while time.time() - start_ts < timeout:
            try:
                resp = await client.get(f"{url}/docs", timeout=1.0)
                if resp.status_code == 200:
                    return True
            except:
                pass
            await asyncio.sleep(1)
    return False

async def restart_worker(url: str):
    """
    [关键] 自动重启 Worker 的逻辑
    """
    info = worker_registry.get(url)
    if not info:
        logger.error(f"Cannot restart unknown worker: {url}")
        return

    logger.warning(f"Initiating restart for worker: {url}...")
    
    # 1. 标记状态
    info["status"] = "restarting"
    
    # 2. 清理旧进程
    if info.get("proc"):
        kill_process_tree(info["proc"].pid)
    
    # 3. 重新启动进程
    port = info["port"]
    # 稍微等待端口释放
    await asyncio.sleep(2)
    
    try:
        # 重新启动并更新注册表
        await start_worker_process(port)
        
        # 4. 等待就绪
        is_ready = await wait_for_worker_ready(url)
        if is_ready:
            logger.info(f"Worker {url} recovered successfully. Adding back to pool.")
            worker_registry[url]["status"] = "idle"
            worker_queue.put_nowait(url)
        else:
            logger.error(f"Worker {url} failed to start after restart.")
            worker_registry[url]["status"] = "dead"
            
    except Exception as e:
        logger.error(f"Fatal error restarting worker {url}: {e}")

# ================= Models =================
class ParseRequest(BaseModel):
    task_id: str
    paper_id: str
    input_pdf: str      # MinIO path: bucket/path/to/file.pdf
    output_md: str      # MinIO path: bucket/path/to/output.md
    callback_url: Optional[str] = None
    parse_method: str = "auto"

class MinerUCallback(BaseModel):
    task_id: str
    paper_id: str
    status: str
    md_key: Optional[str] = None
    error_message: Optional[str] = None
    execution_time_ms: int = 0

# ================= Core Pipeline =================
async def send_callback(url: str, payload: MinerUCallback):
    """
    发送回调到后端，需要携带内部认证 token
    """
    try:
        # 从环境变量获取内部 token
        internal_token = os.getenv("MINERU_INTERNAL_TOKEN", "")
        if not internal_token:
            logger.warning("MINERU_INTERNAL_TOKEN not set, callback may fail authentication")
        
        # 设置 Authorization header
        headers = {
            "Authorization": f"Bearer {internal_token}"
        }
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(url, json=payload.model_dump(), headers=headers)
            logger.info(f"Callback sent for {payload.task_id}: {payload.status}")
    except Exception as e:
        logger.error(f"Callback failed for {payload.task_id}: {e}")

def get_image_mime_type(image_path: str) -> str:
    """
    根据文件扩展名返回对应的 MIME 类型
    """
    ext = Path(image_path).suffix.lower()
    mime_map = {
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.gif': 'image/gif',
        '.webp': 'image/webp',
        '.svg': 'image/svg+xml',
        '.bmp': 'image/bmp'
    }
    return mime_map.get(ext, 'image/jpeg')

def embed_images_as_base64(md_content: str, extract_path: str, md_file_path: str) -> str:
    """
    将 Markdown 中的图片引用转换为 base64 嵌入格式
    
    Args:
        md_content: Markdown 文件内容
        extract_path: ZIP 解压后的根目录路径
        md_file_path: Markdown 文件的完整路径（用于解析相对路径）
        
    Returns:
        处理后的 Markdown 内容（图片已转换为 base64）
    """
    # 匹配 Markdown 中的图片引用: ![](path/to/image.jpg) 或 ![alt](path/to/image.jpg)
    # 支持相对路径和绝对路径
    pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
    
    # Markdown 文件所在目录
    md_dir = os.path.dirname(md_file_path)
    
    def replace_image(match):
        alt_text = match.group(1)
        image_path = match.group(2)
        
        # 跳过已经是 base64 的图片（data:image/...）
        if image_path.startswith('data:image/'):
            return match.group(0)
        
        # 构建完整的图片文件路径
        if os.path.isabs(image_path):
            full_image_path = image_path
        else:
            # 相对路径：相对于 Markdown 文件所在目录
            # 尝试多个可能的路径
            possible_paths = [
                os.path.join(md_dir, image_path),  # 相对于 Markdown 文件目录
                os.path.join(md_dir, image_path.lstrip('./')),  # 去掉 ./ 前缀
                os.path.join(extract_path, image_path),  # 相对于解压根目录
                os.path.join(extract_path, image_path.lstrip('./')),  # 去掉 ./ 前缀
            ]
            
            # 如果还是找不到，尝试递归搜索整个extract_path目录
            full_image_path = None
            for path in possible_paths:
                normalized_path = os.path.normpath(path)
                if os.path.exists(normalized_path) and os.path.isfile(normalized_path):
                    full_image_path = normalized_path
                    break
            
            # 如果常规路径都找不到，尝试递归搜索
            if not full_image_path:
                image_filename = os.path.basename(image_path)
                logger.debug(f"Trying recursive search for image: {image_filename}")
                for root, dirs, files in os.walk(extract_path):
                    if image_filename in files:
                        full_image_path = os.path.join(root, image_filename)
                        logger.info(f"Found image via recursive search: {full_image_path}")
                        break
            
            if not full_image_path:
                # 列出extract_path下的所有图片文件，用于调试
                all_images = []
                for root, dirs, files in os.walk(extract_path):
                    for f in files:
                        if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg')):
                            rel_path = os.path.relpath(os.path.join(root, f), extract_path)
                            all_images.append(rel_path)
                
                logger.warning(
                    f"Image not found: {image_path} (searched from {md_dir}). "
                    f"Available images in extract_path: {all_images[:10]}..."  # 只显示前10个
                )
                return match.group(0)
        
        try:
            # 读取图片文件并转换为 base64
            with open(full_image_path, 'rb') as img_file:
                image_data = img_file.read()
                base64_data = base64.b64encode(image_data).decode('utf-8')
                
                # 获取 MIME 类型
                mime_type = get_image_mime_type(full_image_path)
                
                # 构建 base64 数据 URI
                data_uri = f"data:{mime_type};base64,{base64_data}"
                
                logger.info(f"Embedded image as base64: {image_path} -> {len(image_data)} bytes")
                return f"![{alt_text}]({data_uri})"
        except Exception as e:
            logger.error(f"Failed to embed image {image_path}: {e}")
            return match.group(0)
    
    # 替换所有图片引用
    processed_content = re.sub(pattern, replace_image, md_content)
    return processed_content

async def run_mineru_pipeline(req: ParseRequest, worker_url: str):
    start_time = time.time()
    tmp_dir = f"/tmp/{req.task_id}"
    os.makedirs(tmp_dir, exist_ok=True)
    local_pdf_path = os.path.join(tmp_dir, "input.pdf")
    
    # 标记状态：需要重启 Worker
    need_restart = False
    
    # 更新注册表状态
    if worker_url in worker_registry:
        worker_registry[worker_url]["status"] = f"busy: {req.task_id}"

    try:
        # 1. MinIO 下载
        if not minio_client:
            raise Exception("MinIO client unavailable")
        
        try:
            # 从环境变量或配置获取bucket名称
            bucket = "pdf-raw"  # 默认bucket
            obj_name = req.input_pdf
            
            # 如果包含 "/", 则第一部分可能是bucket名
            if "/" in req.input_pdf:
                parts = req.input_pdf.split("/", 1)
                if len(parts) == 2:
                    bucket, obj_name = parts
            
            logger.info(f"Downloading from MinIO: bucket={bucket}, object={obj_name}")
            minio_client.fget_object(bucket, obj_name, local_pdf_path)
        except Exception as e:
            raise Exception(f"MinIO Download failed: {e}")

        # 2. 调用 MinerU API
        logger.info(f"Task {req.task_id} processing on {worker_url}")
        
        async with httpx.AsyncClient(timeout=float(TASK_TIMEOUT_SECONDS)) as client:
            with open(local_pdf_path, "rb") as f:
                files = {"files": (f"{req.paper_id}.pdf", f, "application/pdf")}
                data = {
                    "parse_method": req.parse_method,
                    "return_md": "true",
                    "output_dir": "./output",
                    "response_format_zip": "true",
                    "return_images": "true",
                    "formula_enable": "true",
                    "table_enable": "true" 
                }
                
                # 发送请求
                response = await client.post(f"{worker_url}/file_parse", files=files, data=data)
                
                if response.status_code != 200:
                    raise Exception(f"Worker returned {response.status_code}: {response.text}")
                
                # 3. 处理结果
                zip_path = os.path.join(tmp_dir, "output.zip")
                with open(zip_path, "wb") as zf:
                    zf.write(response.content)
                
                extract_path = os.path.join(tmp_dir, "extracted")
                shutil.unpack_archive(zip_path, extract_path)
                
                # 记录解压后的目录结构（用于调试）
                logger.debug(f"Extracted ZIP to: {extract_path}")
                for root, dirs, files in os.walk(extract_path):
                    level = root.replace(extract_path, '').count(os.sep)
                    indent = ' ' * 2 * level
                    logger.debug(f"{indent}{os.path.basename(root)}/")
                    subindent = ' ' * 2 * (level + 1)
                    for file in files[:5]:  # 只显示前5个文件
                        logger.debug(f"{subindent}{file}")
                    if len(files) > 5:
                        logger.debug(f"{subindent}... ({len(files) - 5} more files)")
                
                # 查找 Markdown 文件
                md_file_path = None
                for root, _, files in os.walk(extract_path):
                    for file in files:
                        if file.endswith(".md"):
                            md_file_path = os.path.join(root, file)
                            break
                    if md_file_path:
                        break
                
                if not md_file_path:
                    raise Exception("No markdown file found in output")
                
                logger.info(f"Found Markdown file: {md_file_path}")
                
                # 读取 Markdown 内容
                with open(md_file_path, 'r', encoding='utf-8') as f:
                    md_content = f.read()
                
                # 将图片转换为 base64 并嵌入到 Markdown 中
                logger.info(f"Processing images in Markdown file: {md_file_path}")
                md_content_with_base64 = embed_images_as_base64(md_content, extract_path, md_file_path)
                
                # 将处理后的 Markdown 保存到临时文件
                processed_md_path = os.path.join(tmp_dir, "processed.md")
                with open(processed_md_path, 'w', encoding='utf-8') as f:
                    f.write(md_content_with_base64)
                
                # 上传处理后的 Markdown 文件到 MinIO（只上传 Markdown，不上传图片）
                target_bucket = "pdf-parsed"  # 默认bucket
                target_path = req.output_md
                
                if "/" in req.output_md:
                    parts = req.output_md.split("/", 1)
                    if len(parts) == 2:
                        target_bucket, target_path = parts
                
                logger.info(f"Uploading processed Markdown to MinIO: bucket={target_bucket}, key={target_path}")
                minio_client.fput_object(target_bucket, target_path, processed_md_path)
                logger.info(f"Markdown file uploaded successfully (images embedded as base64)")

        # 4. 成功回调
        exec_time = int((time.time() - start_time) * 1000)
        await send_callback(req.callback_url, MinerUCallback(
            task_id=req.task_id, paper_id=req.paper_id, status="SUCCESS",
            md_key=req.output_md, execution_time_ms=exec_time
        ))

    except httpx.ConnectError:
        # [关键] 连接失败，意味着 Worker 进程可能挂了
        logger.critical(f"Task {req.task_id}: Connection refused to {worker_url}. Worker likely crashed.")
        need_restart = True
        await send_callback(req.callback_url, MinerUCallback(
            task_id=req.task_id, paper_id=req.paper_id, status="FAILED",
            error_message="Worker Connection Error", execution_time_ms=int((time.time()-start_time)*1000)
        ))

    except httpx.TimeoutException:
        # 超时，也建议重启，防止显存未释放
        logger.error(f"Task {req.task_id}: Timeout after {TASK_TIMEOUT_SECONDS}s.")
        need_restart = True
        await send_callback(req.callback_url, MinerUCallback(
            task_id=req.task_id, paper_id=req.paper_id, status="TIMEOUT",
            error_message="Processing Timeout", execution_time_ms=int((time.time()-start_time)*1000)
        ))

    except Exception as e:
        # 普通逻辑错误，Worker 应该是好的
        logger.error(f"Task {req.task_id}: Failed. Error: {e}")
        await send_callback(req.callback_url, MinerUCallback(
            task_id=req.task_id, paper_id=req.paper_id, status="FAILED",
            error_message=str(e), execution_time_ms=int((time.time()-start_time)*1000)
        ))

    finally:
        # 清理临时文件
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        
        # 资源归还或重启
        if need_restart:
            # 异步触发重启，不阻塞当前流程
            asyncio.create_task(restart_worker(worker_url))
        else:
            # 正常归还
            logger.info(f"Releasing worker {worker_url}")
            if worker_url in worker_registry:
                worker_registry[worker_url]["status"] = "idle"
            worker_queue.put_nowait(worker_url)

# ================= Lifecycle =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Startup
    logger.info(f"Controller starting. Spawning {MINERU_WORKER_COUNT} workers...")
    
    for i in range(MINERU_WORKER_COUNT):
        port = START_PORT + i
        await start_worker_process(port)
        
    # 等待所有 Worker 就绪
    logger.info("Waiting for workers to come online...")
    active_count = 0
    for url in worker_registry:
        is_ready = await wait_for_worker_ready(url, timeout=30)
        if is_ready:
            worker_queue.put_nowait(url)
            worker_registry[url]["status"] = "idle"
            active_count += 1
            logger.info(f"{url} is ready.")
        else:
            logger.error(f"{url} failed to start within timeout.")
            worker_registry[url]["status"] = "dead"

    logger.info(f"System ready. {active_count}/{MINERU_WORKER_COUNT} workers available.")
    yield
    
    # 2. Shutdown
    logger.info("Shutting down system...")
    for url, info in worker_registry.items():
        if info.get("proc"):
            kill_process_tree(info["proc"].pid)
    logger.info("All workers terminated.")

app = FastAPI(title="MinerU Controller Pro", lifespan=lifespan)

# ================= API Routes =================

@app.post("/parse", status_code=202)
async def submit_job(req: ParseRequest, background_tasks: BackgroundTasks):
    """
    提交解析任务。如果系统满载，返回 503。
    """
    callback_url = req.callback_url or MINERU_DEFAULT_CALLBACK_URL
    if not callback_url:
        raise HTTPException(status_code=400, detail="callback_url is required (or set MINERU_DEFAULT_CALLBACK_URL).")
    req.callback_url = callback_url

    try:
        worker_url = worker_queue.get_nowait()
    except asyncio.QueueEmpty:
        raise HTTPException(status_code=503, detail="System overload. Please try again later.")
    
    logger.info(f"Task {req.task_id} accepted. Assigned to {worker_url}")
    background_tasks.add_task(run_mineru_pipeline, req, worker_url)
    return {"message": "Task accepted", "task_id": req.task_id}

@app.post("/test/direct_parse")
async def debug_direct_parse(file: UploadFile = File(...)):
    """
    测试接口：直接上传文件并返回完整的ZIP结果（包含Markdown和所有资源文件）
    """
    try:
        worker_url = worker_queue.get_nowait()
    except asyncio.QueueEmpty:
        raise HTTPException(status_code=503, detail="System busy")

    temp_id = f"debug_{int(time.time())}"
    tmp_dir = f"/tmp/{temp_id}"
    os.makedirs(tmp_dir, exist_ok=True)
    local_path = os.path.join(tmp_dir, "input.pdf")
    
    need_restart = False

    try:
        with open(local_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
            
        async with httpx.AsyncClient(timeout=float(TASK_TIMEOUT_SECONDS)) as client:
             with open(local_path, "rb") as f:
                files = {"files": ("input.pdf", f, "application/pdf")}
                # 确保请求ZIP格式输出
                data = {
                    "parse_method": "auto", 
                    "return_md": "true", 
                    "output_dir": "./output", 
                    "response_format_zip": "true"  # 关键：要求返回ZIP格式
                }
                resp = await client.post(f"{worker_url}/file_parse", files=files, data=data)
                
                if resp.status_code != 200:
                    raise HTTPException(status_code=resp.status_code, detail=resp.text)
                
                # 验证返回的是ZIP文件
                zip_content = resp.content
                
                # 验证ZIP文件格式（检查ZIP文件头）
                if not zip_content.startswith(b'PK'):
                    # 如果不是ZIP格式，可能是Worker返回了其他格式，尝试重新打包
                    logger.warning(f"Worker returned non-ZIP content, content-type: {resp.headers.get('content-type')}")
                    logger.warning(f"Response preview: {zip_content[:100]}")
                    
                    # 如果返回的是Markdown文本，将其打包成ZIP
                    if resp.headers.get('content-type', '').startswith('text/'):
                        import zipfile
                        import io
                        zip_buffer = io.BytesIO()
                        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                            zip_file.writestr("output.md", zip_content)
                        zip_content = zip_buffer.getvalue()
                        logger.info("Repackaged Markdown content as ZIP")
                    else:
                        raise HTTPException(
                            status_code=500, 
                            detail=f"Worker returned unexpected format. Expected ZIP, got: {resp.headers.get('content-type')}"
                        )
                
                # 验证ZIP文件可以正常解压（可选，用于调试）
                try:
                    import zipfile
                    import io
                    zip_file = zipfile.ZipFile(io.BytesIO(zip_content))
                    file_list = zip_file.namelist()
                    logger.info(f"ZIP contains {len(file_list)} files: {file_list[:5]}...")
                    zip_file.close()
                except Exception as e:
                    logger.warning(f"ZIP validation failed (but continuing): {e}")
                
                filename_str = f"result_{file.filename or 'output'}.zip"
                safe_filename = quote(filename_str)
                
                return Response(
                    content=zip_content, 
                    media_type="application/zip", 
                    headers={
                        "Content-Disposition": f"attachment; filename*=utf-8''{safe_filename}",
                        "Content-Type": "application/zip"
                    }
                )
                                
    except httpx.ConnectError:
        need_restart = True
        raise HTTPException(status_code=500, detail="Worker crashed during processing")
    except httpx.TimeoutException:
        need_restart = True
        raise HTTPException(status_code=504, detail="Processing timeout")
    except Exception as e:
        # 打印错误堆栈方便调试
        logger.exception("Debug Parse Failed") 
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        if need_restart:
            asyncio.create_task(restart_worker(worker_url))
        else:
            worker_queue.put_nowait(worker_url)

@app.get("/health")
def health_check():
    """
    详细的健康检查
    """
    stats = []
    for url, info in worker_registry.items():
        pid = info["proc"].pid if info.get("proc") else None
        
        # 检查进程是否实际存活
        is_alive = False
        if pid and psutil.pid_exists(pid):
            is_alive = True
            
        stats.append({
            "url": url,
            "pid": pid,
            "status": info.get("status", "unknown"),
            "system_alive": is_alive
        })
        
    return {
        "status": "running",
        "free_workers": worker_queue.qsize(),
        "total_workers": MINERU_WORKER_COUNT,
        "details": stats
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=CONTROLLER_PORT)