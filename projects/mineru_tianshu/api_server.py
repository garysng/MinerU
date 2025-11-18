"""
MinerU Tianshu - API Server
天枢API服务器

提供RESTful API接口用于任务提交、查询和管理
"""
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import tempfile
from pathlib import Path
from loguru import logger
import uvicorn
from typing import Optional
from datetime import datetime
import os
import re
import uuid
from abc import ABC, abstractmethod

from task_db import TaskDB

# 初始化 FastAPI 应用
app = FastAPI(
    title="MinerU Tianshu API",
    description="天枢 - 企业级多GPU文档解析服务",
    version="1.0.0"
)

# 添加 CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 初始化数据库
db = TaskDB()

# 配置输出目录
OUTPUT_DIR = Path('/tmp/mineru_tianshu_output')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ==================== 存储后端抽象层 ====================

class StorageBackend(ABC):
    """存储后端抽象基类"""
    
    @abstractmethod
    def upload_file(self, local_path: str, object_name: str) -> str:
        """
        上传文件到对象存储
        
        Args:
            local_path: 本地文件路径
            object_name: 对象存储中的路径/键名
            
        Returns:
            访问 URL
        """
        pass
    
    @abstractmethod
    def get_url(self, object_name: str) -> str:
        """
        获取对象的访问 URL
        
        Args:
            object_name: 对象存储中的路径/键名
            
        Returns:
            访问 URL
        """
        pass
    
    @abstractmethod
    def check_connection(self) -> bool:
        """检查存储后端连接是否正常"""
        pass


class MinioStorage(StorageBackend):
    """MinIO/腾讯云 COS 存储后端（S3兼容）"""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str, secure: bool = True):
        from minio import Minio
        self.endpoint = endpoint
        self.bucket = bucket
        self.secure = secure
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
    
    def upload_file(self, local_path: str, object_name: str) -> str:
        """上传文件到 MinIO/COS"""
        self.client.fput_object(self.bucket, object_name, local_path)
        return self.get_url(object_name)
    
    def get_url(self, object_name: str) -> str:
        """生成访问 URL"""
        scheme = 'https' if self.secure else 'http'
        return f"{scheme}://{self.endpoint}/{object_name}"
    
    def check_connection(self) -> bool:
        """检查连接"""
        try:
            return self.client.bucket_exists(self.bucket)
        except Exception as e:
            logger.error(f"MinIO connection check failed: {e}")
            return False


class OSSStorage(StorageBackend):
    """阿里云 OSS 存储后端"""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str):
        import oss2
        self.endpoint = endpoint
        self.bucket_name = bucket
        
        # 创建认证对象
        auth = oss2.Auth(access_key, secret_key)
        
        # 创建 Bucket 对象
        self.bucket = oss2.Bucket(auth, endpoint, bucket)
    
    def upload_file(self, local_path: str, object_name: str) -> str:
        """上传文件到阿里云 OSS"""
        self.bucket.put_object_from_file(object_name, local_path)
        return self.get_url(object_name)
    
    def get_url(self, object_name: str) -> str:
        """生成访问 URL"""
        # 阿里云 OSS 公有读 URL 格式
        return f"https://{self.bucket_name}.{self.endpoint}/{object_name}"
    
    def check_connection(self) -> bool:
        """检查连接"""
        try:
            # 尝试获取 bucket 信息
            self.bucket.get_bucket_info()
            return True
        except Exception as e:
            logger.error(f"OSS connection check failed: {e}")
            return False


def create_storage_backend() -> Optional[StorageBackend]:
    """
    根据环境变量创建存储后端实例
    
    环境变量:
        STORAGE_TYPE: 存储类型 (minio/cos/oss)
        
        MinIO/COS:
            MINIO_ENDPOINT: 端点
            MINIO_ACCESS_KEY: Access Key
            MINIO_SECRET_KEY: Secret Key
            MINIO_BUCKET: Bucket 名称
            MINIO_SECURE: 是否使用 HTTPS (true/false)
            
        阿里云 OSS:
            OSS_ENDPOINT: 端点 (如 oss-cn-shanghai.aliyuncs.com)
            OSS_ACCESS_KEY: Access Key ID
            OSS_SECRET_KEY: Access Key Secret
            OSS_BUCKET: Bucket 名称
    
    Returns:
        StorageBackend 实例，如果配置不完整则返回 None
    """
    storage_type = os.getenv('STORAGE_TYPE', 'minio').lower()
    
    try:
        if storage_type in ['minio', 'cos']:
            # MinIO 或腾讯云 COS（S3 兼容）
            endpoint = os.getenv('MINIO_ENDPOINT', '')
            access_key = os.getenv('MINIO_ACCESS_KEY', '')
            secret_key = os.getenv('MINIO_SECRET_KEY', '')
            bucket = os.getenv('MINIO_BUCKET', '')
            secure = os.getenv('MINIO_SECURE', 'true').lower() == 'true'
            
            if not all([endpoint, access_key, secret_key, bucket]):
                logger.warning(f"MinIO/COS storage not configured, image upload will be disabled")
                return None
            
            logger.info(f"✅ Initialized {storage_type.upper()} storage: {endpoint}/{bucket}")
            return MinioStorage(endpoint, access_key, secret_key, bucket, secure)
        
        elif storage_type == 'oss':
            # 阿里云 OSS
            endpoint = os.getenv('OSS_ENDPOINT', '')
            access_key = os.getenv('OSS_ACCESS_KEY', '')
            secret_key = os.getenv('OSS_SECRET_KEY', '')
            bucket = os.getenv('OSS_BUCKET', '')
            
            if not all([endpoint, access_key, secret_key, bucket]):
                logger.warning(f"OSS storage not configured, image upload will be disabled")
                return None
            
            logger.info(f"✅ Initialized OSS storage: {endpoint}/{bucket}")
            return OSSStorage(endpoint, access_key, secret_key, bucket)
        
        else:
            logger.warning(f"Unknown storage type: {storage_type}, image upload will be disabled")
            return None
            
    except ImportError as e:
        logger.error(f"Failed to import storage library: {e}")
        logger.error(f"Please install: pip install minio (for MinIO/COS) or pip install oss2 (for OSS)")
        return None
    except Exception as e:
        logger.error(f"Failed to create storage backend: {e}")
        return None


# 初始化存储后端
storage_backend = create_storage_backend()


def process_markdown_images(md_content: str, image_dir: Path, upload_images: bool = False):
    """
    处理 Markdown 中的图片引用
    
    Args:
        md_content: Markdown 内容
        image_dir: 图片所在目录
        upload_images: 是否上传图片到对象存储并替换链接
        
    Returns:
        处理后的 Markdown 内容
    """
    if not upload_images:
        return md_content
    
    if not storage_backend:
        logger.warning("Storage backend not configured, images will not be uploaded")
        return md_content
    
    try:
        # 查找所有 markdown 格式的图片
        img_pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
        
        def replace_image(match):
            alt_text = match.group(1)
            image_path = match.group(2)
            
            # 构建完整的本地图片路径
            full_image_path = image_dir / Path(image_path).name
            
            if full_image_path.exists():
                # 获取文件后缀
                file_extension = full_image_path.suffix
                # 生成 UUID 作为新文件名
                new_filename = f"{uuid.uuid4()}{file_extension}"
                
                try:
                    # 上传到对象存储
                    object_name = f"images/{new_filename}"
                    storage_url = storage_backend.upload_file(str(full_image_path), object_name)
                    
                    # 返回 HTML 格式的 img 标签
                    return f'<img src="{storage_url}" alt="{alt_text}">'
                except Exception as e:
                    logger.error(f"Failed to upload image to storage: {e}")
                    return match.group(0)  # 上传失败，保持原样
            
            return match.group(0)
        
        # 替换所有图片引用
        new_content = re.sub(img_pattern, replace_image, md_content)
        return new_content
        
    except Exception as e:
        logger.error(f"Error processing markdown images: {e}")
        return md_content  # 出错时返回原内容


@app.get("/")
async def root():
    """API根路径"""
    return {
        "service": "MinerU Tianshu",
        "version": "1.0.0",
        "description": "天枢 - 企业级多GPU文档解析服务",
        "docs": "/docs"
    }


@app.post("/api/v1/tasks/submit")
async def submit_task(
    file: UploadFile = File(..., description="文档文件: PDF/图片(MinerU解析) 或 Office/HTML/文本等(MarkItDown解析)"),
    backend: str = Form('pipeline', description="处理后端: pipeline/vlm-transformers/vlm-vllm-engine"),
    lang: str = Form('ch', description="语言: ch/en/korean/japan等"),
    method: str = Form('auto', description="解析方法: auto/txt/ocr"),
    formula_enable: bool = Form(True, description="是否启用公式识别"),
    table_enable: bool = Form(True, description="是否启用表格识别"),
    priority: int = Form(0, description="优先级，数字越大越优先"),
):
    """
    提交文档解析任务
    
    立即返回 task_id，任务在后台异步处理
    """
    try:
        # 保存上传的文件到临时目录
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix)
        
        # 流式写入文件到磁盘，避免高内存使用
        while True:
            chunk = await file.read(1 << 23)  # 8MB chunks
            if not chunk:
                break
            temp_file.write(chunk)
        
        temp_file.close()
        
        # 创建任务
        task_id = db.create_task(
            file_name=file.filename,
            file_path=temp_file.name,
            backend=backend,
            options={
                'lang': lang,
                'method': method,
                'formula_enable': formula_enable,
                'table_enable': table_enable,
            },
            priority=priority
        )
        
        logger.info(f"✅ Task submitted: {task_id} - {file.filename} (priority: {priority})")
        
        return {
            'success': True,
            'task_id': task_id,
            'status': 'pending',
            'message': 'Task submitted successfully',
            'file_name': file.filename,
            'created_at': datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"❌ Failed to submit task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/tasks/{task_id}")
async def get_task_status(
    task_id: str,
    upload_images: bool = Query(False, description="是否上传图片到MinIO并替换链接（仅当任务完成时有效）")
):
    """
    查询任务状态和详情
    
    当任务完成时，会自动返回解析后的 Markdown 内容（data 字段）
    可选择是否上传图片到 MinIO 并替换为 URL
    """
    task = db.get_task(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    response = {
        'success': True,
        'task_id': task_id,
        'status': task['status'],
        'file_name': task['file_name'],
        'backend': task['backend'],
        'priority': task['priority'],
        'error_message': task['error_message'],
        'created_at': task['created_at'],
        'started_at': task['started_at'],
        'completed_at': task['completed_at'],
        'worker_id': task['worker_id'],
        'retry_count': task['retry_count']
    }
    logger.info(f"✅ Task status: {task['status']} - (result_path: {task['result_path']})")
    
    # 如果任务已完成，尝试返回解析内容
    if task['status'] == 'completed':
        if not task['result_path']:
            # 结果文件已被清理
            response['data'] = None
            response['message'] = 'Task completed but result files have been cleaned up (older than retention period)'
            return response
        
        result_dir = Path(task['result_path'])
        logger.info(f"📂 Checking result directory: {result_dir}")
        
        if result_dir.exists():
            logger.info(f"✅ Result directory exists")
            # 递归查找 Markdown 文件（MinerU 输出结构：task_id/filename/auto/*.md）
            md_files = list(result_dir.rglob('*.md'))
            logger.info(f"📄 Found {len(md_files)} markdown files: {[f.relative_to(result_dir) for f in md_files]}")
            
            if md_files:
                try:
                    # 读取 Markdown 内容
                    md_file = md_files[0]
                    logger.info(f"📖 Reading markdown file: {md_file}")
                    with open(md_file, 'r', encoding='utf-8') as f:
                        md_content = f.read()
                    
                    logger.info(f"✅ Markdown content loaded, length: {len(md_content)} characters")
                    
                    # 查找图片目录（在 markdown 文件的同级目录下）
                    image_dir = md_file.parent / 'images'
                    
                    # 处理图片（如果需要）
                    if upload_images and image_dir.exists():
                        logger.info(f"🖼️  Processing images for task {task_id}, upload_images={upload_images}")
                        md_content = process_markdown_images(md_content, image_dir, upload_images)
                    
                    # 添加 data 字段
                    response['data'] = {
                        'markdown_file': md_file.name,
                        'content': md_content,
                        'images_uploaded': upload_images,
                        'has_images': image_dir.exists() if not upload_images else None
                    }
                    logger.info(f"✅ Response data field added successfully")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to read markdown content: {e}")
                    logger.exception(e)
                    # 读取失败不影响状态查询，只是不返回 data
                    response['data'] = None
            else:
                logger.warning(f"⚠️  No markdown files found in {result_dir}")
        else:
            logger.error(f"❌ Result directory does not exist: {result_dir}")
    elif task['status'] == 'completed':
        logger.warning(f"⚠️  Task completed but result_path is empty")
    else:
        logger.info(f"ℹ️  Task status is {task['status']}, skipping content loading")
    
    return response


@app.delete("/api/v1/tasks/{task_id}")
async def cancel_task(task_id: str):
    """
    取消任务（仅限 pending 状态）
    """
    task = db.get_task(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    if task['status'] == 'pending':
        db.update_task_status(task_id, 'cancelled')
        
        # 删除临时文件
        file_path = Path(task['file_path'])
        if file_path.exists():
            file_path.unlink()
        
        logger.info(f"⏹️  Task cancelled: {task_id}")
        return {
            'success': True,
            'message': 'Task cancelled successfully'
        }
    else:
        raise HTTPException(
            status_code=400, 
            detail=f"Cannot cancel task in {task['status']} status"
        )


@app.get("/api/v1/queue/stats")
async def get_queue_stats():
    """
    获取队列统计信息
    """
    stats = db.get_queue_stats()
    
    return {
        'success': True,
        'stats': stats,
        'total': sum(stats.values()),
        'timestamp': datetime.now().isoformat()
    }


@app.get("/api/v1/queue/tasks")
async def list_tasks(
    status: Optional[str] = Query(None, description="筛选状态: pending/processing/completed/failed"),
    limit: int = Query(100, description="返回数量限制", le=1000)
):
    """
    获取任务列表
    """
    if status:
        tasks = db.get_tasks_by_status(status, limit)
    else:
        # 返回所有任务（需要修改 TaskDB 添加这个方法）
        with db.get_cursor() as cursor:
            cursor.execute('''
                SELECT * FROM tasks 
                ORDER BY created_at DESC 
                LIMIT ?
            ''', (limit,))
            tasks = [dict(row) for row in cursor.fetchall()]
    
    return {
        'success': True,
        'count': len(tasks),
        'tasks': tasks
    }


@app.post("/api/v1/admin/cleanup")
async def cleanup_old_tasks(days: int = Query(7, description="清理N天前的任务")):
    """
    清理旧任务记录（管理接口）
    """
    deleted_count = db.cleanup_old_tasks(days)
    
    logger.info(f"🧹 Cleaned up {deleted_count} old tasks")
    
    return {
        'success': True,
        'deleted_count': deleted_count,
        'message': f'Cleaned up tasks older than {days} days'
    }


@app.post("/api/v1/admin/reset-stale")
async def reset_stale_tasks(timeout_minutes: int = Query(60, description="超时时间（分钟）")):
    """
    重置超时的 processing 任务（管理接口）
    """
    reset_count = db.reset_stale_tasks(timeout_minutes)
    
    logger.info(f"🔄 Reset {reset_count} stale tasks")
    
    return {
        'success': True,
        'reset_count': reset_count,
        'message': f'Reset tasks processing for more than {timeout_minutes} minutes'
    }


@app.get("/api/v1/health")
async def health_check():
    """
    健康检查接口
    """
    try:
        # 检查数据库连接
        stats = db.get_queue_stats()
        
        return {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'connected',
            'queue_stats': stats
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                'status': 'unhealthy',
                'error': str(e)
            }
        )


if __name__ == '__main__':
    # 从环境变量读取端口，默认为8000
    api_port = int(os.getenv('API_PORT', '8000'))
    
    logger.info("🚀 Starting MinerU Tianshu API Server...")
    logger.info(f"📖 API Documentation: http://localhost:{api_port}/docs")
    
    uvicorn.run(
        app, 
        host='0.0.0.0', 
        port=api_port,
        log_level='info'
    )

