#!/usr/bin/env python3
"""
完整的阿里云 OSS 工作流程测试脚本

工作流程：
1. 上传本地 PDF 文件到阿里云 OSS
2. 通过 API 提交任务（使用 OSS 对象路径）
3. 等待任务完成
4. 获取并展示结果
5. （可选）自动上传图片到 OSS 并下载到本地

环境变量配置：
- OSS_ENDPOINT: OSS Endpoint（如 oss-cn-beijing.aliyuncs.com）
- OSS_ACCESS_KEY: Access Key ID
- OSS_SECRET_KEY: Access Key Secret
- OSS_BUCKET: Bucket 名称

使用方法：
    # 运行完整流程
    python3 test_full_oss_workflow.py
    
    # 命令行模式
    python3 test_full_oss_workflow.py upload <local_file> [object_key]
    python3 test_full_oss_workflow.py submit <object_key> [file_name]
    python3 test_full_oss_workflow.py full <local_file> [object_key]
"""

import asyncio
import aiohttp
import json
import os
import sys
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime

# ==================== 配置 ====================

API_BASE_URL = os.getenv('API_BASE_URL', 'http://localhost:8000')  # API 服务地址

# 本地文件配置
LOCAL_FILE_PATH = "./1.pdf"  # 要上传的本地文件
OBJECT_KEY_PREFIX = "test_uploads"  # OSS 中的目录前缀

# 任务参数
BACKEND = "http-client"
LANG = "ch"
METHOD = "auto"
FORMULA_ENABLE = True
TABLE_ENABLE = True
PRIORITY = 0

# 结果下载选项
DOWNLOAD_IMAGES = True  # 是否下载图片到本地
OUTPUT_DIR = "./full_oss_workflow_output"  # 本地输出目录


# ==================== 存储上传器 ====================

class OSSUploader:
    """阿里云 OSS 上传器"""
    
    def __init__(self):
        try:
            import oss2
            self.oss2 = oss2
        except ImportError:
            raise ImportError("请安装 oss2: pip install oss2")
        
        # 从环境变量读取配置
        self.endpoint = os.getenv('OSS_ENDPOINT')
        self.access_key = os.getenv('OSS_ACCESS_KEY')
        self.secret_key = os.getenv('OSS_SECRET_KEY')
        self.bucket_name = os.getenv('OSS_BUCKET')
        
        if not all([self.endpoint, self.access_key, self.secret_key, self.bucket_name]):
            raise ValueError(
                "缺少 OSS 配置，请设置环境变量：\n"
                "  OSS_ENDPOINT, OSS_ACCESS_KEY, OSS_SECRET_KEY, OSS_BUCKET"
            )
        
        # 创建 OSS 客户端
        auth = oss2.Auth(self.access_key, self.secret_key)
        self.bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)
        
        print(f"✅ OSS 客户端初始化成功")
        print(f"   Endpoint: {self.endpoint}")
        print(f"   Bucket: {self.bucket_name}")
    
    def upload_file(self, local_path: str, object_key: str) -> str:
        """上传文件到阿里云 OSS"""
        local_path_obj = Path(local_path)
        
        if not local_path_obj.exists():
            raise FileNotFoundError(f"本地文件不存在: {local_path}")
        
        file_size = local_path_obj.stat().st_size
        
        print(f"📤 上传文件到 OSS...")
        print(f"   本地: {local_path} ({file_size} bytes)")
        print(f"   目标: {object_key}")
        
        # 上传文件
        result = self.bucket.put_object_from_file(object_key, str(local_path))
        
        # 构建 URL
        url = f"https://{self.bucket_name}.{self.endpoint}/{object_key}"
        
        print(f"✅ 上传成功！")
        print(f"   ETag: {result.etag}")
        print(f"   URL: {url}")
        
        return url


# ==================== API 客户端 ====================

class TianshuClient:
    """MinerU Tianshu API 客户端"""
    
    def __init__(self, base_url: str = API_BASE_URL):
        self.base_url = base_url.rstrip('/')
    
    async def submit_task_by_oss(
        self,
        session: aiohttp.ClientSession,
        object_name: str,
        file_name: str = None,
        backend: str = "pipeline",
        lang: str = "ch",
        method: str = "auto",
        formula_enable: bool = True,
        table_enable: bool = True,
        priority: int = 0
    ) -> dict:
        """通过 OSS 对象路径提交任务"""
        url = f"{self.base_url}/api/v1/tasks/submit_by_oss"
        
        payload = {
            'object_name': object_name,
            'backend': backend,
            'lang': lang,
            'method': method,
            'formula_enable': formula_enable,
            'table_enable': table_enable,
            'priority': priority
        }
        
        if file_name:
            payload['file_name'] = file_name
        
        print(f"📤 提交任务到 API")
        print(f"   Object: {object_name}")
        if file_name:
            print(f"   File name: {file_name}")
        print(f"   Backend: {backend}")
        
        async with session.post(url, json=payload) as resp:
            status = resp.status
            result = await resp.json()
            
            if status == 200 and result.get('success'):
                print(f"✅ 任务提交成功！")
                print(f"   Task ID: {result['task_id']}")
                print(f"   Status: {result['status']}")
                return result
            else:
                error_detail = result.get('detail', 'Unknown error')
                raise Exception(f"任务提交失败: {error_detail}")
    
    async def get_task_status(
        self,
        session: aiohttp.ClientSession,
        task_id: str,
        upload_images: bool = False
    ) -> dict:
        """查询任务状态"""
        url = f"{self.base_url}/api/v1/tasks/{task_id}"
        params = {'upload_images': str(upload_images).lower()}
        
        async with session.get(url, params=params) as resp:
            return await resp.json()
    
    async def wait_for_task(
        self,
        session: aiohttp.ClientSession,
        task_id: str,
        poll_interval: float = 2.0,
        timeout: float = 600.0
    ) -> dict:
        """等待任务完成"""
        print(f"⏳ 等待任务 {task_id} 完成...")
        
        elapsed = 0.0
        while elapsed < timeout:
            result = await self.get_task_status(session, task_id, upload_images=False)
            status = result['status']
            
            if status == 'completed':
                print(f"✅ 任务完成！")
                return result
            elif status == 'failed':
                error = result.get('error_message', 'Unknown error')
                raise Exception(f"任务失败: {error}")
            elif status == 'cancelled':
                raise Exception("任务已取消")
            
            # 显示进度
            print(f"   Status: {status} (耗时: {elapsed:.1f}s)")
            
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        
        raise TimeoutError(f"任务在 {timeout}s 内未完成")


# ==================== 辅助函数 ====================

def generate_object_key(local_path: str, prefix: str = OBJECT_KEY_PREFIX) -> str:
    """生成 OSS 对象键（带时间戳）"""
    filename = Path(local_path).name
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if prefix:
        return f"{prefix}/{timestamp}_{filename}"
    else:
        return f"{timestamp}_{filename}"


def detect_storage_type(url: str) -> str:
    """
    检测 URL 来自哪种对象存储
    
    Returns:
        'oss', 'cos', 'minio', 'unknown'
    """
    from urllib.parse import urlparse
    parsed = urlparse(url)
    hostname = parsed.hostname or ''
    
    if 'aliyuncs.com' in hostname or 'oss-' in hostname:
        return 'oss'
    elif 'myqcloud.com' in hostname or '.cos.' in hostname:
        return 'cos'
    else:
        return 'unknown'


def parse_oss_url(url: str) -> tuple:
    """
    解析阿里云 OSS URL
    
    Returns:
        (bucket, endpoint, object_key)
    """
    from urllib.parse import urlparse
    parsed = urlparse(url)
    hostname = parsed.hostname or ''
    
    # URL 格式: https://bucket.oss-region.aliyuncs.com/path/to/object
    if '.oss-' in hostname:
        parts = hostname.split('.oss-')
        bucket = parts[0]
        endpoint = f"oss-{parts[1]}"
        object_key = parsed.path.lstrip('/')
        return bucket, endpoint, object_key
    
    return None, None, None


def download_from_oss(url: str, local_path: Path) -> bool:
    """
    使用 OSS SDK 下载文件（支持私有 Bucket）
    
    Args:
        url: OSS 文件 URL
        local_path: 本地保存路径
        
    Returns:
        是否下载成功
    """
    try:
        import oss2
    except ImportError:
        print(f"   ⚠️  oss2 library not found, install it: pip install oss2")
        return False
    
    # 解析 URL
    bucket_name, endpoint, object_key = parse_oss_url(url)
    if not all([bucket_name, endpoint, object_key]):
        print(f"   ⚠️  Failed to parse OSS URL: {url}")
        return False
    
    # 从环境变量读取凭证
    access_key = os.getenv('OSS_ACCESS_KEY', '')
    secret_key = os.getenv('OSS_SECRET_KEY', '')
    
    if not access_key or not secret_key:
        print(f"   ⚠️  OSS credentials not found in environment variables")
        print(f"       Set OSS_ACCESS_KEY and OSS_SECRET_KEY")
        return False
    
    try:
        # 创建 OSS 客户端
        auth = oss2.Auth(access_key, secret_key)
        bucket = oss2.Bucket(auth, endpoint, bucket_name)
        
        # 下载文件
        bucket.get_object_to_file(object_key, str(local_path))
        return True
        
    except Exception as e:
        print(f"   ❌ OSS download failed: {e}")
        return False


async def download_images_to_local(
    session: aiohttp.ClientSession,
    md_content: str,
    output_dir: Path
) -> Tuple[str, int]:
    """
    从对象存储下载图片到本地，并替换 Markdown 中的链接
    支持：
    - 阿里云 OSS（使用 SDK，支持私有 Bucket）
    - 腾讯云 COS（HTTP 直接下载）
    - MinIO（HTTP 直接下载）
    
    Args:
        session: aiohttp 会话
        md_content: Markdown 内容
        output_dir: 本地输出目录
        
    Returns:
        (updated_md_content, downloaded_count) 元组
    """
    import re
    from urllib.parse import urlparse
    
    # 创建图片目录
    images_dir = output_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    
    # 提取图片 URL
    image_urls = []
    
    # 匹配 HTML img 标签: <img src="url" alt="text">
    html_pattern = r'<img\s+src="([^"]+)"(?:\s+alt="([^"]*)")?[^>]*>'
    for match in re.finditer(html_pattern, md_content):
        url = match.group(1)
        alt_text = match.group(2) or ""
        image_urls.append((alt_text, url))
    
    # 匹配 Markdown 格式: ![alt](url)
    md_pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
    for match in re.finditer(md_pattern, md_content):
        alt_text = match.group(1)
        url = match.group(2)
        # 只添加 HTTP/HTTPS URL（跳过相对路径）
        if url.startswith(('http://', 'https://')):
            image_urls.append((alt_text, url))
    
    if not image_urls:
        print("   ℹ️  No images found in Markdown")
        return md_content, 0
    
    print(f"   📥 Found {len(image_urls)} images to download")
    
    downloaded_count = 0
    updated_content = md_content
    
    for alt_text, url in image_urls:
        try:
            # 检测存储类型
            storage_type = detect_storage_type(url)
            
            # 从 URL 提取文件名
            filename = Path(urlparse(url).path).name
            if not filename or '?' in filename:
                ext = '.png'  # 默认扩展名
                # 从 URL 猜测扩展名
                if url.endswith(('.jpg', '.jpeg')):
                    ext = '.jpg'
                elif url.endswith('.png'):
                    ext = '.png'
                elif url.endswith('.gif'):
                    ext = '.gif'
                filename = f"image_{downloaded_count:03d}{ext}"
            
            image_path = images_dir / filename
            success = False
            
            # 根据存储类型选择下载方式
            if storage_type == 'oss':
                # 使用 OSS SDK 下载（支持私有 Bucket）
                print(f"   🔐 Downloading from OSS: {filename}")
                success = download_from_oss(url, image_path)
                if success and image_path.exists():
                    file_size = image_path.stat().st_size
                    print(f"   ✅ Downloaded: {filename} ({file_size} bytes)")
            else:
                # 使用 HTTP 直接下载（COS/MinIO 公有读）
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        image_data = await response.read()
                        with open(image_path, 'wb') as f:
                            f.write(image_data)
                        success = True
                        print(f"   ✅ Downloaded: {filename} ({len(image_data)} bytes)")
                    else:
                        print(f"   ❌ Failed to download {url}: HTTP {response.status}")
            
            if success:
                # 替换 Markdown 中的链接为本地路径
                local_path = f"images/{filename}"
                
                # 替换 HTML img 标签
                html_old = f'<img src="{url}"'
                html_new = f'<img src="{local_path}"'
                updated_content = updated_content.replace(html_old, html_new)
                
                # 替换 Markdown 格式
                md_old = f']({url})'
                md_new = f']({local_path})'
                updated_content = updated_content.replace(md_old, md_new)
                
                downloaded_count += 1
                
        except Exception as e:
            print(f"   ❌ Error downloading {url}: {e}")
    
    return updated_content, downloaded_count


# ==================== 主工作流程 ====================

async def full_workflow(local_file: str, object_key: Optional[str] = None):
    """完整工作流程：上传 → 提交 → 等待 → 获取结果"""
    
    print("=" * 70)
    print("MinerU Tianshu - 完整 OSS 工作流程")
    print("=" * 70)
    print()
    
    # 检查本地文件
    local_file_path = Path(local_file)
    if not local_file_path.exists():
        print(f"❌ 本地文件不存在: {local_file}")
        return
    
    file_size = local_file_path.stat().st_size
    print(f"📄 本地文件: {local_file}")
    print(f"📦 文件大小: {file_size} bytes ({file_size / 1024 / 1024:.2f} MB)")
    print()
    
    # 生成对象键
    if not object_key:
        object_key = generate_object_key(local_file)
    
    file_name = local_file_path.name
    
    try:
        # ==================== 步骤 1: 上传到 OSS ====================
        print("📋 步骤 1: 上传文件到阿里云 OSS")
        print("-" * 70)
        
        uploader = OSSUploader()
        oss_url = uploader.upload_file(str(local_file_path), object_key)
        print()
        
        # ==================== 步骤 2: 提交任务 ====================
        print("📋 步骤 2: 通过 OSS 对象路径提交任务")
        print("-" * 70)
        
        client = TianshuClient(API_BASE_URL)
        
        async with aiohttp.ClientSession() as session:
            result = await client.submit_task_by_oss(
                session,
                object_name=object_key,  # 使用对象路径（不是完整 URL）
                file_name=file_name,
                backend=BACKEND,
                lang=LANG,
                method=METHOD,
                formula_enable=FORMULA_ENABLE,
                table_enable=TABLE_ENABLE,
                priority=PRIORITY
            )
            
            task_id = result['task_id']
            print()
            
            # ==================== 步骤 3: 等待任务完成 ====================
            print("📋 步骤 3: 等待任务完成")
            print("-" * 70)
            
            await client.wait_for_task(session, task_id)
            print()
            
            # ==================== 步骤 4: 获取结果 ====================
            print("📋 步骤 4: 获取任务结果")
            print("-" * 70)
            
            # 获取结果（可选：自动上传图片到 OSS）
            final_result = await client.get_task_status(
                session,
                task_id,
                upload_images=True  # 自动上传图片到对象存储
            )
            
            if not final_result.get('data'):
                print("⚠️  未返回解析内容（可能已被清理）")
                return
            
            data = final_result['data']
            md_content = data['content']
            md_filename = data['markdown_file']
            images_uploaded = data.get('images_uploaded', False)
            
            # 检测 Markdown 中是否有云端图片链接
            import re
            html_pattern = r'<img\s+src="(https?://[^"]+)"'
            md_pattern = r'!\[[^\]]*\]\((https?://[^)]+)\)'
            cloud_image_urls = []
            cloud_image_urls.extend(re.findall(html_pattern, md_content))
            cloud_image_urls.extend(re.findall(md_pattern, md_content))
            images_count = len(cloud_image_urls)
            
            print(f"✅ 结果获取成功！")
            print(f"   Markdown 文件: {md_filename}")
            print(f"   内容长度: {len(md_content)} 字符")
            print(f"   图片已上传: {'是' if images_uploaded else '否'}")
            print(f"   检测到图片: {images_count} 张")
            print()
            
            # ==================== 步骤 5: 保存到本地 ====================
            print("📋 步骤 5: 保存结果到本地")
            print("-" * 70)
            
            output_dir = Path(OUTPUT_DIR)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # 保存 Markdown（云端链接版本）
            md_path = output_dir / f"{task_id}_{md_filename}"
            with open(md_path, 'w', encoding='utf-8') as f:
                f.write(md_content)
            print(f"✅ Markdown 已保存: {md_path}")
            
            # 保存元数据
            metadata_path = output_dir / f"{task_id}_metadata.json"
            with open(metadata_path, 'w', encoding='utf-8') as f:
                metadata = {
                    'task_id': task_id,
                    'file_name': file_name,
                    'object_key': object_key,
                    'oss_url': oss_url,
                    'status': final_result['status'],
                    'created_at': final_result['created_at'],
                    'completed_at': final_result.get('completed_at'),
                    'images_uploaded': images_uploaded,
                    'images_count': images_count,
                    'markdown_file': md_filename,
                    'content_length': len(md_content)
                }
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            print(f"✅ 元数据已保存: {metadata_path}")
            print()
            
            # ==================== 步骤 6: 下载图片（可选）====================
            images_downloaded = 0
            if DOWNLOAD_IMAGES and images_uploaded and images_count > 0:
                print("📋 步骤 6: 下载图片到本地")
                print("-" * 70)
                print(f"🖼️  Downloading {images_count} images from object storage...")
                
                task_output_dir = output_dir / task_id
                task_output_dir.mkdir(parents=True, exist_ok=True)
                
                updated_md_content, images_downloaded = await download_images_to_local(
                    session,
                    md_content,
                    task_output_dir
                )
                
                if images_downloaded > 0:
                    # 保存更新后的 Markdown
                    local_md_path = task_output_dir / md_filename
                    with open(local_md_path, 'w', encoding='utf-8') as f:
                        f.write(updated_md_content)
                    
                    print(f"✅ 已下载 {images_downloaded} 张图片")
                    print(f"✅ 更新后的 Markdown: {local_md_path}")
                    print(f"   图片目录: {task_output_dir / 'images'}")
                else:
                    print("⚠️  未下载任何图片")
                
                print()
            elif DOWNLOAD_IMAGES and images_uploaded and images_count == 0:
                print("ℹ️  DOWNLOAD_IMAGES 已启用，但未检测到云端图片链接")
                print()
            
            # ==================== 总结 ====================
            print("=" * 70)
            print("✅ 工作流程完成！")
            print("=" * 70)
            print()
            print("📊 处理摘要：")
            print(f"   本地文件: {local_file}")
            print(f"   文件大小: {file_size} bytes")
            print(f"   OSS 对象: {object_key}")
            print(f"   任务 ID: {task_id}")
            print(f"   任务状态: {final_result['status']}")
            print(f"   检测到图片: {images_count} 张")
            if images_uploaded:
                print(f"   图片已上传到 OSS: 是")
            print()
            print("📁 输出文件：")
            print(f"   Markdown: {md_path}")
            print(f"   元数据: {metadata_path}")
            if images_downloaded > 0:
                print(f"   图片目录: {task_output_dir / 'images'} ({images_downloaded} 张)")
            print()
            
            # 预览 Markdown
            print("📄 Markdown 内容预览（前 300 字符）：")
            print("-" * 70)
            preview = md_content[:300]
            if len(md_content) > 300:
                preview += "\n... (truncated)"
            print(preview)
            print()
    
    except Exception as e:
        print(f"❌ 工作流程失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# ==================== 命令行工具 ====================

def cli_upload():
    """CLI: 上传文件到 OSS"""
    if len(sys.argv) < 3:
        print("用法: python3 test_full_oss_workflow.py upload <local_file> [object_key]")
        print()
        print("示例:")
        print("  python3 test_full_oss_workflow.py upload ./report.pdf")
        print("  python3 test_full_oss_workflow.py upload ./report.pdf documents/report.pdf")
        sys.exit(1)
    
    local_file = sys.argv[2]
    object_key = sys.argv[3] if len(sys.argv) > 3 else None
    
    if not Path(local_file).exists():
        print(f"❌ 文件不存在: {local_file}")
        sys.exit(1)
    
    if not object_key:
        object_key = generate_object_key(local_file)
    
    try:
        uploader = OSSUploader()
        url = uploader.upload_file(local_file, object_key)
        
        print()
        print(f"✅ 上传成功！")
        print(f"   Object Key: {object_key}")
        print(f"   URL: {url}")
        print()
        print(f"💡 提交任务命令:")
        print(f"   python3 test_full_oss_workflow.py submit {object_key}")
    
    except Exception as e:
        print(f"❌ 上传失败: {e}")
        sys.exit(1)


async def cli_submit():
    """CLI: 提交任务"""
    if len(sys.argv) < 3:
        print("用法: python3 test_full_oss_workflow.py submit <object_key> [file_name]")
        print()
        print("示例:")
        print("  python3 test_full_oss_workflow.py submit test_uploads/20250101_120000_report.pdf")
        print("  python3 test_full_oss_workflow.py submit documents/report.pdf report.pdf")
        sys.exit(1)
    
    object_key = sys.argv[2]
    file_name = sys.argv[3] if len(sys.argv) > 3 else None
    
    client = TianshuClient(API_BASE_URL)
    
    async with aiohttp.ClientSession() as session:
        try:
            result = await client.submit_task_by_oss(
                session,
                object_name=object_key,
                file_name=file_name,
                backend=BACKEND,
                lang=LANG
            )
            
            print()
            print(f"✅ 任务已提交: {result['task_id']}")
            print(f"💡 查询状态命令:")
            print(f"   python3 test_oss_submit.py status {result['task_id']}")
        
        except Exception as e:
            print(f"❌ 提交失败: {e}")
            sys.exit(1)


def show_usage():
    """显示使用说明"""
    print("=" * 70)
    print("MinerU Tianshu - 完整 OSS 工作流程测试")
    print("=" * 70)
    print()
    print("使用方法：")
    print()
    print("1. 运行完整工作流程（推荐）：")
    print("   python3 test_full_oss_workflow.py")
    print("   python3 test_full_oss_workflow.py full <local_file> [object_key]")
    print()
    print("2. 仅上传文件：")
    print("   python3 test_full_oss_workflow.py upload <local_file> [object_key]")
    print()
    print("3. 仅提交任务（假设文件已上传）：")
    print("   python3 test_full_oss_workflow.py submit <object_key> [file_name]")
    print()
    print("环境变量配置：")
    print()
    print("阿里云 OSS:")
    print("  export OSS_ENDPOINT=oss-cn-beijing.aliyuncs.com")
    print("  export OSS_ACCESS_KEY=<your_access_key_id>")
    print("  export OSS_SECRET_KEY=<your_access_key_secret>")
    print("  export OSS_BUCKET=<your_bucket_name>")
    print()
    print("API 服务地址（可选）:")
    print("  export API_BASE_URL=http://localhost:8000")
    print()


# ==================== 入口 ====================

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "upload":
            cli_upload()
        
        elif command == "submit":
            asyncio.run(cli_submit())
        
        elif command == "full":
            local_file = sys.argv[2] if len(sys.argv) > 2 else LOCAL_FILE_PATH
            object_key = sys.argv[3] if len(sys.argv) > 3 else None
            asyncio.run(full_workflow(local_file, object_key))
        
        elif command == "help":
            show_usage()
        
        else:
            print(f"❌ 未知命令: {command}")
            print()
            show_usage()
    
    else:
        # 无参数：运行完整工作流程
        asyncio.run(full_workflow(LOCAL_FILE_PATH))

