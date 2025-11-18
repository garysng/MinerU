#!/usr/bin/env python3
"""
完整示例：提交 PDF → 等待完成 → 自动上传图片到对象存储 → 下载图片到本地 → 保存结果

功能：
1. 提交本地 PDF 文件
2. 等待任务完成
3. 通过 API 自动上传图片到对象存储（upload_images=true）
   - 支持 MinIO、腾讯云 COS、阿里云 OSS
   - 存储类型在服务端配置（STORAGE_TYPE 环境变量）
4. 保存 Markdown（包含对象存储图片链接）到本地
5. 【新增】从对象存储下载图片到本地（可选，DOWNLOAD_IMAGES=True）
   - 提取 Markdown 中的图片 URL
   - 下载所有图片到本地 images/ 目录
   - 更新 Markdown 中的图片链接为本地路径
   - 保存更新后的 Markdown 文件
6. 其他文件（JSON、PDF 等）保留在服务器本地路径

输出文件结构（DOWNLOAD_IMAGES=True 时）：
./auto_upload_output/
├── {task_id}_{filename}.md       # 原始 Markdown（对象存储链接）
├── {task_id}_metadata.json       # 任务元数据
└── {task_id}/                    # 任务专属目录
    ├── {filename}.md             # 更新后的 Markdown（本地路径）
    └── images/                   # 下载的图片
        ├── image_000.png
        ├── image_001.png
        └── ...

服务端环境变量要求（在 api_server.py 所在服务器配置）：

MinIO/腾讯云 COS:
  - STORAGE_TYPE: "minio" 或 "cos"
  - MINIO_ENDPOINT: Endpoint
  - MINIO_ACCESS_KEY: Access Key
  - MINIO_SECRET_KEY: Secret Key
  - MINIO_BUCKET: Bucket 名称
  - MINIO_SECURE: "true" 或 "false"

阿里云 OSS:
  - STORAGE_TYPE: "oss"
  - OSS_ENDPOINT: Endpoint (如 oss-cn-shanghai.aliyuncs.com)
  - OSS_ACCESS_KEY: Access Key ID
  - OSS_SECRET_KEY: Access Key Secret
  - OSS_BUCKET: Bucket 名称

客户端配置（仅当下载 OSS 私有 Bucket 图片时需要）：
  - OSS_ACCESS_KEY: 阿里云 Access Key ID
  - OSS_SECRET_KEY: 阿里云 Access Key Secret
  
  如果 OSS Bucket 为公有读，则客户端无需配置凭证。
"""

import asyncio
import aiohttp
import json
import re
import os
from pathlib import Path
from typing import Optional, List, Tuple
from urllib.parse import urlparse

# ==================== 配置 ====================
API_BASE_URL = "https://facb9f32ae0653ea-8000.cn-south-1.gpu-instance.ppinfra.com"
LOCAL_PDF_PATH = "./1.pdf"  # 要上传的 PDF 文件路径
OUTPUT_DIR = "./auto_upload_output"  # 本地保存目录

# 任务参数
BACKEND = "http-client"  # 或 "pipeline"
PARSE_METHOD = "auto"
LANG_LIST = ["zh"]

# 下载选项
DOWNLOAD_IMAGES = True  # ✅ 设置为 True 可下载图片到本地
                        # ❌ 设置为 False 只使用对象存储链接

# ==================== 辅助函数 ====================

def extract_image_urls(md_content: str) -> List[Tuple[str, str]]:
    """
    从 Markdown 内容中提取图片 URL
    
    Args:
        md_content: Markdown 内容
        
    Returns:
        [(alt_text, image_url), ...] 列表
    """
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
    
    return image_urls


def detect_storage_type(url: str) -> str:
    """
    检测 URL 来自哪种对象存储
    
    Returns:
        'oss', 'cos', 'minio', 'unknown'
    """
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


async def download_images_from_storage(
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
    # 创建图片目录
    images_dir = output_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    
    # 提取图片 URL
    image_urls = extract_image_urls(md_content)
    
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
            image_data = None
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


# ==================== 客户端 ====================

class TianshuClient:
    """MinerU Tianshu API 客户端"""
    
    def __init__(self, base_url: str = API_BASE_URL):
        self.base_url = base_url.rstrip('/')
    
    async def submit_task(
        self,
        session: aiohttp.ClientSession,
        file_path: str,
        backend: str = "pipeline",
        parse_method: str = "auto",
        lang_list: list = None
    ) -> dict:
        """提交任务"""
        if lang_list is None:
            lang_list = ["zh"]
        
        url = f"{self.base_url}/api/v1/tasks/submit"
        
        # 准备文件
        file_name = Path(file_path).name
        with open(file_path, 'rb') as f:
            file_data = f.read()
        
        # 构建表单数据
        form = aiohttp.FormData()
        form.add_field('file',
                      file_data,
                      filename=file_name,
                      content_type='application/pdf')
        form.add_field('backend', backend)
        form.add_field('parse_method', parse_method)
        for lang in lang_list:
            form.add_field('lang_list', lang)
        
        print(f"📤 Submitting task: {file_name}")
        print(f"   Backend: {backend}")
        print(f"   Parse method: {parse_method}")
        print(f"   Languages: {lang_list}")
        
        async with session.post(url, data=form) as resp:
            result = await resp.json()
            if result.get('success'):
                print(f"✅ Task submitted successfully!")
                print(f"   Task ID: {result['task_id']}")
                return result
            else:
                raise Exception(f"Failed to submit task: {result}")
    
    async def get_task_status(
        self,
        session: aiohttp.ClientSession,
        task_id: str,
        upload_images: bool = False
    ) -> dict:
        """
        查询任务状态
        
        Args:
            task_id: 任务 ID
            upload_images: 是否自动上传图片到 COS/MinIO（如果为 True，会替换 Markdown 中的图片链接）
        """
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
        """
        等待任务完成
        
        Returns:
            任务最终状态（不包含图片上传，需要单独调用 get_task_status(upload_images=True)）
        """
        print(f"⏳ Waiting for task {task_id} to complete...")
        
        elapsed = 0.0
        while elapsed < timeout:
            result = await self.get_task_status(session, task_id, upload_images=False)
            status = result['status']
            
            if status == 'completed':
                print(f"✅ Task completed!")
                return result
            elif status == 'failed':
                error = result.get('error_message', 'Unknown error')
                raise Exception(f"Task failed: {error}")
            elif status == 'cancelled':
                raise Exception("Task was cancelled")
            
            # 显示进度
            print(f"   Status: {status} (elapsed: {elapsed:.1f}s)")
            
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        
        raise TimeoutError(f"Task did not complete within {timeout}s")


# ==================== 主函数 ====================

async def main():
    """完整的工作流程"""
    
    print("=" * 60)
    print("MinerU Tianshu - 自动上传图片到对象存储示例")
    print("=" * 60)
    print()
    
    # 检查 PDF 文件
    if not Path(LOCAL_PDF_PATH).exists():
        print(f"❌ PDF file not found: {LOCAL_PDF_PATH}")
        print(f"   Please update LOCAL_PDF_PATH in the script")
        return
    
    # 创建输出目录
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    client = TianshuClient(API_BASE_URL)
    
    async with aiohttp.ClientSession() as session:
        # ==================== 步骤 1: 提交任务 ====================
        print("📋 Step 1: Submit task")
        print("-" * 60)
        
        result = await client.submit_task(
            session,
            file_path=LOCAL_PDF_PATH,
            backend=BACKEND,
            parse_method=PARSE_METHOD,
            lang_list=LANG_LIST
        )
        
        task_id = result['task_id']
        print()
        
        # ==================== 步骤 2: 等待任务完成 ====================
        print("📋 Step 2: Wait for task completion")
        print("-" * 60)
        
        await client.wait_for_task(session, task_id)
        print()
        
        # ==================== 步骤 3: 获取结果（自动上传图片到对象存储）====================
        print("📋 Step 3: Get results with auto image upload to storage")
        print("-" * 60)
        print("🖼️  Requesting API to upload images to object storage...")
        
        final_result = await client.get_task_status(
            session,
            task_id,
            upload_images=True  # 🔑 关键参数：自动上传图片到对象存储
        )
        
        if not final_result.get('data'):
            print("❌ No data returned. Task may have failed or results were cleaned up.")
            return
        
        data = final_result['data']
        md_content = data['content']
        md_filename = data['markdown_file']
        images_uploaded = data['images_uploaded']
        
        print(f"✅ Results retrieved successfully!")
        print(f"   Markdown file: {md_filename}")
        print(f"   Content length: {len(md_content)} characters")
        print(f"   Images uploaded to storage: {images_uploaded}")
        print()
        
        # ==================== 步骤 4: 保存到本地 ====================
        print("📋 Step 4: Save results to local")
        print("-" * 60)
        
        # 保存 Markdown（包含对象存储图片链接）
        local_md_path = output_dir / f"{task_id}_{md_filename}"
        with open(local_md_path, 'w', encoding='utf-8') as f:
            f.write(md_content)
        print(f"✅ Saved Markdown to: {local_md_path}")
        
        # 保存完整的 API 响应（包含元数据）
        metadata_path = output_dir / f"{task_id}_metadata.json"
        with open(metadata_path, 'w', encoding='utf-8') as f:
            # 不保存 content（已经保存为 .md 文件）
            metadata = {k: v for k, v in final_result.items() if k != 'data'}
            metadata['data_info'] = {
                'markdown_file': md_filename,
                'content_length': len(md_content),
                'images_uploaded': images_uploaded
            }
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved metadata to: {metadata_path}")
        print()
        
        # ==================== 步骤 5: 下载图片到本地（可选）====================
        images_downloaded = 0
        final_md_path = local_md_path
        
        if DOWNLOAD_IMAGES and images_uploaded:
            print("📋 Step 5: Download images from storage to local")
            print("-" * 60)
            print("🖼️  Downloading images from object storage...")
            
            # 创建任务专属输出目录
            task_output_dir = output_dir / task_id
            task_output_dir.mkdir(parents=True, exist_ok=True)
            
            # 下载图片并更新 Markdown 内容
            updated_md_content, images_downloaded = await download_images_from_storage(
                session,
                md_content,
                task_output_dir
            )
            
            if images_downloaded > 0:
                # 保存更新后的 Markdown（包含本地图片路径）
                final_md_path = task_output_dir / md_filename
                with open(final_md_path, 'w', encoding='utf-8') as f:
                    f.write(updated_md_content)
                
                print(f"✅ Downloaded {images_downloaded} images")
                print(f"✅ Saved updated Markdown to: {final_md_path}")
                print(f"   Images directory: {task_output_dir / 'images'}")
            else:
                print("⚠️  No images downloaded")
            
            print()
        elif DOWNLOAD_IMAGES and not images_uploaded:
            print("ℹ️  DOWNLOAD_IMAGES is True but no images were uploaded to storage")
            print("   Skipping image download step")
            print()
        
        # ==================== 总结 ====================
        print("=" * 60)
        print("✅ 完成！")
        print("=" * 60)
        print()
        print("📁 本地文件：")
        
        if images_downloaded > 0:
            print(f"   • Markdown (含本地图片路径): {final_md_path}")
            print(f"   • 图片目录: {task_output_dir / 'images'} ({images_downloaded} 张图片)")
            print(f"   • 原始 Markdown (含云端链接): {local_md_path}")
        else:
            print(f"   • Markdown (含对象存储图片链接): {local_md_path}")
        
        print(f"   • 元数据: {metadata_path}")
        print()
        
        print("🖼️  图片存储：")
        if images_uploaded:
            print(f"   • 云端：所有图片已上传到对象存储（MinIO/COS/OSS）")
        if images_downloaded > 0:
            print(f"   • 本地：已下载 {images_downloaded} 张图片到本地")
            print(f"   • Markdown 图片链接：本地路径 (images/xxx.png)")
        elif DOWNLOAD_IMAGES:
            print(f"   • 本地：未下载图片（DOWNLOAD_IMAGES=True 但没有找到图片）")
        else:
            print(f"   • 本地：未下载图片（DOWNLOAD_IMAGES=False）")
            print(f"   • Markdown 图片链接：对象存储 URL")
        
        print(f"   • 存储类型由服务端配置决定（STORAGE_TYPE 环境变量）")
        print()
        print("📂 服务器本地文件（如需要）：")
        result_path = final_result.get('result_path')
        if result_path:
            print(f"   • 完整结果路径: {result_path}")
            print(f"   • 包含: JSON、Layout PDF、原始 PDF 等")
            print(f"   • 可通过服务器直接访问或使用其他方式传输")
        print()
        
        # 显示 Markdown 预览
        print("📄 Markdown 内容预览（前 200 字符）：")
        print("-" * 60)
        
        # 如果下载了图片，显示更新后的 Markdown，否则显示原始的
        if images_downloaded > 0:
            with open(final_md_path, 'r', encoding='utf-8') as f:
                preview_content = f.read()
            preview = preview_content[:200]
            if len(preview_content) > 200:
                preview += "\n... (truncated)"
            print(f"(显示本地图片版本)")
        else:
            preview = md_content[:200]
            if len(md_content) > 200:
                preview += "\n... (truncated)"
            if images_uploaded:
                print(f"(显示云端图片链接版本)")
        
        print(preview)
        print()


if __name__ == "__main__":
    asyncio.run(main())

