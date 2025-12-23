"""
MinerU Tianshu RunPod Client - Complete Workflow
天枢 RunPod 客户端 - 完整工作流程

演示完整的文档解析工作流程：
1. 上传文件到阿里云 OSS
2. 提交任务到 RunPod Serverless API
3. 等待任务完成并获取结果

架构说明：
- RunPod Handler 只负责透传任务参数给 MinerU API 服务器
- 文件下载、解析等业务逻辑由 MinerU API 服务器处理
- RunPod Handler 等待 API 服务器完成任务后返回结果
"""
import requests
import json
import time
import hashlib
from pathlib import Path
from typing import Dict, Any
from loguru import logger


class OSSUploader:
    """阿里云 OSS 上传器"""
    
    def __init__(self, access_key_id: str, access_key_secret: str, endpoint: str, bucket_name: str):
        """
        初始化 OSS 上传器
        
        Args:
            access_key_id: 阿里云 Access Key ID
            access_key_secret: 阿里云 Access Key Secret
            endpoint: OSS 端点 (如: oss-cn-beijing.aliyuncs.com)
            bucket_name: OSS Bucket 名称
        """
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        self._client = None
        self._init_client()
    
    def _init_client(self):
        """初始化 OSS 客户端"""
        try:
            import oss2
            auth = oss2.Auth(self.access_key_id, self.access_key_secret)
            self._client = oss2.Bucket(auth, self.endpoint, self.bucket_name)
            logger.info(f"✅ OSS client initialized: {self.bucket_name}")
        except ImportError:
            raise ImportError("请安装 oss2 库: pip install oss2")
    
    def upload_file(self, local_path: str, object_name: str) -> str:
        """
        上传文件到 OSS
        
        Args:
            local_path: 本地文件路径
            object_name: OSS 中的对象名称/路径
            
        Returns:
            上传后的对象名称
        """
        local_file = Path(local_path)
        if not local_file.exists():
            raise FileNotFoundError(f"本地文件不存在: {local_path}")
        
        logger.info(f"📤 Uploading {local_file.name} to OSS://{object_name}")
        
        # 上传文件到 OSS
        with open(local_file, 'rb') as f:
            self._client.put_object(object_name, f)
        
        logger.info(f"✅ Upload completed: {object_name}")
        return object_name
    
    def generate_object_name(self, local_path: str, prefix: str = "documents") -> str:
        """
        生成对象存储中的文件名
        
        Args:
            local_path: 本地文件路径
            prefix: 前缀目录
            
        Returns:
            对象名称
        """
        local_file = Path(local_path)
        
        # 生成基于文件内容的哈希值，避免重复上传
        with open(local_file, 'rb') as f:
            file_hash = hashlib.md5(f.read()).hexdigest()[:8]
        
        # 构造对象名称: prefix/filename_hash.ext
        object_name = f"{prefix}/{local_file.stem}_{file_hash}{local_file.suffix}"
        return object_name


class RunPodClient:
    """RunPod Serverless 客户端"""
    
    def __init__(self, endpoint_id: str, api_key: str):
        """
        初始化 RunPod 客户端
        
        Args:
            endpoint_id: RunPod Endpoint ID
            api_key: RunPod API Key
        """
        self.endpoint_id = endpoint_id
        self.api_key = api_key
        self.base_url = f"https://api.runpod.ai/v2/{endpoint_id}"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def submit_task(self, object_name: str, **kwargs) -> Dict[str, Any]:
        """
        提交文档解析任务
        
        Args:
            object_name: OSS 对象路径或完整 URL
            **kwargs: 其他参数（file_name, backend, lang, etc.）
            
        Returns:
            RunPod 响应
        """
        payload = {
            "input": {
                "object_name": object_name,
                **kwargs
            }
        }
        
        logger.info(f"📤 Submitting task: {object_name}")
        
        response = requests.post(
            f"{self.base_url}/run",
            headers=self.headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Task submitted: {result.get('id')}")
            return result
        else:
            logger.error(f"❌ Submission failed: {response.status_code} - {response.text}")
            response.raise_for_status()
    
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        查询任务状态
        
        Args:
            task_id: RunPod 任务 ID
            
        Returns:
            任务状态和结果
        """
        response = requests.get(
            f"{self.base_url}/status/{task_id}",
            headers=self.headers,
            timeout=10
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"❌ Status query failed: {response.status_code} - {response.text}")
            response.raise_for_status()
    
    def wait_for_completion(self, task_id: str, timeout: int = 300, poll_interval: int = 5) -> Dict[str, Any]:
        """
        等待任务完成
        
        Args:
            task_id: RunPod 任务 ID
            timeout: 超时时间（秒）
            poll_interval: 轮询间隔（秒）
            
        Returns:
            最终结果
        """
        start_time = time.time()
        
        logger.info(f"⏳ Waiting for task {task_id} to complete...")
        
        while time.time() - start_time < timeout:
            try:
                status = self.get_task_status(task_id)
                
                if status.get('status') == 'COMPLETED':
                    logger.info(f"✅ Task {task_id} completed")
                    return status
                elif status.get('status') == 'FAILED':
                    logger.error(f"❌ Task {task_id} failed")
                    return status
                elif status.get('status') in ['IN_QUEUE', 'IN_PROGRESS']:
                    logger.info(f"🔄 Task {task_id} status: {status.get('status')}")
                else:
                    logger.info(f"📊 Task {task_id} status: {status}")
                
                time.sleep(poll_interval)
                
            except Exception as e:
                logger.warning(f"⚠️  Status check failed: {e}")
                time.sleep(poll_interval)
        
        logger.error(f"⏰ Task {task_id} timed out after {timeout} seconds")
        return {"status": "TIMEOUT", "message": f"Task timed out after {timeout} seconds"}
    
    def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        payload = {
            "input": {
                "action": "health"
            }
        }
        
        response = requests.post(
            f"{self.base_url}/run",
            headers=self.headers,
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()


def example_complete_workflow():
    """完整工作流程示例：上传文件 + 处理任务"""
    logger.info("=" * 60)
    logger.info("🧪 Complete Workflow Example: Upload + Process")
    logger.info("=" * 60)
    
    # 配置（请替换为你的实际值）
    ENDPOINT_ID = "your-endpoint-id"
    API_KEY = "your-api-key"
    
    # 本地文件路径
    local_file = "sample.pdf"  # 请确保此文件存在
    
    # OSS 配置
    oss_config = {
        'access_key_id': 'your-access-key-id',
        'access_key_secret': 'your-access-key-secret',
        'endpoint': 'oss-cn-beijing.aliyuncs.com',
        'bucket_name': 'your-bucket-name'
    }
    
    try:
        # 检查本地文件
        if not Path(local_file).exists():
            logger.error(f"❌ 本地文件不存在: {local_file}")
            logger.info("请将要处理的文件重命名为 'sample.pdf' 并放在当前目录")
            return
        
        # 1. 初始化 OSS 上传器
        logger.info("🔧 Initializing OSS uploader...")
        uploader = OSSUploader(**oss_config)
        
        # 2. 上传文件到 OSS
        logger.info("📤 Uploading file to OSS...")
        object_name = uploader.generate_object_name(local_file, prefix="documents")
        uploader.upload_file(local_file, object_name)
        
        # 3. 初始化 RunPod 客户端
        logger.info("🚀 Initializing RunPod client...")
        client = RunPodClient(ENDPOINT_ID, API_KEY)
        
        # 4. 健康检查
        logger.info("🔍 Performing health check...")
        health = client.health_check()
        logger.info(f"Health status: {health}")
        
        # 5. 提交任务到 RunPod
        logger.info("📋 Submitting task to RunPod...")
        task_result = client.submit_task(
            object_name=object_name,
            file_name=Path(local_file).name,
            backend="pipeline",
            lang="ch",
            method="auto",
            formula_enable=True,
            table_enable=True,
            priority=0
        )
        
        task_id = task_result.get('id')
        if not task_id:
            logger.error("❌ Failed to get task ID")
            return
        
        # 6. 等待任务完成
        logger.info(f"⏳ Waiting for task completion: {task_id}")
        final_result = client.wait_for_completion(task_id, timeout=300)
        
        # 7. 处理结果
        if final_result.get('status') == 'COMPLETED':
            output = final_result.get('output', {})
            if output.get('success'):
                logger.info("🎉 Complete workflow finished successfully!")
                logger.info(f"   Original file: {local_file}")
                logger.info(f"   Object name: {object_name}")
                logger.info(f"   Parser used: {output.get('parser')}")
                logger.info(f"   File size: {output.get('file_size', 0)} bytes")
                logger.info(f"   Content length: {output.get('content_length')} characters")
                logger.info(f"   Processing time: {output.get('processing_time'):.2f}s")
                logger.info(f"   Result files: {len(output.get('result_files', []))}")
                logger.info(f"   Images: {output.get('image_count', 0)}")
                
                # 保存解析结果到本地
                if output.get('content'):
                    output_file = f"output_{Path(local_file).stem}.md"
                    with open(output_file, 'w', encoding='utf-8') as f:
                        f.write(output['content'])
                    logger.info(f"📝 Content saved to {output_file}")
                
                # 显示结果预览
                content = output.get('content', '')
                if content:
                    preview = content[:200] + "..." if len(content) > 200 else content
                    logger.info(f"📄 Content preview:\n{preview}")
                    
            else:
                logger.error(f"❌ Task failed: {output.get('error')}")
        else:
            logger.error(f"❌ Task did not complete successfully: {final_result}")
    
    except Exception as e:
        logger.error(f"❌ Complete workflow failed: {e}")
        import traceback
        logger.error(traceback.format_exc())


def example_batch_workflow():
    """批量文件处理示例：上传多个文件并批量处理"""
    logger.info("=" * 60)
    logger.info("🧪 Batch Workflow Example: Upload + Process Multiple Files")
    logger.info("=" * 60)
    
    # 配置
    ENDPOINT_ID = "your-endpoint-id"
    API_KEY = "your-api-key"
    
    # 本地文件列表
    local_files = [
        {"path": "doc1.pdf", "lang": "ch"},
        {"path": "doc2.docx", "lang": "en"},
        {"path": "data.xlsx", "lang": "ch"},
    ]
    
    # OSS 配置
    oss_config = {
        'access_key_id': 'your-access-key-id',
        'access_key_secret': 'your-access-key-secret',
        'endpoint': 'oss-cn-beijing.aliyuncs.com',
        'bucket_name': 'your-bucket-name'
    }
    
    try:
        # 1. 初始化客户端
        uploader = OSSUploader(**oss_config)
        client = RunPodClient(ENDPOINT_ID, API_KEY)
        
        # 2. 批量上传文件
        uploaded_files = []
        logger.info("📤 Uploading files to OSS...")
        
        for file_info in local_files:
            local_path = file_info['path']
            
            if not Path(local_path).exists():
                logger.warning(f"⚠️  File not found, skipping: {local_path}")
                continue
            
            try:
                object_name = uploader.generate_object_name(local_path, prefix="batch")
                uploader.upload_file(local_path, object_name)
                
                uploaded_files.append({
                    'local_path': local_path,
                    'object_name': object_name,
                    'lang': file_info['lang']
                })
                
            except Exception as e:
                logger.error(f"❌ Failed to upload {local_path}: {e}")
        
        if not uploaded_files:
            logger.error("❌ No files uploaded successfully")
            return
        
        logger.info(f"✅ Uploaded {len(uploaded_files)} files")
        
        # 3. 批量提交任务
        tasks = []
        logger.info("📋 Submitting tasks to RunPod...")
        
        for file_info in uploaded_files:
            logger.info(f"📤 Submitting: {file_info['object_name']}")
            
            task_result = client.submit_task(
                object_name=file_info['object_name'],
                file_name=Path(file_info['local_path']).name,
                backend="pipeline",
                lang=file_info['lang'],
                formula_enable=True,
                table_enable=True
            )
            
            tasks.append({
                'id': task_result.get('id'),
                'local_path': file_info['local_path'],
                'object_name': file_info['object_name'],
                'submitted_at': time.time()
            })
            
            # 避免过快提交
            time.sleep(1)
        
        logger.info(f"✅ Submitted {len(tasks)} tasks")
        
        # 4. 等待所有任务完成
        completed_tasks = []
        logger.info("⏳ Waiting for all tasks to complete...")
        
        for task in tasks:
            logger.info(f"⏳ Waiting for {Path(task['local_path']).name}...")
            
            result = client.wait_for_completion(task['id'], timeout=300)
            
            completed_tasks.append({
                'local_path': task['local_path'],
                'object_name': task['object_name'],
                'task_id': task['id'],
                'result': result
            })
        
        # 5. 汇总结果
        logger.info("=" * 60)
        logger.info("📊 Batch Processing Summary")
        logger.info("=" * 60)
        
        successful = 0
        failed = 0
        
        for task in completed_tasks:
            status = task['result'].get('status')
            file_name = Path(task['local_path']).name
            
            if status == 'COMPLETED':
                output = task['result'].get('output', {})
                if output.get('success'):
                    successful += 1
                    logger.info(f"✅ {file_name}: {output.get('parser')} - {output.get('processing_time', 0):.1f}s")
                    
                    # 保存结果
                    if output.get('content'):
                        output_file = f"batch_output_{Path(task['local_path']).stem}.md"
                        with open(output_file, 'w', encoding='utf-8') as f:
                            f.write(output['content'])
                        logger.info(f"   📝 Saved to {output_file}")
                else:
                    failed += 1
                    logger.error(f"❌ {file_name}: {output.get('error', 'Unknown error')}")
            else:
                failed += 1
                logger.error(f"❌ {file_name}: {status}")
        
        logger.info(f"📈 Final Results: {successful} successful, {failed} failed")
    
    except Exception as e:
        logger.error(f"❌ Batch workflow failed: {e}")
        import traceback
        logger.error(traceback.format_exc())


def show_configuration_guide():
    """显示配置指南"""
    print("📋 配置指南")
    print("=" * 60)
    print()
    print("1. RunPod 配置:")
    print("   - ENDPOINT_ID: 你的 RunPod Endpoint ID")
    print("   - API_KEY: 你的 RunPod API Key")
    print()
    print("2. 阿里云 OSS 配置:")
    print("   oss_config = {")
    print("       'access_key_id': 'your-access-key-id',")
    print("       'access_key_secret': 'your-access-key-secret',")
    print("       'endpoint': 'oss-cn-beijing.aliyuncs.com',")
    print("       'bucket_name': 'your-bucket-name'")
    print("   }")
    print()
    print("   常用 OSS 端点:")
    print("   - 华北2（北京）: oss-cn-beijing.aliyuncs.com")
    print("   - 华东1（杭州）: oss-cn-hangzhou.aliyuncs.com")
    print("   - 华东2（上海）: oss-cn-shanghai.aliyuncs.com")
    print("   - 华南1（深圳）: oss-cn-shenzhen.aliyuncs.com")
    print()
    print("3. 准备文件:")
    print("   - 单文件处理: 将文件命名为 'sample.pdf' 放在当前目录")
    print("   - 批量处理: 将文件命名为 'doc1.pdf', 'doc2.docx', 'data.xlsx' 等")
    print()
    print("4. 依赖安装:")
    print("   pip install requests loguru oss2")
    print()


if __name__ == '__main__':
    # 设置日志级别
    logger.remove()
    logger.add(lambda msg: print(msg, end=''), level="INFO", format="{time:HH:mm:ss} | {level} | {message}")
    
    print("🚀 MinerU Tianshu RunPod Client - Complete Workflow")
    print("=" * 60)
    print()
    print("选择要运行的示例:")
    print("1. 完整工作流程 (推荐) - 上传文件 + 处理任务")
    print("2. 批量工作流程 - 上传多个文件 + 批量处理")
    print("3. 显示配置指南")
    print()
    
    choice = input("请输入选择 (1-3): ").strip()
    
    if choice == '1':
        print()
        print("🔄 启动完整工作流程...")
        print("请确保:")
        print("- 已配置 RunPod ENDPOINT_ID 和 API_KEY")
        print("- 已配置 OSS 连接信息")
        print("- 当前目录有 'sample.pdf' 文件")
        print()
        input("按 Enter 继续...")
        example_complete_workflow()
    elif choice == '2':
        print()
        print("🔄 启动批量工作流程...")
        print("请确保:")
        print("- 已配置 RunPod ENDPOINT_ID 和 API_KEY")
        print("- 已配置 OSS 连接信息")
        print("- 当前目录有要处理的文件 (doc1.pdf, doc2.docx, data.xlsx 等)")
        print()
        input("按 Enter 继续...")
        example_batch_workflow()
    elif choice == '3':
        show_configuration_guide()
    else:
        print("❌ 无效选择")
        print()
        print("💡 提示:")
        print("- 选项 1: 处理单个文件的完整流程")
        print("- 选项 2: 批量处理多个文件的完整流程")
        print("- 选项 3: 查看详细的配置说明")
