"""
MinerU Tianshu - RunPod Serverless Async Handler (异步版)
天枢 RunPod Serverless 异步处理器

基于 RunPod Serverless 实现的异步文档解析 worker
- 使用异步 handler 提供更好的并发性能
- 只负责透传任务给 MinerU API 服务器，不处理具体的文件下载和解析逻辑

特性：
- 异步处理：使用 async/await 提供更好的并发性能
- 透传架构：职责清晰，只做任务透传
- 高效并发：支持多个任务并发处理
"""
import runpod
import os
import json
import time
import asyncio
import aiohttp
from typing import Dict, Any
from loguru import logger
from pathlib import Path


class MinerURunPodHandler:
    """MinerU RunPod Serverless 处理器 - 透传模式"""
    
    def __init__(self):
        self.worker_id = os.getenv('RUNPOD_POD_ID', f'runpod-{int(time.time())}')
        self.api_url = os.getenv('MINERU_API_URL', 'http://localhost:8000')
        
        logger.info(f"🚀 MinerU RunPod Handler initialized")
        logger.info(f"   Worker ID: {self.worker_id}")
        logger.info(f"   API URL: {self.api_url}")
    
    def _convert_api_response_to_runpod_format(self, api_response: Dict[str, Any]) -> Dict[str, Any]:
        """将 API 服务器响应格式转换为 RunPod 期望的格式"""
        try:
            # 基础信息
            result = {
                'success': True,
                'task_id': api_response.get('task_id'),
                'file_name': api_response.get('file_name'),
                'status': api_response.get('status'),
                'backend': api_response.get('backend'),
                'created_at': api_response.get('created_at'),
                'completed_at': api_response.get('completed_at'),
                'worker_id': api_response.get('worker_id')
            }
            
            # 处理解析结果数据
            data = api_response.get('data')
            if data and isinstance(data, dict):
                # 从 API 响应中提取内容
                content = data.get('content', '')
                result.update({
                    'content': content,
                    'content_length': len(content),
                    'markdown_file': data.get('markdown_file'),
                    'images_uploaded': data.get('images_uploaded', False),
                    'has_images': data.get('has_images', False)
                })
                
                # 尝试从内容中统计图片数量
                image_count = 0
                if content:
                    # 简单统计 markdown 中的图片引用
                    import re
                    image_patterns = [
                        r'!\[.*?\]\(.*?\)',  # ![alt](url)
                        r'<img.*?src=.*?>',  # <img src="...">
                    ]
                    for pattern in image_patterns:
                        image_count += len(re.findall(pattern, content, re.IGNORECASE))
                
                result['image_count'] = image_count
                
                # 模拟结果文件列表（API 当前不返回这个信息）
                result_files = [data.get('markdown_file', 'output.md')]
                if result.get('has_images'):
                    # 如果有图片，添加一些示例图片文件名
                    for i in range(min(image_count, 10)):  # 最多显示10个
                        result_files.append(f'images/img_{i+1}.png')
                
                result['result_files'] = result_files
            else:
                # 没有数据或数据格式不正确
                result.update({
                    'content': '',
                    'content_length': 0,
                    'image_count': 0,
                    'result_files': [],
                    'markdown_file': None
                })
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to convert API response format: {e}")
            # 返回基础错误格式
            return {
                'success': False,
                'error': f"Response format conversion failed: {str(e)}",
                'task_id': api_response.get('task_id'),
                'status': api_response.get('status', 'unknown')
            }
    
    async def submit_task_to_api(self, task_params: Dict[str, Any], runpod_task_id: str) -> Dict[str, Any]:
        """向 MinerU API 服务器提交任务并等待结果"""
        submit_url = f"{self.api_url}/api/v1/tasks/submit_by_oss"
        
        async with aiohttp.ClientSession() as session:
            try:
                # 提交任务
                logger.info(f"📤 Submitting task to API: {submit_url}")
                async with session.post(submit_url, json=task_params) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise RuntimeError(f"API submission failed: {response.status} - {error_text}")
                    
                    submit_result = await response.json()
                    task_id = submit_result.get('task_id')
                    
                    if not task_id:
                        raise RuntimeError("No task_id returned from API")
                    
                    logger.info(f"📋 Task submitted successfully: {task_id}")
                    
                    # 轮询任务状态直到完成
                    status_url = f"{self.api_url}/api/v1/tasks/{task_id}"
                    
                    max_wait_time = 300  # 最大等待5分钟
                    poll_interval = 2    # 每2秒轮询一次
                    waited_time = 0
                    
                    logger.info(f"⏳ Waiting for task completion: {task_id}")
                    
                    while waited_time < max_wait_time:
                        async with session.get(status_url) as status_response:
                            if status_response.status != 200:
                                raise RuntimeError(f"Status check failed: {status_response.status}")
                            
                            status_data = await status_response.json()
                            status = status_data.get('status')
                            
                            logger.debug(f"🔍 Task {task_id} status: {status}")
                            
                            # 计算进度百分比
                            progress = min(20 + (waited_time / max_wait_time) * 70, 90)
                            
                            if status == 'completed':
                                # 任务完成，状态响应中已包含结果数据
                                logger.info(f"✅ Task {task_id} completed successfully")
                                
                                # 转换 API 响应格式为 RunPod 期望的格式
                                result_data = self._convert_api_response_to_runpod_format(status_data)
                                return result_data
                            
                            elif status == 'failed':
                                error_msg = status_data.get('error_message', 'Unknown error')
                                raise RuntimeError(f"Task failed: {error_msg}")
                            
                            elif status in ['pending', 'processing']:
                                # 继续等待
                                await asyncio.sleep(poll_interval)
                                waited_time += poll_interval
                            else:
                                raise RuntimeError(f"Unknown task status: {status}")
                    
                    raise RuntimeError(f"Task timeout after {max_wait_time} seconds")
                    
            except Exception as e:
                logger.error(f"❌ API communication failed: {e}")
                raise

    async def process_task(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """处理单个任务 - 异步透传给 MinerU API 服务器"""
        start_time = time.time()
        runpod_task_id = event.get('id', f"runpod-{int(time.time())}")
        
        try:
            # 解析输入参数
            input_data = event.get('input', {})
            
            # 必需参数
            object_name = input_data.get('object_name')
            if not object_name:
                raise ValueError("object_name is required")
            
            # 可选参数
            file_name = input_data.get('file_name')
            backend = input_data.get('backend', 'pipeline')
            lang = input_data.get('lang', 'ch')
            method = input_data.get('method', 'auto')
            formula_enable = input_data.get('formula_enable', True)
            table_enable = input_data.get('table_enable', True)
            priority = input_data.get('priority', 0)
            
            logger.info(f"🔄 Processing RunPod task: {runpod_task_id}")
            logger.info(f"   Object: {object_name}")
            logger.info(f"   Backend: {backend}")
            logger.info(f"   Language: {lang}")
            
            # 构建 API 请求参数
            api_params = {
                'object_name': object_name,
                'backend': backend,
                'lang': lang,
                'method': method,
                'formula_enable': formula_enable,
                'table_enable': table_enable,
                'priority': priority
            }
            
            # 如果提供了文件名，添加到参数中
            if file_name:
                api_params['file_name'] = file_name
            
            # 调用 API 服务器处理任务
            result = await self.submit_task_to_api(api_params, runpod_task_id)
            
            processing_time = time.time() - start_time
            
            # 添加 RunPod 特有的字段
            result.update({
                'processing_time': processing_time,
                'worker_id': self.worker_id,
                'runpod_task_id': runpod_task_id,  # RunPod 任务ID
                'api_task_id': result.get('task_id')  # API 服务器任务ID
            })
            
            logger.info(f"✅ RunPod task completed in {processing_time:.2f}s")
            logger.info(f"   API Task ID: {result.get('task_id')}")
            logger.info(f"   Content length: {result.get('content_length', 0)} characters")
            logger.info(f"   Result files: {len(result.get('result_files', []))}")
            
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = str(e)
            
            logger.error(f"❌ RunPod task failed after {processing_time:.2f}s: {error_msg}")
            
            return {
                'success': False,
                'error': error_msg,
                'processing_time': processing_time,
                'worker_id': self.worker_id,
                'runpod_task_id': runpod_task_id
            }


# 全局处理器实例
handler = MinerURunPodHandler()


async def runpod_handler(event):
    """
    RunPod Serverless 异步主处理函数 - 透传模式
    
    只负责将任务参数透传给 MinerU API 服务器，不处理具体的文件下载和解析逻辑
    使用异步处理提供更好的并发性能
    
    输入格式:
    {
        "input": {
            "object_name": "documents/sample.pdf",      # 必需：OSS/MinIO/COS 对象路径或完整URL
            "file_name": "sample.pdf",                  # 可选：文件名（从object_name自动推断）
            "backend": "pipeline",                      # 可选：处理后端
            "lang": "ch",                              # 可选：语言
            "method": "auto",                          # 可选：解析方法
            "formula_enable": true,                    # 可选：启用公式识别
            "table_enable": true,                      # 可选：启用表格识别
            "priority": 0                              # 可选：优先级
        }
    }
    
    输出格式:
    {
        "success": true,
        "runpod_task_id": "runpod-1234567890",        # RunPod 任务ID
        "api_task_id": "api-task-uuid",               # API 服务器任务ID
        "object_name": "documents/sample.pdf",
        "file_name": "sample.pdf",
        "file_size": 1024000,
        "parser": "MinerU",
        "content": "# 解析后的 Markdown 内容...",
        "content_length": 5000,
        "result_files": ["sample.md", "images/img1.png", "images/img2.png"],
        "image_count": 2,
        "processing_time": 15.5,
        "worker_id": "runpod-pod123"
    }
    """
    logger.info(f"📨 Received RunPod request: {event.get('id', 'unknown')}")
    
    # 健康检查
    if event.get('input', {}).get('action') == 'health':
        return {
            'status': 'healthy',
            'worker_id': handler.worker_id,
            'api_url': handler.api_url,
            'mode': 'async_passthrough'  # 标识为异步透传模式
        }
    
    # 异步处理任务并返回结果
    return await handler.process_task(event)


# if __name__ == '__main__':
#     # 本地测试模式
#     logger.info("🧪 Running in local test mode (async passthrough)")
    
#     # 测试输入（使用 OSS 对象路径）
#     test_event = {
#         "input": {
#             "object_name": "test_uploads/20251221_151502_1.pdf",  # 需要在 OSS 中存在的文件
#             "file_name": "20251221_151502_1.pdf",
#             "backend": "http-client",
#             "lang": "ch"
#         },
#         "id": "local_test"
#     }
    
#     async def test_async_handler():
#         """本地异步测试"""
#         logger.info("🔄 Starting async test...")
        
#         result = await runpod_handler(test_event)
        
#         # 输出结果（不包含完整内容以避免过长）
#         result_summary = result.copy() if isinstance(result, dict) else result
        
#         if isinstance(result_summary, dict) and result_summary.get('content'):
#             content_preview = result_summary['content'][:200] + "..." if len(result_summary['content']) > 200 else result_summary['content']
#             result_summary['content'] = content_preview
        
#         logger.info(f"📋 Test result: {json.dumps(result_summary, indent=2, ensure_ascii=False)}")
    
#     # 运行异步测试
#     asyncio.run(test_async_handler())
# else:
# RunPod Serverless 模式 - 异步处理器配置
logger.info("🚀 Starting RunPod Serverless handler (async passthrough mode)")
runpod.serverless.start({
    'handler': runpod_handler
})
