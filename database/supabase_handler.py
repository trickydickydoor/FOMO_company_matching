import json
import os
import time
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime
import pytz
from supabase import create_client, Client
import logging

class SupabaseHandler:
    """处理与 Supabase 数据库的所有交互"""
    
    def __init__(self, config_file: str = 'supabase_config.json', log_callback: Optional[Callable] = None):
        """
        初始化 Supabase 处理器
        
        Args:
            config_file: 配置文件路径
            log_callback: 日志回调函数
        """
        self.config_file = config_file
        self.log_callback = log_callback or print
        self.client: Optional[Client] = None
        self.table_name: Optional[str] = None
        self.api_key_type: Optional[str] = None
        
        # 设置日志
        self.logger = logging.getLogger(__name__)
        
        # 加载配置并初始化客户端
        self._load_config()
    
    def _load_config(self) -> bool:
        """加载 Supabase 配置"""
        try:
            # 尝试多个可能的配置文件路径
            possible_paths = [
                self.config_file,
                os.path.join(os.path.dirname(os.path.abspath(__file__)), self.config_file),
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), self.config_file),
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'database', self.config_file),
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'crawler', self.config_file)
            ]
            
            config_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    config_path = path
                    break
            
            if not config_path:
                self.log_callback(f"警告: 未找到 Supabase 配置文件: {self.config_file}")
                return False
            
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # 支持两种配置格式
            if 'supabase' in config:
                # 新格式: {"supabase": {"url": "...", "anon_key": "..."}}
                supabase_config = config['supabase']
                url = supabase_config.get('url', '').strip()
                key = supabase_config.get('anon_key', '').strip()
                self.table_name = supabase_config.get('table_name', 'news_items').strip()
            else:
                # 旧格式: {"url": "...", "key": "..."}
                url = config.get('url', '').strip()
                key = config.get('key', '').strip()
                self.table_name = config.get('table_name', 'news_items').strip()
            
            if not url or not key:
                self.log_callback("警告: Supabase 配置不完整 (URL 或 Key 为空)")
                return False
            
            # 创建 Supabase 客户端
            self.client = create_client(url, key)
            
            # 检测 API Key 类型
            self.api_key_type = self._check_api_key_type(key)
            self.log_callback(f"Supabase 已连接 (API Key 类型: {self.api_key_type})")
            
            return True
            
        except json.JSONDecodeError as e:
            self.log_callback(f"警告: Supabase 配置文件格式错误: {e}")
            return False
        except Exception as e:
            self.log_callback(f"警告: 加载 Supabase 配置失败: {e}")
            return False
    
    def _check_api_key_type(self, api_key: str) -> str:
        """
        检测 API Key 类型
        
        Returns:
            'service_role', 'anon', 或 'unknown'
        """
        try:
            # 解析 JWT
            parts = api_key.split('.')
            if len(parts) != 3:
                return 'unknown'
            
            # 解码 payload (base64url)
            import base64
            payload = parts[1]
            # 添加 padding
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += '=' * padding
            
            decoded = base64.urlsafe_b64decode(payload)
            data = json.loads(decoded)
            
            # 检查 role
            role = data.get('role', '')
            if role == 'service_role':
                return 'service_role'
            elif role == 'anon':
                return 'anon'
            else:
                return 'unknown'
        except Exception:
            return 'unknown'
    
    def _convert_to_utc(self, time_str: str) -> str:
        """
        将东八区时间转换为 UTC 时间
        
        Args:
            time_str: 时间字符串 (格式: YYYY-MM-DD HH:MM:SS)
            
        Returns:
            UTC 时间字符串 (ISO 格式)
        """
        try:
            # 解析时间字符串
            dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            
            # 设置为东八区时间
            tz = pytz.timezone('Asia/Shanghai')
            dt_with_tz = tz.localize(dt)
            
            # 转换为 UTC
            dt_utc = dt_with_tz.astimezone(pytz.UTC)
            
            # 返回 ISO 格式
            return dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        except Exception as e:
            self.logger.error(f"时间转换失败: {e}, 原始时间: {time_str}")
            # 返回原始时间加上时区
            return time_str + '+08:00'
    
    def _clean_data_for_query(self, data: Any) -> Any:
        """
        清理数据以避免 Supabase JSON 生成错误
        
        Args:
            data: 要清理的数据
            
        Returns:
            清理后的数据
        """
        if isinstance(data, dict):
            return {k: self._clean_data_for_query(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._clean_data_for_query(item) for item in data]
        elif isinstance(data, str):
            # 替换可能导致问题的字符
            return data.replace('\x00', '').replace('\r\n', '\n').strip()
        else:
            return data
    
    def _progressive_batch_query(self, table, field_name: str, values: List[str], 
                                initial_batch_size: int = 20) -> set:
        """
        渐进式批处理查询
        
        Args:
            table: Supabase 表对象
            field_name: 要查询的字段名
            values: 要查询的值列表
            initial_batch_size: 初始批处理大小
            
        Returns:
            存在的值集合
        """
        existing = set()
        batch_size = initial_batch_size
        
        i = 0
        while i < len(values):
            batch = values[i:i + batch_size]
            try:
                # 清理批次数据
                clean_batch = [self._clean_data_for_query(v) for v in batch]
                response = table.select(field_name).in_(field_name, clean_batch).execute()
                
                if response.data:
                    existing.update(item[field_name] for item in response.data)
                
                i += batch_size
                # 成功后可以尝试增加批次大小
                if batch_size < initial_batch_size:
                    batch_size = min(batch_size * 2, initial_batch_size)
                    
            except Exception as e:
                error_msg = str(e)
                self.logger.error(f"批量查询失败 (批次大小: {batch_size}): {error_msg}")
                
                # 如果批次大小大于1，减小批次大小重试
                if batch_size > 1:
                    batch_size = max(1, batch_size // 2)
                    self.log_callback(f"减小批次大小到 {batch_size} 并重试...")
                else:
                    # 批次大小为1仍然失败，跳过这个值
                    self.log_callback(f"跳过问题值: {batch[0][:50]}...")
                    i += 1
        
        return existing
    
    def check_existing_items(self, items: List[Dict[str, Any]], 
                           check_fields: List[str] = ['url', 'title']) -> List[Dict[str, Any]]:
        """
        检查并过滤已存在的项目
        
        Args:
            items: 要检查的项目列表
            check_fields: 要检查的字段列表
            
        Returns:
            不存在的项目列表
        """
        if not self.client or not items:
            return items
        
        try:
            table = self.client.table(self.table_name)
            new_items = items.copy()
            
            for field in check_fields:
                if not new_items:
                    break
                
                # 提取所有值
                values = list(set(item.get(field, '') for item in new_items if item.get(field)))
                
                if not values:
                    continue
                
                self.log_callback(f"正在检查 {len(values)} 个{field}...")
                
                # 使用渐进式批处理查询
                existing = self._progressive_batch_query(table, field, values)
                
                # 过滤掉已存在的项目
                before_count = len(new_items)
                new_items = [item for item in new_items if item.get(field) not in existing]
                removed = before_count - len(new_items)
                
                if removed > 0:
                    self.log_callback(f"发现 {removed} 篇文章的{field}已存在，已过滤")
            
            return new_items
            
        except Exception as e:
            self.log_callback(f"检查重复项时出错: {e}")
            return items
    
    def insert_items(self, items: List[Dict[str, Any]], convert_time: bool = True) -> int:
        """
        插入项目到数据库
        
        Args:
            items: 要插入的项目列表
            convert_time: 是否转换时间到 UTC
            
        Returns:
            成功插入的数量
        """
        if not self.client or not items:
            return 0
        
        try:
            table = self.client.table(self.table_name)
            
            # 准备数据
            clean_items = []
            for item in items:
                clean_item = self._clean_data_for_query(item.copy())
                
                # 转换时间
                if convert_time and 'published_at' in clean_item:
                    clean_item['published_at'] = self._convert_to_utc(clean_item['published_at'])
                
                clean_items.append(clean_item)
            
            # 批量插入
            batch_size = 100
            total_uploaded = 0
            
            for i in range(0, len(clean_items), batch_size):
                batch = clean_items[i:i + batch_size]
                try:
                    response = table.insert(batch).execute()
                    if response.data:
                        total_uploaded += len(response.data)
                except Exception as e:
                    self.log_callback(f"批量插入失败: {e}")
                    # 尝试单个插入
                    for item in batch:
                        try:
                            response = table.insert(item).execute()
                            if response.data:
                                total_uploaded += 1
                        except Exception as e2:
                            self.log_callback(f"单个插入失败: {e2}")
            
            return total_uploaded
            
        except Exception as e:
            self.log_callback(f"插入数据时出错: {e}")
            return 0
    
    def upload_with_deduplication(self, items: List[Dict[str, Any]], 
                                check_fields: List[str] = ['url', 'title']) -> Dict[str, int]:
        """
        上传数据并进行去重
        
        Args:
            items: 要上传的项目列表
            check_fields: 用于去重的字段
            
        Returns:
            包含上传统计的字典
        """
        if not self.client:
            return {'total': len(items), 'uploaded': 0, 'duplicates': 0}
        
        # 检查重复
        new_items = self.check_existing_items(items, check_fields)
        duplicates = len(items) - len(new_items)
        
        # 上传新项目
        uploaded = self.insert_items(new_items)
        
        return {
            'total': len(items),
            'uploaded': uploaded,
            'duplicates': duplicates
        }
    
    def get_diagnostic_info(self, error: Exception) -> str:
        """
        获取错误诊断信息
        
        Args:
            error: 异常对象
            
        Returns:
            诊断信息字符串
        """
        error_msg = str(error)
        
        # 错误诊断
        if 'Network' in error_msg or 'Connection' in error_msg:
            return "网络连接问题，请检查网络和 Supabase URL"
        elif '401' in error_msg or 'unauthorized' in error_msg:
            return "认证失败，请检查 API Key 是否正确"
        elif '403' in error_msg or 'forbidden' in error_msg:
            if self.api_key_type == 'anon':
                return "权限不足：anon key 无法写入数据，需要 service_role key"
            else:
                return "权限被拒绝，请检查 RLS 策略"
        elif 'JSON' in error_msg:
            return "数据格式问题，某些字段包含特殊字符"
        elif 'duplicate' in error_msg.lower():
            return "数据重复，某些记录已存在"
        else:
            return f"未知错误: {error_msg}"