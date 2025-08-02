#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新闻公司关联匹配脚本
自动检测新闻内容中提及的公司，并更新数据库
"""

import os
import sys
import json
import logging
import threading
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.supabase_handler import SupabaseHandler


class CompanyMatcher:
    """新闻公司匹配器"""
    
    def __init__(self):
        """初始化匹配器"""
        self.setup_logging()
        self.supabase_handler = None
        self.companies: List[str] = []
        self.company_mapping: Dict[str, str] = {}  # 别名到公司名的映射
        self.processed_count = 0
        self.matched_count = 0
        self.lock = threading.Lock()
        
    def setup_logging(self):
        """设置日志"""
        # 确保日志目录存在
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # 配置日志格式
        log_file = os.path.join(log_dir, f'company_matching_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def init_supabase(self) -> bool:
        """初始化 Supabase 连接"""
        try:
            # 尝试从环境变量获取配置
            supabase_url = os.getenv('SUPABASE_URL')
            supabase_key = os.getenv('SUPABASE_KEY')
            
            if supabase_url and supabase_key:
                # 使用环境变量配置
                config = {
                    'supabase': {
                        'url': supabase_url,
                        'anon_key': supabase_key,
                        'table_name': 'news_items'
                    }
                }
                # 临时写入配置文件
                config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'temp_config.json')
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f)
                
                self.supabase_handler = SupabaseHandler(config_file=config_path, log_callback=self.logger.info)
                
                # 清理临时配置文件
                os.remove(config_path)
            else:
                # 使用本地配置文件
                self.supabase_handler = SupabaseHandler(log_callback=self.logger.info)
            
            if not self.supabase_handler.client:
                self.logger.error("Supabase 客户端初始化失败")
                return False
                
            self.logger.info("Supabase 连接初始化成功")
            return True
            
        except Exception as e:
            self.logger.error(f"初始化 Supabase 连接时出错: {e}")
            return False
    
    def get_companies(self) -> bool:
        """获取所有公司名称和别名"""
        try:
            self.logger.info("开始获取公司列表和别名...")
            
            # 从 companies 表获取公司名称和别名
            response = self.supabase_handler.client.table('companies').select('name, aliases').execute()
            
            if not response.data:
                self.logger.warning("未获取到任何公司数据")
                return False
            
            # 构建公司名称列表和别名映射
            all_terms = []
            self.company_mapping = {}
            
            for company in response.data:
                company_name = company.get('name', '').strip()
                aliases = company.get('aliases', []) or []
                
                if not company_name:
                    continue
                
                # 添加公司名称
                company_name_lower = company_name.lower()
                all_terms.append(company_name_lower)
                self.company_mapping[company_name_lower] = company_name
                
                # 添加所有别名
                if isinstance(aliases, list):
                    for alias in aliases:
                        if alias and isinstance(alias, str):
                            alias_lower = alias.lower().strip()
                            if alias_lower and alias_lower not in self.company_mapping:
                                all_terms.append(alias_lower)
                                self.company_mapping[alias_lower] = company_name
            
            # 去重并过滤空值
            self.companies = list(set(filter(None, all_terms)))
            
            total_aliases = len(self.companies) - len(response.data)
            self.logger.info(f"成功获取 {len(response.data)} 家公司，{total_aliases} 个别名，总计 {len(self.companies)} 个匹配词条")
            return True
            
        except Exception as e:
            self.logger.error(f"获取公司列表时出错: {e}")
            return False
    
    def get_news_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        """分批获取新闻数据"""
        try:
            response = self.supabase_handler.client.table('news_items')\
                .select('id, content')\
                .range(offset, offset + limit - 1)\
                .execute()
            
            return response.data if response.data else []
            
        except Exception as e:
            self.logger.error(f"获取新闻数据时出错 (offset={offset}): {e}")
            return []
    
    def match_companies_in_content(self, content: str) -> List[str]:
        """在内容中匹配公司名称和别名"""
        if not content or not self.companies:
            return []
        
        # 转换为小写进行匹配
        content_lower = content.lower()
        matched_companies = set()  # 使用set避免重复
        
        for term in self.companies:
            if not term:
                continue
                
            # 计算匹配词条在内容中出现的次数
            count = content_lower.count(term)
            
            # 如果出现2次及以上，认为相关
            if count >= 2:
                # 通过映射找到原始公司名称
                original_company = self.company_mapping.get(term, term)
                matched_companies.add(original_company)
        
        return list(matched_companies)
    
    
    def process_news_batch(self, news_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """处理一批新闻数据"""
        results = []
        
        for news in news_batch:
            try:
                news_id = news.get('id')
                content = news.get('content', '')
                
                if not news_id:
                    continue
                
                # 匹配公司
                matched_companies = self.match_companies_in_content(content)
                
                # 准备更新数据
                result = {
                    'id': news_id,
                    'companies': matched_companies
                }
                results.append(result)
                
                # 更新计数器
                with self.lock:
                    self.processed_count += 1
                    if matched_companies:
                        self.matched_count += 1
                
            except Exception as e:
                self.logger.error(f"处理新闻 {news.get('id')} 时出错: {e}")
        
        return results
    
    def update_news_companies(self, updates: List[Dict[str, Any]]) -> int:
        """批量更新新闻的公司字段"""
        if not updates:
            return 0
        
        success_count = 0
        batch_size = 100
        
        try:
            # 分批更新
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]
                
                try:
                    # 使用 upsert 进行批量更新
                    for update in batch:
                        self.supabase_handler.client.table('news_items')\
                            .update({'companies': update['companies']})\
                            .eq('id', update['id'])\
                            .execute()
                        success_count += 1
                        
                except Exception as e:
                    self.logger.error(f"批量更新失败，尝试单个更新: {e}")
                    # 如果批量更新失败，尝试单个更新
                    for update in batch:
                        try:
                            self.supabase_handler.client.table('news_items')\
                                .update({'companies': update['companies']})\
                                .eq('id', update['id'])\
                                .execute()
                            success_count += 1
                        except Exception as e2:
                            self.logger.error(f"更新新闻 {update['id']} 失败: {e2}")
            
            return success_count
            
        except Exception as e:
            self.logger.error(f"批量更新新闻公司字段时出错: {e}")
            return success_count
    
    def run_matching(self):
        """执行匹配任务"""
        start_time = time.time()
        self.logger.info("=" * 50)
        self.logger.info("开始执行新闻公司关联匹配任务")
        self.logger.info("=" * 50)
        
        # 初始化 Supabase 连接
        if not self.init_supabase():
            self.logger.error("Supabase 连接初始化失败，退出任务")
            return False
        
        # 获取公司列表
        if not self.get_companies():
            self.logger.error("获取公司列表失败，退出任务")
            return False
        
        # 获取新闻总数
        try:
            count_response = self.supabase_handler.client.table('news_items')\
                .select('id', count='exact').execute()
            total_news = count_response.count if count_response.count else 0
            self.logger.info(f"数据库中共有 {total_news} 条新闻需要处理")
        except Exception as e:
            self.logger.error(f"获取新闻总数失败: {e}")
            return False
        
        if total_news == 0:
            self.logger.info("没有新闻数据需要处理")
            return True
        
        # 多线程处理配置
        max_workers = 3  # 考虑 Supabase 免费版限制
        batch_size = 1000
        all_updates = []
        
        self.logger.info(f"使用 {max_workers} 个线程，每批处理 {batch_size} 条新闻")
        
        # 分批处理新闻
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            # 提交所有批次任务
            for offset in range(0, total_news, batch_size):
                future = executor.submit(self._process_batch_worker, offset, batch_size)
                futures.append(future)
            
            # 收集结果
            for future in as_completed(futures):
                try:
                    batch_updates = future.result()
                    all_updates.extend(batch_updates)
                    
                    # 输出进度
                    progress = (self.processed_count / total_news) * 100
                    self.logger.info(f"处理进度: {self.processed_count}/{total_news} ({progress:.1f}%)")
                    
                except Exception as e:
                    self.logger.error(f"处理批次时出错: {e}")
        
        # 批量更新数据库
        self.logger.info(f"开始更新数据库，共 {len(all_updates)} 条记录...")
        updated_count = self.update_news_companies(all_updates)
        
        # 输出统计信息
        end_time = time.time()
        duration = end_time - start_time
        
        self.logger.info("=" * 50)
        self.logger.info("任务执行完成")
        self.logger.info(f"处理时间: {duration:.2f} 秒")
        self.logger.info(f"处理的新闻总数: {self.processed_count}")
        self.logger.info(f"匹配到公司的新闻数: {self.matched_count}")
        self.logger.info(f"未匹配到公司的新闻数: {self.processed_count - self.matched_count}")
        self.logger.info(f"成功更新的记录数: {updated_count}")
        self.logger.info(f"匹配成功率: {(self.matched_count/self.processed_count*100):.1f}%" if self.processed_count > 0 else "0%")
        self.logger.info("=" * 50)
        
        return True
    
    def _process_batch_worker(self, offset: int, batch_size: int) -> List[Dict[str, Any]]:
        """批次处理工作线程"""
        try:
            # 获取这一批的新闻数据
            news_batch = self.get_news_batch(offset, batch_size)
            
            if not news_batch:
                return []
            
            # 处理这一批新闻
            batch_updates = self.process_news_batch(news_batch)
            
            return batch_updates
            
        except Exception as e:
            self.logger.error(f"批次工作线程出错 (offset={offset}): {e}")
            return []


def main():
    """主函数"""
    try:
        matcher = CompanyMatcher()
        success = matcher.run_matching()
        
        if success:
            print("新闻公司关联匹配任务执行成功")
            sys.exit(0)
        else:
            print("新闻公司关联匹配任务执行失败")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("任务被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"任务执行时发生异常: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()