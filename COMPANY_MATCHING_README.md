# 新闻公司关联匹配系统

## 功能概述

本系统通过 GitHub Actions 自动化执行新闻与公司的关联匹配，每小时运行一次，检测新闻内容中提及的公司名称，并更新数据库。

## 文件结构

```
.github/workflows/company-matching.yml  # GitHub Actions 工作流
scripts/company_matcher.py             # 核心匹配逻辑
logs/                                   # 日志文件目录
```

## 配置步骤

### 1. GitHub Secrets 配置

在 GitHub 仓库设置中添加以下 Secrets：

- `SUPABASE_URL`: 你的 Supabase 项目 URL
- `SUPABASE_KEY`: 你的 Supabase Service Role Key

### 2. 数据库表结构要求

**companies 表：**
- `name` (text): 公司名称

**news_items 表：**
- `id` (主键): 新闻 ID
- `content` (text): 新闻内容
- `companies` (text[]): 关联的公司列表（数组）

## 匹配规则

- **关键词匹配**: 公司名称在新闻内容中出现 ≥2 次才认为相关
- **大小写不敏感**: 匹配时忽略大小写
- **全量处理**: 每次运行都重新处理所有新闻
- **空数组处理**: 未匹配到公司的新闻会被设置为空数组 `[]`

## 性能参数

- **并发线程数**: 3个（适配 Supabase 免费版）
- **批处理大小**: 1000条新闻/批
- **超时时间**: 5小时
- **适用数据量**: 10万级新闻 + 千级公司

## 运行方式

### 自动运行
- GitHub Actions 每小时自动执行（cron: '0 * * * *'）

### 手动运行
1. 进入 GitHub 仓库的 Actions 页面
2. 选择 "新闻公司关联匹配" 工作流
3. 点击 "Run workflow" 手动触发

### 本地测试
```bash
cd scripts
python company_matcher.py
```

## 日志和监控

- **日志文件**: 存储在 `logs/` 目录
- **GitHub Actions**: 在 Actions 页面查看运行状态
- **日志保留**: GitHub Actions 工件保留7天

## 输出统计

每次运行后会输出：
- 处理时间
- 处理的新闻总数
- 匹配到公司的新闻数
- 未匹配到公司的新闻数
- 成功更新的记录数
- 匹配成功率

## 故障排除

### 常见问题

1. **Supabase 连接失败**
   - 检查 SUPABASE_URL 和 SUPABASE_KEY 是否正确
   - 确认 Service Role Key 有足够权限

2. **任务超时**
   - 检查数据量是否超过预期
   - 考虑调整批处理大小或并发数

3. **内存不足**
   - 减少批处理大小
   - 减少并发线程数

### 错误日志查看

1. GitHub Actions 页面查看实时日志
2. 下载 `matching-logs` 工件查看详细日志

## 扩展性考虑

当数据量增长到百万级时：
- 调整批处理大小（减少到500条/批）
- 考虑升级到付费版 GitHub Actions
- 考虑迁移到其他云服务平台

## 安全注意事项

- API Key 存储在 GitHub Secrets 中，不会暴露在代码中
- 日志文件不包含敏感信息
- 数据库连接使用 HTTPS 加密