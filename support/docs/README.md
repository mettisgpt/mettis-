# FinRAG: Financial Retrieval Augmented Generation

[![](https://dcbadge.vercel.app/api/server/trsr8SXpW5)](https://discord.gg/trsr8SXpW5)

![Visitors](https://api.visitorbadge.io/api/VisitorHit?user=AI4Finance-Foundation&repo=FinRAG&countColor=%23B17A)

## 1. 准备工作

### 1.1 安装minicoda
#### 网址
`https://docs.anaconda.com/free/miniconda/miniconda-other-installer-links/#linux-installers`
#### python3.10.14
`wget https://repo.anaconda.com/miniconda/Miniconda3-py310_24.4.0-0-Linux-x86_64.sh`
#### 配置环境变量
`export PATH=$HOME/miniconda3/bin:$PATH`

### 1.2 启动Milvus向量数据库
- 使用docker-compose启动Milvus服务
```
cd docker     #切换至docker配置目录环境
docker-compose up -d           #启动项目中的服务，并在后台以守护进程方式运行
```
如果对docker不了解，可以看下以下文章: \
[docker-compose快速入门](https://blog.csdn.net/m0_37899908/article/details/131268835) \
[docker-conpose命令解读](https://blog.csdn.net/weixin_42494218/article/details/135986248) \
术语说明: \
  守护进程: 是一类在后台运行的特殊进程，用于执行特定的系统任务,会一直存在。如果以非守护进程启动，服务容易被终止。

- Milvus 前端展示地址
`http://{ip}:3100/#/` 把ip替换为你所在服务器的ip地址即可

### 1.3 Embedding以及Rerank模型下载
- 新建/data/WoLLM 目录
- 将以下两个模型下载到新建的目录中
- Embedding Model 下载：`git clone https://www.modelscope.cn/maidalun/bce-embedding-base_v1.git`
- Rerank Model 下载：`git clone https://www.modelscope.cn/maidalun/bce-reranker-base_v1.git`
说明: \
  Embedding Model: 主要是完成将自然语言文本转化为固定维度向量的工作，主要在知识库的建模，用户查询query表示时会应用。 \
  Rerank Model：对结果进行重排操作。 \
  这里采用的都是bce的模型，因为其在RAG上表现较好，可以参考资料，了解一下背景: \
  [BCE Embedding技术报告](https://zhuanlan.zhihu.com/p/681370855) \
  下载好两个模型后, 将模型放到指定的位置，并更新项目conf.config.py文件中EMBEDDING_MODEL和RERANK_MODEL对应参数的路径(和模型路径保持一致)。如图:
![img.png](img.png)


### 1.4 安装依赖及修改配置信息
#### 新建python虚拟环境
`python -m venv .venv`
#### 激活环境
`source .venv/bin/activate`
#### 安装项目依赖
`pip install -r requirements.txt`

### 1.5 修改配置文件

- 配置文件在conf/config.py
- 配置文件的各项信息修改为自己的信息, 每个变量已经加了详细注释.
- 可能需要改动的参数一般就是两个模型文件目录，如图:
![img_1.png](img_1.png)
- 如果你需要修改端口，或者服务器变更，你需要修改docker.docker-compose.yml中的配置参数，一般就是修改ip和端口。
![img_2.png](img_2.png)

### 1.6 修改环境变量

- 将.env_user复制为.env `cp .env_user .env` [重要,重要,重要. 必须复制一下]
- 修改.env的LLM以及OSS相关变量信息(目前只需要复制一下即可, 不需要修改里面的内容了)

## 2. 启动App
### 2.1 第一种方式启动
`python main.py`
### 2.2 第二种方式启动(为之后打进docker做准备,目前先用第一种方式)
`bin/start.sh`


ToDo
- [ ] rag 优化
  - [ ] 多级索引优化
  - [ ] 多查询优化
  - [ ] query优化
  - [ ] 解析优化

## 3. Financial Database Query System Improvements

### 3.1 Problem Description

The original implementation of the `get_head_id` method in `financial_db.py` had several critical limitations when querying financial data:

1. **Missing Data**: It returned a `SubHeadID` that doesn't have any data for certain combinations of company, metric, period, and consolidation type. For example, when querying for "Depreciation and Amortisation" data for UBL (United Bank Limited), the method returns `SubHeadID` 480, but there's no data available for this ID.

2. **Incorrect Metric Matching**: Some metrics were incorrectly matched due to simple string matching without industry context validation.

3. **Invalid Join Conditions**: The SQL queries used incorrect table relationships, leading to missing or inaccurate data.

4. **Lack of Industry Validation**: Metrics were matched without considering industry relevance.

However, data exists for the same metrics under different `SubHeadID`s that weren't being considered.

### 3.2 Comprehensive Solution

A series of improvements have been implemented to address these issues:

#### 3.2.1 Metric Matching Fix

A new function `get_available_head_id` has been implemented in `fix_head_id.py` that:

1. Retrieves all possible `SubHeadID`s for a given metric name from both regular heads (`tbl_headsmaster`) and ratio heads (`tbl_ratiosheadmaster`)
2. Validates metrics against company industry/sector information
3. Checks each `SubHeadID` to see if it actually has data for the specified company, period, and consolidation
4. Returns the first `SubHeadID` that has data

#### 3.2.2 Dynamic Period Resolution

Enhanced date handling with:
- Support for natural language date references like "last quarter" or "fiscal year 2022"
- Company-specific fiscal year awareness
- Flexible period formats (quarters, years, specific dates)

#### 3.2.3 Improved Query Approach

Redesigned the query construction process with:
- Proper table relationships and join conditions
- Metadata-first approach for efficient queries
- Specialized handling for different data types (regular, quarterly, TTM, dissection)

#### 3.2.4 Bug Fixes

- Fixed duplicate `term_id` assignment in query functions
- Corrected invalid SQL join conditions
- Improved error handling and logging

### 3.3 Usage

Instead of using the original `get_head_id` method directly, use the new `get_available_head_id` function when you need to ensure that the returned `SubHeadID` actually has data:

```python
from app.core.database.fix_head_id import get_available_head_id

# Get company ID, period_end, and consolidation_id as usual
company_id = db.get_company_id('UBL')
period_end = db._format_date('31-3-2021')
consolidation_id = db.get_consolidation_id('Unconsolidated')

# Use the fixed method to get a head_id that has data
head_id, is_ratio = get_available_head_id(db, company_id, 'Depreciation and Amortisation', period_end, consolidation_id)
```

### 3.4 Testing

Several test scripts have been created to validate the improvements:

#### 3.4.1 Metric Matching Tests

The `test_ubl_depreciation.py` script demonstrates the metric matching issue and solution:

1. It first uses the original `get_head_id` method, which returns `SubHeadID` 480, but no data is found
2. It then uses the new `get_available_head_id` function, which returns `SubHeadID` 139, and successfully retrieves data
3. It also shows all `SubHeadID`s with 'Depreciation' or 'Amortisation' in the name, along with their data counts

#### 3.4.2 Dynamic Period Resolution Tests

The `test_dynamic_period.py` script validates the dynamic period resolution functionality:

1. Tests various natural language date references ("last quarter", "current fiscal year", etc.)
2. Verifies that company-specific fiscal years are correctly handled
3. Confirms that different period formats are properly resolved

#### 3.4.3 Query Approach Tests

The `test_improved_query.py` script tests the improved query construction:

1. Validates proper table relationships and join conditions
2. Tests specialized handling for different data types
3. Verifies that industry validation is correctly applied

### 3.5 Documentation

Detailed documentation is available in the `docs` directory:

- [Main Documentation](docs/index.md)
- [Metric Matching Fix](docs/index.md#metric-matching-fix)
- [Dynamic Period Resolution](docs/dynamic_period_resolution.md)
- [Improved Query Approach](docs/improved_query_approach.md)
- [Duplicate Term ID Fix](docs/duplicate_term_id_fix.md)
- [Financial Database Fix](docs/financial_db_fix.md)
- [SQL Query Fix](README_SQL_QUERY_FIX.md)
