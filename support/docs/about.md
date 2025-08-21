# FinRAG – Financial Retrieval Augmented Generation

FinRAG is an end-to-end platform that answers natural-language questions about corporate financials by combining:

* **Structured data** from a Microsoft SQL Server instance (MGFinancials)
* **Large Language Models (LLMs)** – inference with Mistral-7B-Instruct
* **Semantic search** via Milvus vector database
* **Retrieval-Augmented Generation (RAG)** orchestration for accurate, explainable results

---
## 1  High-Level Workflow

1. **User query** ▶ *CLI* or *FastAPI* endpoint.
2. **Entity extraction** (<mcsymbol name="_extract_entities" filename="financial_rag.py" path="app/core/rag/financial_rag.py" startline="100" type="function"></mcsymbol>) identifies company, metric, term (period), and consolidation.
3. **SQL construction** (<mcsymbol name="build_financial_query" filename="financial_db.py" path="app/core/database/financial_db.py" startline="740" type="function"></mcsymbol>) uses metadata to tailor joins and filters.
4. **Data retrieval** ▶ results returned as Pandas `DataFrame`.
5. **LLM generation** ▶ formatted answer with context, units, and date.

---
## 2  Project Structure

| Layer | Folder | Key Files | Purpose |
|-------|--------|-----------|---------|
| API | `app/finrag_server.py` | FastAPI endpoints (`/ask`, `/status`) | Expose RAG services |
| RAG Core | `app/core/rag/` | `financial_rag.py` | Orchestrates query → SQL → answer |
| Database | `app/core/database/` | `financial_db.py` | Metadata loading, dynamic SQL |
| Chat/LLM | `app/core/chat/` | `mistral_chat.py` | Mistral 7B wrapper via ctransformers |
| Vector Store | `app/core/vectorstore/` | `customer_milvus_client.py` | Milvus operations |
| Docs | `docs/` | How-to & design notes | Documentation |
| Tests | `test/`, `tests/` | Pytest suites | Regression coverage |

---
## 3  Key Features (2025-08 Release)

### 3.1 Relative-Period Resolution
Natural phrases like **“latest quarter”** or **“last reported annual”** map to valid `TermID` & `PeriodEnd` values; logic lives in <mcsymbol name="get_term_id" filename="financial_db.py" path="app/core/database/financial_db.py" startline="300" type="function"></mcsymbol>.

### 3.2 Automatic Head ID Validation
Before querying, <mcsymbol name="get_available_head_id" filename="fix_head_id.py" path="app/core/database/fix_head_id.py" startline="1" type="function"></mcsymbol> ensures the chosen metric actually has data for the target company, reducing empty‐result errors.

### 3.3 Enhanced Logging & Error Handling
`loguru` captures company/sector mapping, consolidation, resolved dates, and SQL text to `logs/` for quick diagnosis.

### 3.4 Period-End Direct Query
Users can specify literal dates (e.g., **“Revenue on 2023-12-31”**). See `PERIOD_END_QUERY.md` for patterns and examples.

### 3.5 Trailing-Twelve-Months (TTM) Queries
FinRAG now understands TTM requests, automatically selecting `tbl_financialrawdataTTM` when available (with graceful fallback to `tbl_financialrawdata`). Relative terms such as **“ttm”**, **“trailing twelve months”**, or **“last 12 months”** are detected by the entity extractor and resolved to the latest TTM period.

Key points:
* Entity extraction enhanced to label `relative_term_type="ttm"`.
* <mcsymbol name="get_term_id" filename="financial_db.py" path="app/core/database/financial_db.py" startline="300" type="function"></mcsymbol> determines the correct `TermID` & `PeriodEnd` for TTM.
* <mcsymbol name="build_financial_query" filename="financial_db.py" path="app/core/database/financial_db.py" startline="740" type="function"></mcsymbol> dynamically swaps tables based on a `is_ttm_query` flag.

---
## 4  Database Schema Snapshot

* **Metadata**: `tbl_companieslist`, `tbl_headsmaster`, `tbl_terms`, `tbl_consolidation`, etc.
* **Financials**: `tbl_financialrawdata`, `tbl_financialrawdata_Quarter`, `tbl_financialrawdataTTM`
* **Ratios**: `tbl_ratiorawdata`
* **Vector Store**: Milvus manages embeddings for unstructured docs / Q&A history.

A full ER diagram is available in `docs/index.md`.

---
## 5  Setup Guide

### 5.1 Prerequisites
* **Python** 3.10-3.11
* **SQL Server** with MGFinancials data
* **Docker + Docker Compose** (for Milvus)
* **Hardware**: ≥ 8 GB RAM; ≥ 10 GB disk

### 5.2 Installation
```bash
# 1 Clone
$ git clone https://github.com/AI4Finance-Foundation/FinRAG.git
$ cd FinRAG

# 2 Virtual env
$ python -m venv .venv
$ .venv\Scripts\activate            # PowerShell

# 3 Dependencies
$ pip install -r requirements.txt

# 4 Model file (place in repo root)
$ wget https://huggingface.co/.../Mistral-7B-Instruct-v0.1.Q4_K_M.gguf

# 5 Vector store
$ cd docker && docker-compose up -d
```

### 5.3 Run the Server
```bash
$ python main.py
# → FastAPI docs at http://localhost:8000/docs
```

### 5.4 CLI Quick Start
```bash
$ python financial_rag_cli.py --query "Net Profit for Atlas Honda in latest quarter"
```

---
## 6  Testing & CI

Run **all** unit tests:
```bash
$ pytest -q
```
Key coverage areas:
* `test_financial_db.py` – metadata load & SQL generation
* `test_financial_rag.py` – end-to-end RAG
* `test_period_end_*` – date parsing robustness

---
## 7  Deployment Notes

* Production inference can swap local LLM for an **OpenAI**-compatible endpoint by adjusting `conf/config.py`.
* Use environment variables (`FINRAG_DB_CONN`, `FINRAG_MODEL_PATH`, etc.) for container orchestration.
* Example Dockerfiles are provided for the API and worker services.

---
## 8  Contributing

1. Fork ➜ create feature branch.
2. Follow *black* + *isort* formatting.
3. Add/extend tests, update docs.
4. Submit PR; GitHub Actions will run the test matrix.

---
## 9  License

FinRAG is released under the **MIT License**.  See [`LICENSE`](LICENSE) for details.