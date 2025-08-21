# Financial RAG System with Mistral and SQL Server

This extension to the FinRAG framework adds a Retrieval-Augmented Generation (RAG) system that connects Mistral-7B-Instruct-v0.1 to a SQL Server financial database. The system dynamically extracts metadata from the database schema and uses it to construct SQL queries for answering financial questions.

## Features

- **Dynamic Metadata Extraction**: Automatically loads and caches metadata tables for companies, financial metrics, time periods, etc.
- **Natural Language Query Processing**: Parses financial questions to extract entities like company names, metrics, time periods, and consolidation types.
- **SQL Query Generation**: Dynamically builds SQL queries based on extracted entities and metadata relationships.
- **Mistral Integration**: Uses the Mistral-7B-Instruct-v0.1 model for natural language understanding and response generation.
- **Seamless FinRAG Integration**: Works alongside the existing FinRAG framework for a comprehensive question-answering system.

## Setup

### Prerequisites

1. SQL Server with the financial database (MGFinancials)
2. ODBC Driver 17 for SQL Server
3. Mistral-7B-Instruct-v0.1.Q4_K_M.gguf model file

### Installation

1. Install the required dependencies:

```bash
pip install -r requirements.txt
```

2. Download the Mistral model and place it in the project root:

```bash
# Download the model from HuggingFace or another source
# Place it in the project root directory as Mistral-7B-Instruct-v0.1.Q4_K_M.gguf
```

3. Configure the database connection in `mssql_query.py` if needed:

```python
# Server & database config
server = 'MUHAMMADUSMAN'      # Or your SQL Server name
database = 'MGFinancials'     # Replace with your DB name
```

## Testing

The system includes several test scripts to verify functionality:

1. Test the database connection and metadata loading:

```bash
python test/test_financial_db.py
```

2. Test the Mistral model integration:

```bash
python test/test_mistral.py
```

3. Test the complete Financial RAG system:

```bash
python test/test_financial_rag.py
```

## Usage

Start the FinRAG server:

```bash
python main.py
```

The server will automatically detect financial queries and route them to the Financial RAG system. You can interact with it through the `/chat` endpoint.

### Example Queries

- "What was the EPS of HBL in Q2 2023 (standalone)?"
- "Revenue of OGDC in 2022 full year, consolidated"
- "What is the ROE of UBL for FY 2023?"

## Database Schema

The system relies on the following database tables:

### Metadata Tables

- `tbl_companieslist`: Maps ticker and company_name to internal company_id
- `tbl_industrynames`, `tbl_sectornames`: Map industries and sectors
- `tbl_industryandsectormapping`: Links company_id to industry/sector
- `tbl_unitofmeasurement`: Defines units like PKR, Million, Billion with scale factors
- `tbl_statementsname`: Maps statement categories: Income, Balance Sheet, etc.
- `tbl_headsmaster`: Maps financial line items: Revenue, Net Profit, etc.
- `tbl_ratiosheadmaster`: Defines financial ratios: EPS, ROE, Current Ratio
- `tbl_consolidation`: Flags: standalone vs consolidated
- `tbl_terms`, `tbl_termsmapping`: Defines term logic: 3M, 6M, 9M, 12M → Q1, H1, FY
- `tbl_disectionmaster`: Used for segmented data: product, region, line of business

### Financial Data Tables

- `tbl_financialrawdata`: Raw non-ratio financial data
- `tbl_financialrawdata_Quarter`: Derived quarterly data
- `tbl_financialrawdataTTM`: Trailing Twelve Months data
- `tbl_ratiorawdata`: Ratio data
- `tbl_disectionrawdata`: Dissection data

## Architecture

The Financial RAG system consists of three main components:

1. **FinancialDatabase**: Handles database connections, metadata loading, and query generation.
2. **MistralChat**: Manages the Mistral model for natural language understanding and response generation.
3. **FinancialRAG**: Integrates the database and model components to process financial queries.

The system is integrated with the existing FinRAG server through the `/chat` endpoint, which detects financial queries and routes them to the appropriate component.

## Extending the System

To add support for additional financial metrics or data types:

1. Update the metadata extraction in `financial_db.py`
2. Add new query generation functions as needed
3. Extend the entity extraction in `financial_rag.py`

## Troubleshooting

- **Database Connection Issues**: Verify SQL Server connection settings and ODBC driver installation.
- **Model Loading Errors**: Ensure the Mistral model file is in the correct location and format.
- **Query Processing Errors**: Check the logs for details on entity extraction and query generation issues.

## License

This project is licensed under the same license as the FinRAG framework.