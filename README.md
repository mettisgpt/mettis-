# FinRAG - Financial Retrieval Augmented Generation System

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

FinRAG is an advanced Financial Retrieval Augmented Generation (RAG) system that combines natural language processing with sophisticated database querying capabilities to provide intelligent financial data analysis and insights.

## 🚀 Features

### Core Capabilities
- **Natural Language Querying**: Ask financial questions in plain English
- **Comprehensive Financial Data Access**: Query company financials, ratios, and dissection metrics
- **Advanced Entity Extraction**: Intelligent recognition of companies, metrics, time periods, and consolidation types
- **Multi-Interface Support**: Command-line, API server, and enhanced debug interfaces
- **Real-time Data Processing**: Direct SQL Server integration with optimized queries
- **Dissection Data Support**: Per-share metrics, annual growth rates, percentage calculations
- **Dynamic Period Resolution**: Flexible handling of fiscal years, quarters, and relative periods
- **Comprehensive Logging**: Detailed debugging and monitoring capabilities

### Advanced Features
- **Enhanced Query Processing**: Sophisticated metric resolution and data validation
- **Fallback Mechanisms**: Intelligent suggestions when exact matches aren't found
- **Multiple Data Types**: Financial statements, financial ratios, and dissection analytics
- **Consolidation Support**: Consolidated and unconsolidated financial data
- **Period Flexibility**: Support for specific dates, fiscal years, quarters, and relative periods (YTD, TTM, etc.)

## 📋 Prerequisites

- **Python 3.9 or higher**
- **Microsoft SQL Server** with financial database
- **Mistral-7B-Instruct language model** (Q4_K_M quantized version)
- **Required Python packages** (see requirements.txt)

### Database Requirements
The system expects a SQL Server database with the following key tables:
- `tbl_financialrawdata_Quarter` - Quarterly financial statement data
- `tbl_ratiorawdata` - Financial ratios and calculated metrics
- `tbl_disectionrawdata` - Dissection/per-share analytics
- `tbl_headsmaster` - Metric definitions and metadata
- `tbl_company` - Company information and identifiers
- `tbl_terms` - Financial periods and terms

## 🛠️ Installation

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/FinRAG.git
cd FinRAG
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Download Language Model
Download the Mistral-7B-Instruct-v0.1.Q4_K_M.gguf model and place it in the project root directory.

### 4. Configure Database Connection
Update the database configuration in `conf/config.py`:
```python
# Database configuration
DATABASE_CONFIG = {
    'server': 'your_server_name',
    'database': 'your_database_name',
    'username': 'your_username',
    'password': 'your_password'
}
```

## 🚀 Usage

### Command Line Interface

#### Basic Usage
```bash
python financial_rag_cli.py "What is the debt to equity ratio of UBL for FY 2022?"
```

#### Example Queries
```bash
# Financial statement queries
python financial_rag_cli.py "Show me total assets of HBL for Q1 2023"

# Ratio queries
python financial_rag_cli.py "What is the ROE of MCB Bank for fiscal year 2022?"

# Dissection queries
python financial_rag_cli.py "What is the earnings per share of UBL for 2022?"
python financial_rag_cli.py "Show annual growth rate of revenue for BAFL"
```

### API Server

#### Standard Server
```bash
python -m app.finrag_server
```
Access at: http://localhost:8000

#### Enhanced Debug Server
```bash
python support/debug/enhanced_finrag_server.py
```
Access at: http://localhost:8000 (with comprehensive logging)

#### API Endpoints
- `POST /query` - Submit financial queries
- `GET /health` - Health check endpoint
- `GET /metrics` - System metrics and statistics

### Docker Deployment
```bash
# Build the container
docker build -t finrag .

# Run with docker-compose
cd docker
docker-compose up -d
```

## 🏗️ Architecture

### Core Components

#### 1. Application Layer (`app/`)
- **`finrag_server.py`**: FastAPI-based web server
- **`core/`**: Core functionality modules
  - `database/`: Database connection and query management
  - `rag/`: RAG implementation and processing
  - `chat/`: Conversation management
  - `process_query.py`: Main query processing pipeline
- **`models/`**: Data models for API responses and internal structures

#### 2. Enhanced Processing Engine (`support/debug/`)
- **`enhanced_financial_rag.py`**: Advanced RAG implementation with:
  - Sophisticated entity extraction
  - Multi-type metric resolution
  - Dynamic period handling
  - Comprehensive error handling
- **`enhanced_finrag_server.py`**: Debug-enabled server with detailed logging

#### 3. Configuration (`conf/`)
- **`config.py`**: Database connections, model settings, and system parameters

#### 4. Utilities (`utils.py`)
- Logging configuration
- Helper functions
- Common utilities

### Data Flow

1. **Query Input**: User submits natural language query
2. **Entity Extraction**: System identifies companies, metrics, periods, and consolidation types
3. **Metric Resolution**: Maps natural language terms to database identifiers
4. **Query Building**: Constructs optimized SQL queries based on data type
5. **Data Retrieval**: Executes queries against financial database
6. **Response Generation**: Formats results into natural language responses
7. **Output Delivery**: Returns formatted response to user

## 🧪 Testing

### Comprehensive Test Suite
The system includes extensive testing capabilities in `support/tests/`:

#### Test Categories
- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end query processing
- **Database Tests**: SQL query validation
- **Dissection Tests**: Specialized per-share and growth rate queries
- **Period Resolution Tests**: Date and fiscal year handling
- **Entity Extraction Tests**: Company and metric recognition

#### Running Tests
```bash
# Run all tests
python support/debug/run_all_tests.py

# Run specific test categories
python support/tests/test_dissection_queries.py
python support/tests/test_enhanced_rag.py
python support/tests/test_dynamic_period_resolution.py

# Run dissection-specific tests
python support/tests/run_dissection_tests.py
```

### Debug and Development Tools
- **`support/debug/debug_entity_extraction.py`**: Entity extraction testing
- **`support/debug/debug_metric_matching.py`**: Metric resolution debugging
- **`support/debug/get_schema.py`**: Database schema exploration
- **`support/debug/mssql_query.py`**: Direct SQL query testing

## 📊 Supported Query Types

### 1. Financial Statement Queries
- Balance sheet items (assets, liabilities, equity)
- Income statement items (revenue, expenses, profit)
- Cash flow statement items

**Examples:**
- "What are the total assets of UBL for Q4 2022?"
- "Show me the revenue of HBL for fiscal year 2023"
- "What is the net profit of MCB Bank for Q2 2022?"

### 2. Financial Ratio Queries
- Profitability ratios (ROE, ROA, profit margins)
- Liquidity ratios (current ratio, quick ratio)
- Leverage ratios (debt-to-equity, debt-to-assets)
- Efficiency ratios (asset turnover, inventory turnover)

**Examples:**
- "What is the ROE of BAFL for 2022?"
- "Show me the debt to equity ratio of UBL"
- "What is the current ratio of HBL for Q1 2023?"

### 3. Dissection/Per-Share Queries
- Earnings per share (EPS)
- Book value per share
- Dividend per share
- Annual growth rates
- Percentage metrics

**Examples:**
- "What is the EPS of UBL for 2022?"
- "Show me the annual growth rate of revenue for MCB Bank"
- "What percentage of assets does cash represent for HBL?"

### 4. Time Period Flexibility
- Specific dates ("31-Dec-2022")
- Fiscal years ("FY 2022", "2022")
- Quarters ("Q1 2023", "first quarter 2023")
- Relative periods ("latest", "most recent", "YTD", "TTM")

## 🔧 Configuration Options

### Database Configuration
```python
# conf/config.py
DATABASE_CONFIG = {
    'server': 'localhost',
    'database': 'FinancialDB',
    'driver': 'ODBC Driver 17 for SQL Server',
    'trusted_connection': 'yes'
}
```

### Model Configuration
```python
MODEL_CONFIG = {
    'model_path': 'mistral-7b-instruct-v0.1.Q4_K_M.gguf',
    'max_tokens': 2048,
    'temperature': 0.1,
    'context_length': 4096
}
```

### Logging Configuration
```python
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'handlers': ['file', 'console']
}
```

## 📁 Project Structure

```
FinRAG/
├── app/                          # Main application
│   ├── core/                     # Core modules
│   │   ├── database/             # Database connections
│   │   ├── rag/                  # RAG implementation
│   │   ├── chat/                 # Chat management
│   │   └── process_query.py      # Query processing
│   ├── models/                   # Data models
│   └── finrag_server.py          # API server
├── support/                      # Support tools and enhanced features
│   ├── debug/                    # Debug and development tools
│   │   ├── enhanced_financial_rag.py    # Enhanced RAG engine
│   │   ├── enhanced_finrag_server.py    # Debug server
│   │   └── [various debug tools]
│   ├── tests/                    # Comprehensive test suite
│   └── docs/                     # Documentation
├── conf/                         # Configuration
├── logs/                         # Log files
├── docs/                         # Project documentation
├── examples/                     # Usage examples
├── financial_rag_cli.py          # Command-line interface
├── requirements.txt              # Python dependencies
├── Dockerfile                    # Container configuration
└── README.md                     # This file
```

## 🔍 Advanced Features

### Entity Extraction Engine
The system uses sophisticated NLP techniques to extract:
- **Companies**: Fuzzy matching with company database
- **Metrics**: Financial statement items, ratios, and dissection metrics
- **Time Periods**: Flexible date parsing and fiscal year resolution
- **Consolidation Types**: Automatic detection of consolidated vs. unconsolidated requests

### Query Optimization
- **Intelligent Caching**: Frequently accessed data caching
- **Query Planning**: Optimized SQL generation based on data type
- **Fallback Strategies**: Alternative suggestions when exact matches fail
- **Data Validation**: Comprehensive validation of query results

### Error Handling
- **Graceful Degradation**: System continues operating with partial failures
- **Detailed Error Messages**: Clear explanations of issues and suggestions
- **Comprehensive Logging**: Full audit trail of query processing
- **Recovery Mechanisms**: Automatic retry and alternative approaches

## 📈 Performance Considerations

### Optimization Strategies
- **Database Indexing**: Optimized indexes for common query patterns
- **Connection Pooling**: Efficient database connection management
- **Query Caching**: Results caching for frequently requested data
- **Lazy Loading**: On-demand loading of large datasets

### Scalability Features
- **Horizontal Scaling**: Support for multiple server instances
- **Load Balancing**: Distribution of query load
- **Asynchronous Processing**: Non-blocking query execution
- **Resource Management**: Efficient memory and CPU utilization

## 🛡️ Security

### Data Protection
- **SQL Injection Prevention**: Parameterized queries and input validation
- **Access Control**: Role-based access to financial data
- **Audit Logging**: Complete audit trail of data access
- **Encryption**: Data encryption in transit and at rest

### Authentication & Authorization
- **API Key Management**: Secure API access control
- **User Authentication**: Integration with enterprise authentication systems
- **Permission Management**: Granular access control to different data types

## 🚨 Troubleshooting

### Common Issues

#### Database Connection Issues
```bash
# Check database connectivity
python support/debug/mssql_query.py

# Verify database schema
python support/debug/get_schema.py
```

#### Model Loading Issues
- Ensure Mistral model file is in the correct location
- Check file permissions and disk space
- Verify model file integrity

#### Query Processing Issues
```bash
# Debug entity extraction
python support/debug/debug_entity_extraction.py

# Test metric matching
python support/debug/debug_metric_matching.py

# Check period resolution
python support/tests/test_dynamic_period_resolution.py
```

### Logging and Monitoring
- **Log Files**: Check `logs/` directory for detailed error information
- **Debug Mode**: Use enhanced server for comprehensive logging
- **Health Checks**: Monitor system health via `/health` endpoint

## 🤝 Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch
3. Install development dependencies: `pip install -r requirements-dev.txt`
4. Run tests: `python support/debug/run_all_tests.py`
5. Submit a pull request

### Code Standards
- Follow PEP 8 style guidelines
- Include comprehensive tests for new features
- Update documentation for API changes
- Ensure backward compatibility

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Mistral AI** for the language model
- **FastAPI** for the web framework
- **SQLAlchemy** for database ORM
- **Contributors** and the open-source community

## 📞 Support

For support, questions, or feature requests:
- Create an issue on GitHub
- Check the documentation in `support/docs/`
- Review the test examples in `support/tests/`

---

**FinRAG** - Transforming financial data analysis through intelligent natural language processing.