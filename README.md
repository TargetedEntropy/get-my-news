# Newsfilter.io API Scraper

A Python-based scraper for the newsfilter.io API that collects financial news articles and stores them in a MySQL database with proper relationship modeling.

## Overview

This scraper is designed to run as a scheduled cron job, respecting API rate limits and ensuring data integrity through proper database relationships. It processes news articles with associated sources, ticker symbols, industries, and sectors.

## Features

- **Rate Limited**: Respects newsfilter.io's 100 API calls per 24-hour limit
- **Single Instance**: Prevents multiple scraper processes from running simultaneously
- **Comprehensive Logging**: Standard logging with statistics reporting
- **Robust Data Model**: Normalized database schema with proper relationships
- **Cron Ready**: Designed for automated execution via crontab
- **Environment Based**: Secure credential management via .env files

## Requirements

- Python 3.8+
- MySQL 5.7+ or MariaDB 10.3+
- newsfilter.io API credentials

## Installation

1. **Clone and setup the project:**
   ```bash
   # Run the provided setup script to create directory structure
   # (See setup commands in project documentation)
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Set up database:**
   ```python
   # Create your MySQL database and update connection string in .env
   # Run initial table creation (see usage section)
   ```

## Configuration

### Environment Variables (.env)

```bash
# API Configuration
NEWSFILTER_API_KEY=your_api_key_here
NEWSFILTER_API_URL=https://api.newsfilter.io

# Database Configuration
DATABASE_URL=mysql+pymysql://username:password@localhost/newsfilter_db

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=logs/scraper.log

# Rate Limiting
MAX_DAILY_REQUESTS=100
RATE_LIMIT_FILE=data/rate_limit.json
```

## Usage

### Manual Execution
```bash
python scraper.py
```

### Cron Setup
```bash
# Add to crontab (runs every hour)
0 * * * * /path/to/python /path/to/newsfilter_scraper/scraper.py
```

### Database Setup
```python
from core.database import DatabaseManager
from models.models import Base

# Initialize database
db_manager = DatabaseManager("your_database_url")
db_manager.create_tables()
```

## Architecture

### System Flow

```mermaid
graph TD
    A[Cron Trigger] --> B[Check Process Lock]
    B -->|Lock Acquired| C[Check Rate Limit]
    B -->|Lock Exists| Z[Exit - Already Running]
    C -->|Under Limit| D[Authenticate API]
    C -->|Over Limit| Y[Exit - Rate Limited]
    D --> E[Fetch Articles]
    E --> F[Process & Store Data]
    F --> G[Update Rate Limit Counter]
    G --> H[Log Statistics]
    H --> I[Release Process Lock]
    I --> J[Exit]
```

### Rate Limiting Flow

```mermaid
graph TD
    A[API Call Request] --> B[Read rate_limit.json]
    B --> C{Check if new day?}
    C -->|Yes| D[Reset counter to 0]
    C -->|No| E[Keep current counter]
    D --> F[Check if counter < 100]
    E --> F
    F -->|Yes| G[Make API Call]
    F -->|No| H[Block API Call]
    G --> I[Increment counter]
    I --> J[Update rate_limit.json]
    H --> K[Log rate limit exceeded]
```

### Data Processing Flow

```mermaid
graph TD
    A[API Response] --> B[Parse Articles Array]
    B --> C[For Each Article]
    C --> D[Extract Source Info]
    C --> E[Extract Symbols]
    C --> F[Extract Industries]
    C --> G[Extract Sectors]
    D --> H[Get/Create Source]
    E --> I[Get/Create Symbols]
    F --> J[Get/Create Industries]
    G --> K[Get/Create Sectors]
    H --> L[Create Article Record]
    I --> L
    J --> L
    K --> L
    L --> M[Save to Database]
    M --> N[Update Statistics]
```

### Database Schema

```mermaid
erDiagram
    articles {
        string id PK
        text title
        text description
        text source_url
        text image_url
        datetime published_at
        string source_id FK
        datetime created_at
        datetime updated_at
    }
    
    sources {
        string id PK
        string name
        datetime created_at
        datetime updated_at
    }
    
    symbols {
        string symbol PK
        datetime created_at
    }
    
    industries {
        string name PK
        datetime created_at
    }
    
    sectors {
        string name PK
        datetime created_at
    }
    
    articles ||--|| sources : "belongs to"
    articles ||--o{ symbols : "mentions"
    articles ||--o{ industries : "relates to"
    articles ||--o{ sectors : "relates to"
```

## Project Structure

```
newsfilter_scraper/
├── .env                    # API credentials and configuration
├── config/                 # Configuration management
├── models/                 # SQLAlchemy database models
├── core/                   # Core functionality
│   ├── api_client.py      # HTTP API client with auth
│   ├── rate_limiter.py    # API usage tracking
│   ├── process_lock.py    # Single instance enforcement
│   └── database.py        # Database management
├── utils/                  # Utilities
│   ├── logger.py          # Logging helpers
│   └── stats.py           # Statistics collection
├── logs/                   # Log files
├── data/                   # Data files (rate limiting, etc.)
└── scraper.py             # Main entry point
```

## Monitoring

The scraper provides comprehensive logging and statistics:

- **Daily API usage tracking**
- **Articles processed per run**
- **Database operation results**
- **Error handling and reporting**
- **Performance metrics**

### Sample Output
```
[2024-06-26 10:00:01] INFO: Scraper started
[2024-06-26 10:00:01] INFO: Rate limit check: 45/100 calls used today
[2024-06-26 10:00:02] INFO: API authentication successful
[2024-06-26 10:00:03] INFO: Fetched 25 articles from API
[2024-06-26 10:00:05] INFO: Processed 25 articles, 3 new, 22 duplicates
[2024-06-26 10:00:05] INFO: Created 2 new sources, 5 new symbols
[2024-06-26 10:00:05] INFO: Rate limit updated: 46/100 calls used today
[2024-06-26 10:00:05] INFO: Scraper completed successfully
```

## Error Handling

- **API Rate Limiting**: Gracefully handles and logs rate limit violations
- **Database Errors**: Rollback transactions on failures
- **Network Issues**: Retry logic with exponential backoff
- **Duplicate Data**: Handles duplicate articles and related entities
- **Process Conflicts**: Prevents multiple instances from running

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Testing

```bash
# Run unit tests
python -m pytest tests/

# Run specific test modules
python -m pytest tests/test_rate_limiter.py
```

## API Documentation

For newsfilter.io API documentation, visit: [newsfilter.io docs](https://newsfilter.io/docs)

## Troubleshooting

### Common Issues

1. **Rate limit exceeded**: Check `data/rate_limit.json` and wait for reset
2. **Database connection errors**: Verify MySQL credentials in `.env`
3. **Multiple instances**: Check for stale lock files in `/tmp/`
4. **API authentication**: Verify API key in `.env` file

### Debug Mode

Enable debug logging by setting `LOG_LEVEL=DEBUG` in your `.env` file.
