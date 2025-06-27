# HTTP API client with authentication for newsfilter.io

import time
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging


class NewsfilterAPIError(Exception):
    """Custom exception for API-related errors"""
    pass


class NewsfilterAPIClient:
    """Client for interacting with the newsfilter.io API"""
    
    def __init__(self, api_key: str, base_url: str = "https://api.newsfilter.io", 
                 timeout: int = 30, retry_attempts: int = 3, retry_backoff: float = 1.0):
        """
        Initialize the API client
        
        Args:
            api_key: Your newsfilter.io API key
            base_url: Base URL for the API
            timeout: Request timeout in seconds
            retry_attempts: Number of retry attempts for failed requests
            retry_backoff: Backoff multiplier for retries
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_backoff = retry_backoff
        
        self.logger = logging.getLogger(__name__)
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NewsfilterScraper/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        
        self._authenticated = False
    
    def authenticate(self) -> bool:
        """
        Authenticate with the API
        
        Returns:
            bool: True if authentication successful
        """
        try:
            # Test authentication with a simple API call
            response = self._make_request('GET', '/health', authenticate=True)
            
            if response and response.status_code == 200:
                self._authenticated = True
                self.logger.info("API authentication successful")
                return True
            else:
                self.logger.error(f"API authentication failed: {response.status_code if response else 'No response'}")
                return False
                
        except Exception as e:
            self.logger.error(f"Authentication error: {str(e)}")
            return False
    
    def get_articles(self, limit: int = 100, offset: int = 0, 
                    symbol: Optional[str] = None, 
                    source: Optional[str] = None,
                    since: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Fetch articles from the API
        
        Args:
            limit: Maximum number of articles to fetch
            offset: Offset for pagination
            symbol: Filter by ticker symbol (optional)
            source: Filter by source (optional)
            since: Only get articles since this datetime (optional)
        
        Returns:
            List[Dict]: List of article data
        """
        if not self._authenticated:
            if not self.authenticate():
                raise NewsfilterAPIError("Authentication required")
        
        # Build query parameters
        params = {
            'limit': limit,
            'offset': offset
        }
        
        if symbol:
            params['symbol'] = symbol
        if source:
            params['source'] = source
        if since:
            params['since'] = since.isoformat()
        
        try:
            response = self._make_request('GET', '/articles', params=params, authenticate=True)
            
            if response and response.status_code == 200:
                data = response.json()
                articles = data.get('articles', [])
                
                self.logger.info(f"Successfully fetched {len(articles)} articles from API")
                return articles
            else:
                error_msg = f"Failed to fetch articles: {response.status_code if response else 'No response'}"
                if response:
                    try:
                        error_data = response.json()
                        error_msg += f" - {error_data.get('message', 'Unknown error')}"
                    except:
                        error_msg += f" - {response.text}"
                
                raise NewsfilterAPIError(error_msg)
                
        except requests.exceptions.RequestException as e:
            raise NewsfilterAPIError(f"Network error while fetching articles: {str(e)}")
    
    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None, 
                     data: Optional[Dict] = None, authenticate: bool = False) -> Optional[requests.Response]:
        """
        Make an HTTP request with retry logic
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            authenticate: Whether to include authentication
        
        Returns:
            requests.Response: The response object
        """
        url = f"{self.base_url}{endpoint}"
        
        # Add authentication if required
        headers = {}
        if authenticate:
            headers['Authorization'] = f"Bearer {self.api_key}"
        
        for attempt in range(self.retry_attempts + 1):
            try:
                self.logger.debug(f"Making {method} request to {url} (attempt {attempt + 1})")
                
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=data,
                    headers=headers,
                    timeout=self.timeout
                )
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                # Handle server errors with retry
                if response.status_code >= 500:
                    if attempt < self.retry_attempts:
                        wait_time = self.retry_backoff * (2 ** attempt)
                        self.logger.warning(f"Server error {response.status_code}. Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                
                return response
                
            except requests.exceptions.Timeout:
                if attempt < self.retry_attempts:
                    wait_time = self.retry_backoff * (2 ** attempt)
                    self.logger.warning(f"Request timeout. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise NewsfilterAPIError(f"Request timeout after {self.retry_attempts} attempts")
            
            except requests.exceptions.ConnectionError:
                if attempt < self.retry_attempts:
                    wait_time = self.retry_backoff * (2 ** attempt)
                    self.logger.warning(f"Connection error. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise NewsfilterAPIError(f"Connection error after {self.retry_attempts} attempts")
            
            except Exception as e:
                self.logger.error(f"Unexpected error in API request: {str(e)}")
                raise NewsfilterAPIError(f"Unexpected error: {str(e)}")
        
        return None
    
    def get_sources(self) -> List[Dict[str, Any]]:
        """
        Get list of available news sources
        
        Returns:
            List[Dict]: List of source information
        """
        if not self._authenticated:
            if not self.authenticate():
                raise NewsfilterAPIError("Authentication required")
        
        try:
            response = self._make_request('GET', '/sources', authenticate=True)
            
            if response and response.status_code == 200:
                data = response.json()
                return data.get('sources', [])
            else:
                raise NewsfilterAPIError(f"Failed to fetch sources: {response.status_code if response else 'No response'}")
                
        except requests.exceptions.RequestException as e:
            raise NewsfilterAPIError(f"Network error while fetching sources: {str(e)}")
    
    def check_rate_limit_status(self) -> Dict[str, Any]:
        """
        Check current rate limit status
        
        Returns:
            Dict: Rate limit information
        """
        try:
            response = self._make_request('GET', '/rate-limit', authenticate=True)
            
            if response and response.status_code == 200:
                return response.json()
            else:
                return {
                    'remaining': 0,
                    'reset_time': None,
                    'error': f"Failed to check rate limit: {response.status_code if response else 'No response'}"
                }
                
        except Exception as e:
            self.logger.error(f"Error checking rate limit: {str(e)}")
            return {
                'remaining': 0,
                'reset_time': None,
                'error': str(e)
            }
    
    def close(self):
        """Close the session"""
        if self.session:
            self.session.close()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()