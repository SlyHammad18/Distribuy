import os
import logging
from functools import wraps
from datetime import datetime, timedelta
from flask import request, jsonify
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

# Configure logging
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'application.log')),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class Config:
    """Application configuration"""
    # JWT
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'your-super-secret-key-change-in-production')
    JWT_EXPIRATION_HOURS = 24

    # Database connections
    # User Service
    USER_DB_HOST = os.getenv('USER_DB_HOST', 'localhost')
    USER_DB_PORT = int(os.getenv('USER_DB_PORT', 15432))
    USER_DB_NAME = os.getenv('USER_DB_NAME', 'user_service_db')
    USER_DB_USER = os.getenv('USER_DB_USER', 'postgres')
    USER_DB_PASSWORD = os.getenv('USER_DB_PASSWORD', 'postgres')
    USER_DB_REPLICA_HOST = os.getenv('USER_DB_REPLICA_HOST', 'localhost')
    USER_DB_REPLICA_PORT = int(os.getenv('USER_DB_REPLICA_PORT', 15433))

    # Order Service
    ORDER_DB_HOST = os.getenv('ORDER_DB_HOST', 'localhost')
    ORDER_DB_PORT = int(os.getenv('ORDER_DB_PORT', 15434))
    ORDER_DB_NAME = os.getenv('ORDER_DB_NAME', 'order_service_db')
    ORDER_DB_USER = os.getenv('ORDER_DB_USER', 'postgres')
    ORDER_DB_PASSWORD = os.getenv('ORDER_DB_PASSWORD', 'postgres')
    ORDER_DB_REPLICA_HOST = os.getenv('ORDER_DB_REPLICA_HOST', 'localhost')
    ORDER_DB_REPLICA_PORT = int(os.getenv('ORDER_DB_REPLICA_PORT', 15435))

    # Inventory Service
    INVENTORY_DB_HOST = os.getenv('INVENTORY_DB_HOST', 'localhost')
    INVENTORY_DB_PORT = int(os.getenv('INVENTORY_DB_PORT', 15436))
    INVENTORY_DB_NAME = os.getenv('INVENTORY_DB_NAME', 'inventory_service_db')
    INVENTORY_DB_USER = os.getenv('INVENTORY_DB_USER', 'postgres')
    INVENTORY_DB_PASSWORD = os.getenv('INVENTORY_DB_PASSWORD', 'postgres')
    INVENTORY_DB_REPLICA_HOST = os.getenv('INVENTORY_DB_REPLICA_HOST', 'localhost')
    INVENTORY_DB_REPLICA_PORT = int(os.getenv('INVENTORY_DB_REPLICA_PORT', 15437))

    # MongoDB
    MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    MONGO_PORT = int(os.getenv('MONGO_PORT', 37017))
    MONGO_SECONDARY_HOST = os.getenv('MONGO_SECONDARY_HOST', 'localhost')
    MONGO_SECONDARY_PORT = int(os.getenv('MONGO_SECONDARY_PORT', 37018))
    MONGO_USER = os.getenv('MONGO_USER', 'admin')
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'admin')
    MONGO_DB = os.getenv('MONGO_DB', 'product_catalog_db')

class DatabaseConnection:
    """Helper class for database connections"""
    
    @staticmethod
    def get_postgres_connection(
        host,
        port,
        database,
        user,
        password,
        timeout=5,
        max_retries=3,
        retry_delay=1,
        log_retries=True,
    ):
        """Get PostgreSQL connection with configurable retry logic."""
        import time
        
        for attempt in range(max_retries):
            try:
                try:
                    import psycopg2
                    conn = psycopg2.connect(
                        host=host,
                        port=port,
                        database=database,
                        user=user,
                        password=password,
                        connect_timeout=timeout
                    )
                    conn.autocommit = True
                    logger.info(f"Connected to PostgreSQL at {host}:{port}/{database} using psycopg2")
                    return conn
                except Exception as psycopg2_error:
                    logger.warning(f"psycopg2 unavailable, trying pg8000 fallback: {str(psycopg2_error)}")
                    import pg8000.dbapi as pg8000
                    conn = pg8000.connect(
                        host=host,
                        port=port,
                        database=database,
                        user=user,
                        password=password,
                        timeout=timeout,
                    )
                    conn.autocommit = True
                    conn._use_function_select = True
                    logger.info(f"Connected to PostgreSQL at {host}:{port}/{database} using pg8000")
                    return conn
            except Exception as e:
                attempt_num = attempt + 1
                if attempt_num < max_retries:
                    if log_retries:
                        logger.warning(f"PostgreSQL connection attempt {attempt_num} failed: {str(e)}. Retrying in {retry_delay}s...")
                    if retry_delay > 0:
                        time.sleep(retry_delay)
                else:
                    if log_retries:
                        logger.error(f"Failed to connect to PostgreSQL after {max_retries} attempts: {str(e)}")
                    raise

    @staticmethod
    def get_mongo_connection(host, port, database, user, password):
        """Get MongoDB connection"""
        from pymongo import MongoClient
        
        try:
            connection_string = f"mongodb://{user}:{password}@{host}:{port}/{database}?authSource=admin&retryWrites=true"
            client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
            # Test connection
            client.admin.command('ping')
            db = client[database]
            logger.info(f"Connected to MongoDB at {host}:{port}/{database}")
            return db
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

class TokenManager:
    """Token management using Flask's built-in itsdangerous serializer"""

    @staticmethod
    def _serializer():
        return URLSafeTimedSerializer(Config.JWT_SECRET_KEY)
    
    @staticmethod
    def generate_token(user_id, email, is_admin=False):
        """Generate a signed access token"""
        payload = {
            'user_id': user_id,
            'email': email,
            'is_admin': is_admin,
            'iat': datetime.utcnow().isoformat()
        }
        return TokenManager._serializer().dumps(payload)

    @staticmethod
    def verify_token(token):
        """Verify a signed access token"""
        try:
            payload = TokenManager._serializer().loads(
                token,
                max_age=Config.JWT_EXPIRATION_HOURS * 3600,
            )
            return payload
        except SignatureExpired:
            logger.warning("Token expired")
            return None
        except BadSignature as e:
            logger.warning(f"Invalid token: {str(e)}")
            return None

def require_auth(f):
    """Decorator to require authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = None
        
        # Check for token in Authorization header
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            try:
                token = auth_header.split(" ")[1]
            except IndexError:
                return jsonify({'error': 'Invalid authorization header format'}), 401
        
        if not token:
            return jsonify({'error': 'Authentication token is missing'}), 401
        
        payload = TokenManager.verify_token(token)
        if not payload:
            return jsonify({'error': 'Invalid or expired token'}), 401
        
        # Store user info in request context
        request.user_id = payload.get('user_id')
        request.email = payload.get('email')
        request.is_admin = payload.get('is_admin', False)
        
        return f(*args, **kwargs)
    
    return decorated_function

def require_admin(f):
    """Decorator to require admin authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not hasattr(request, 'is_admin') or not request.is_admin:
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    
    return decorated_function

def log_request_response(f):
    """Decorator to log request and response"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        logger.info(f"Request: {request.method} {request.path}")
        try:
            response = f(*args, **kwargs)
            return response
        except Exception as e:
            logger.error(f"Error in {request.path}: {str(e)}")
            return jsonify({'error': 'Internal server error'}), 500
    
    return decorated_function

def json_response(status_code=200, data=None, error=None, message=None):
    """Helper to create JSON responses"""
    response = {
        'status': 'success' if status_code < 400 else 'error',
        'code': status_code
    }
    if data:
        response['data'] = data
    if error:
        response['error'] = error
    if message:
        response['message'] = message
    
    return jsonify(response), status_code
