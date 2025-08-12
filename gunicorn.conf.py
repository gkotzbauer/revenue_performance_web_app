# Gunicorn configuration for Revenue Performance Web App
# This file sets proper timeouts to prevent worker timeouts during pipeline execution

# Worker processes
workers = 2
worker_class = "uvicorn.workers.UvicornWorker"

# Timeout settings
timeout = 600  # 10 minutes - enough for pipeline execution
keepalive = 5
worker_tmp_dir = "/dev/shm"  # Use shared memory for better performance

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Process naming
proc_name = "revenue-performance-app"

# Server socket
bind = "0.0.0.0:10000"

# Worker lifecycle
max_requests = 1000
max_requests_jitter = 100
preload_app = True

# Security
limit_request_line = 4094
limit_request_fields = 100
limit_request_field_size = 8190
