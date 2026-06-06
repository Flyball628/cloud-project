import os
import redis
from flask import Flask, jsonify

app = Flask(__name__)

# 从环境变量读取 Redis 配置
redis_host = os.environ.get('REDIS_HOST', 'localhost')
redis_port = os.environ.get('REDIS_PORT', 6379)
redis_password = os.environ.get('REDIS_PASSWORD', '')

try:
    r = redis.Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        decode_responses=True
    )
    r.ping()
    redis_ok = True
except Exception as e:
    print(f"Redis connection failed: {e}")
    redis_ok = False

@app.route('/api/ping')
def ping():
    return jsonify({"status": "ok", "redis": redis_ok})

@app.route('/api/hello')
def hello():
    return jsonify({"message": "Hello from Flask on K8s!"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
