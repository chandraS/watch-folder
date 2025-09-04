# Watch Folder Service



The solution deploys a scalable monitoring system for Linode Object Storage buckets that detects object changes and delivers real-time notifications via webhooks. 

## Features

### Core Monitoring
- **Multi-Region Support**: Monitor buckets across all Linode Object Storage regions
- **Real-time Detection**: Detects object creation, updates, and modifications
- **Scalable Architecture**: Handle hundreds of buckets with optimized thread pools
- **State Management**: Tracks object state with Redis for efficient change detection

- **Redis Sentinel**: High availability with automatic failover
- **OAuth2 Authentication**: Secure webhook delivery with client credentials flow
- **Circuit Breaker Pattern**: Automatic failure handling for webhook endpoints
- **Horizontal Auto-scaling**: Kubernetes HPA for dynamic scaling
- **Logging**: Structured JSON logs with Promtail/Loki integration

### Security & Reliability
- **Credential Management**: Secure storage via Infisical vault integration
- **JWT Authentication**: API access control with token-based auth
- **Retry Logic**: Exponential backoff for failed webhook deliveries
- **Health Checks**: Kubernetes-native health and readiness probes

## Architecture

The system consists of two main components:

### Monitor Service
- Scans Linode buckets for object changes
- Implements staggered scanning to distribute load
- Publishes notifications to Redis queue
- Provides REST API for bucket management

### Consumer Service
- Processes notifications from Redis queue
- Delivers webhooks with OAuth2 authentication
- Implements retry logic and circuit breakers
- Handles parallel webhook delivery

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Linode Buckets  │    │   Redis Queue   │    │   Webhook APIs  │
│                 │    │                 │    │                 │
│ • Bucket A      │◄───┤ Monitor Service ├───►│ Consumer Service├───► • External APIs
│ • Bucket B      │    │                 │    │                 │     • Internal APIs
│ • Bucket C      │    │ • Change detect │    │ • OAuth2 auth   │     
│ • ...           │    │ • State mgmt    │    │ • Circuit break │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                       ┌─────────────────┐
                       │ Redis Sentinel  │
                       │     (HA)        │
                       └─────────────────┘
```

## Prerequisites

- LKE cluster (1.31+)
- Redis with Sentinel (included in deployment)
- Infisical vault for credential management
- Linode Object Storage buckets with access keys

## Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/linode-object-monitor
cd linode-object-monitor
```


### 2. Configure API Authentication
```bash
# Create API authentication secrets
kubectl create secret generic api-auth-secrets \
  --from-literal=jwt-secret="your-jwt-secret-key" \
  --from-literal=client-id="your-api-client-id" \
  --from-literal=client-secret="your-api-client-secret" \
  -n object-monitor
```

### 3. Deploy Redis Cluster
```bash
kubectl apply -f K8/redis.yaml
```

### 4. Deploy Configuration
```bash
kubectl apply -f K8/configmap.yaml
```

### 5. Deploy Services
```bash
# Deploy monitor service
kubectl apply -f bucket-monitor/monitor-deployment.yaml

# Deploy consumer service  
kubectl apply -f webhook-consumer/consumer-deployment.yaml
```

### 6. Verify Deployment
```bash
# Check pods are running
kubectl get pods -n object-monitor

# Check service health
kubectl port-forward svc/bucket-monitor 8080:8080 -n object-monitor
curl http://localhost:8080/health
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `POLLING_INTERVAL` | Bucket scan interval (seconds) | `30` |
| `LOG_LEVEL` | Log level (DEBUG/INFO/WARN/ERROR) | `INFO` |
| `REDIS_HOST` | Redis service hostname | `redis` |
| `JWT_SECRET` | JWT signing secret | Required |
| `API_CLIENT_ID` | API authentication client ID | Required |
| `API_CLIENT_SECRET` | API authentication secret | Required |

### Infisical Integration

Set these environment variables for secure credential storage:

```bash
INFISICAL_API_URL=https://your-infisical-instance.com
INFISICAL_TOKEN=your-service-token
INFISICAL_PROJECT_ID=your-project-id
INFISICAL_ENV=production
```

### Webhook Configuration

Buckets can be configured with webhook URLs and OAuth2 authentication:

```yaml
buckets:
  - name: "my-bucket"
    endpoint: "us-east-1.linodeobjects.com"
    webhook_url: "https://api.example.com/webhooks/bucket-events"
    webhook_auth:
      type: "oauth2"
      client_id: "webhook-client-id"
      client_secret: "webhook-client-secret"
      token_url: "https://auth.example.com/oauth/token"
```

## API Reference

In order to get the `watch-folder-url`, create a DNS record for a hostname that is to be mapped to the Linode nodebalancer exposing the service

### Authentication

Get an access token:
```bash
curl -X POST https://<watch-folder-url>/api/auth/token \
  -H "Content-Type: application/json" \
  -d '{"client_id":"your-client-id","client_secret":"your-client-secret"}'
```

### Bucket Management

#### Add a Bucket
```bash
curl -X POST https://<watch-folder-url>/api/buckets/add \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-new-bucket",
    "access_key": "access-key",
    "secret_key": "secret-key", 
    "region": "us-east-1.linodeobjects.com",
    "webhook_url": "https://api.example.com/webhook"
  }'
```

#### Update a Bucket
```bash
curl -X POST https://<watch-folder-url>/api/buckets/update \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-bucket",
    "webhook_url": "https://new-webhook-url.com"
  }'
```

#### Enable/Disable Notifications
```bash
# Enable notifications
curl -X POST "https://<watch-folder-url>/api/bucket-notifications/enable?bucket=my-bucket" \
  -H "Authorization: Bearer $TOKEN"

# Disable notifications  
curl -X POST "https://<watch-folder-url>/api/bucket-notifications/disable?bucket=my-bucket" \
  -H "Authorization: Bearer $TOKEN"
```


## Webhook Payload

When objects are detected, webhooks receive this payload:

```json
{
  "bucket": "my-bucket",
  "key": "path/to/object.jpg",
  "region": "us-east-1.linodeobjects.com",
  "etag": "d41d8cd98f00b204e9800998ecf8427e",
  "size": 1024,
  "last_modified": 1640995200.0,
  "event_type": "created",
  "content_type": "image/jpeg",
  "metadata": {},
  "detection_time": 1640995260.0,
  "is_recent": true,
  "delivery_attempt_time": "2025-04-10T12:00:00Z"
}
```

Event types: `created`, `updated`, `detected`

## Scaling Configuration

### Thread Pool Settings

```yaml
parallel:
  max_workers: 60              # Main thread pool size
  rate_limit_per_region: 15    # Concurrent ops per region
  stagger_method: "hash"       # Load distribution method
```

### Consumer Settings

```yaml
consumer:
  batch_size: 10              # Messages per batch
  webhook_threads: 20         # Concurrent webhook deliveries
  polling_interval: 1         # Queue check interval
```

### Auto-scaling

The system includes Horizontal Pod Autoscaler configurations:

- **Monitor**: 3-5 replicas based on CPU/memory
- **Consumer**: 2-4 replicas based on CPU utilization

## Logging

### Structured Logging

The service outputs structured JSON logs compatible with Loki/Grafana:

```json
{
  "timestamp": "2024-01-01T12:00:00Z",
  "level": "INFO",
  "logger": "webhook-consumer",
  "message": "Successfully delivered notification",
  "bucket": "my-bucket",
  "object_key": "file.jpg",
  "event_type": "created",
  "webhook_url": "https://api.example.com/webhook",
  "response_code": 200,
  "duration_ms": 150,
  "request_id": "abc123"
}
```

### Log Collection

Optional Promtail sidecar containers collect logs for centralized monitoring:

```bash
# Deploy logging infrastructure
kubectl apply -f K8/logging/
```


## Troubleshooting

### Common Issues

#### Redis Connection Issues
```bash
# Check Redis pod status
kubectl get pods -n object-monitor -l app=redis

# Check Sentinel status
kubectl exec -it redis-0 -n object-monitor -- redis-cli -p 26379 sentinel masters
```

#### Webhook Delivery Failures
```bash
# Check consumer logs for circuit breaker status
kubectl logs -f deployment/webhook-consumer -n object-monitor

```

#### High CPU Usage
```bash
# Check thread pool configuration
# Reduce max_workers and rate_limit_per_region in configmap

# Check auto-scaling status
kubectl get hpa -n object-monitor
```

### Debugging

Enable debug logging:
```bash
kubectl set env deployment/bucket-monitor LOG_LEVEL=DEBUG -n object-monitor
kubectl set env deployment/webhook-consumer LOG_LEVEL=DEBUG -n object-monitor
```

## Performance

### Benchmarks

- **Bucket Capacity**: 500+ buckets per cluster
- **Object Detection**: ~1000 objects/second scanning rate
- **Webhook Delivery**: ~200 webhooks/second delivery rate
- **Latency**: <2s average detection-to-delivery time

### Resource Requirements

| Component | CPU | Memory | Storage |
|-----------|-----|--------|---------|
| Monitor | 4-8 cores | 8-16GB | - |
| Consumer | 2-4 cores | 4-8GB | - |
| Redis | 2-4 cores | 4-8GB | 20GB |



