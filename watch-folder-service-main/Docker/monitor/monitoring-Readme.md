# Monitor Service (monitor.py)

## Overview

The Monitor Service is the core component responsible for scanning Linode Object Storage buckets, detecting object changes, and publishing notifications to a Redis queue. 


## Core Classes

### `S3ClientCache`

Manages S3 client instances with caching to prevent unnecessary recreation.

#### Methods

- **`__init__()`**: Initializes empty cache with thread-safe locks
- **`get_client_key(endpoint, access_key, secret_key)`**: Generates unique cache keys
- **`get_client(endpoint, access_key, secret_key)`**: Returns cached S3 client or creates new one
- **`clear_cache()`**: Clears all cached S3 clients
- **`get_stats()`**: Returns cache hit/miss statistics

### `BucketScanner`

Thread worker for scanning individual buckets and detecting object changes.

#### Methods

- **`__init__(bucket_config, redis_client, global_config, result_queue, client_cache)`**: Initialize scanner thread
- **`run()`**: Main scanning logic that:
  - Gets S3 client from cache
  - Paginates through bucket objects
  - Compares object state (ETag, last modified)
  - Determines event type (created, updated, detected)
  - Publishes notifications to Redis queue
  - Updates object state tracking

### `MultiRegionMonitor`

Main orchestrator class that manages bucket scanning across multiple regions.

#### Initialization Methods

- **`__init__()`**: Initialize monitor with configuration and Redis clients
- **`refresh_config_from_redis()`**: Loads bucket configurations from Redis and merges with Infisical credentials
- **`handle_signal(signum, frame)`**: Graceful shutdown handler for SIGTERM/SIGINT
- **`load_last_scan_times()`**: Loads previous scan timestamps from Redis
- **`initialize_staggered_schedule()`**: Distributes bucket scan times evenly across polling interval
- **`init_thread_pools()`**: Creates region-specific thread pools for rate limiting
- **`get_all_regions()`**: Extracts unique regions from bucket configurations

#### Scanning Logic Methods

- **`is_bucket_due_for_scan(bucket_config)`**: Determines if bucket needs scanning based on interval and offset
- **`get_buckets_due_for_scan()`**: Returns list of buckets ready for scanning
- **`scan_bucket_with_region_pool(bucket_config, region_pool)`**: Delegates bucket scan to region-specific thread pool
- **`scan_bucket(bucket_config)`**: Creates and runs BucketScanner instance
- **`process_due_buckets()`**: Main orchestration method that:
  - Groups buckets by region for rate limiting
  - Uses thread pools for parallel processing
  - Collects and returns scan results
- **`update_scan_stats(results)`**: Updates monitoring statistics from scan results

#### Configuration Management Methods

- **`add_bucket_to_active_config(bucket_info)`**: Adds new bucket to active monitoring:
  - Validates bucket configuration
  - Stores credentials in Infisical vault
  - Saves config to Redis (persistent)
  - Adds to in-memory configuration
  - Initializes scan metadata
  - Creates regional thread pools if needed

- **`update_bucket_config(bucket_info)`**: Updates existing bucket configuration:
  - Updates non-sensitive fields in memory
  - Saves changes to Redis
  - Clears S3 client cache if endpoint changed

- **`delete_bucket_config(bucket_name)`**: Removes bucket from monitoring:
  - Removes from in-memory config
  - Deletes from Redis
  - Cleans up scan state
  - Removes credentials from Infisical

#### Health & API Methods

- **`start_health_server()`**: Starts HTTP server on port 8080 with endpoints:

  **Health Endpoints:**
  - `GET /health` - Basic health check
  - `GET /ready` - Readiness check (Redis + Sentinel + webhook config)
  - `GET /metrics` - Comprehensive metrics including scan stats, cache stats, Redis status

  **Authentication Endpoints:**
  - `POST /api/auth/token` - JWT token generation using client credentials

  **Bucket Management Endpoints:**
  - `GET /api/bucket-notifications/status` - List all buckets with notification status
  - `POST /api/bucket-notifications/enable?bucket=name` - Enable notifications for bucket
  - `POST /api/bucket-notifications/disable?bucket=name` - Disable notifications for bucket
  - `POST /api/buckets/add` - Add new bucket to monitoring
  - `POST /api/buckets/update` - Update existing bucket configuration
  - `POST /api/buckets/delete?bucket=name` - Remove bucket from monitoring

#### Main Execution Method

- **`run()`**: Main monitoring loop that:
  - Processes buckets due for scanning
  - Updates scan statistics
  - Refreshes last scan times from Redis
  - Logs cache statistics periodically
  - Handles errors gracefully
  - Sleeps between scan cycles

## Configuration

### Thread Pool Configuration

```yaml
parallel:
  max_workers: 60              # Main thread pool size
  rate_limit_per_region: 15    # Max concurrent operations per region
  stagger_method: "hash"       # Distribution method (hash/equal/random)
```

### Scanning Configuration

```yaml
defaults:
  polling_interval: 60         # Seconds between bucket scans
  max_objects_per_batch: 1000  # Objects processed per scan
```

### Redis Configuration

```yaml
redis:
  sentinel_host: "redis-sentinel"
  sentinel_port: 26379
  master_name: "mymaster"
  queue_name: "linode:notifications:queue"
  state_prefix: "linode:objstore:state:"
  ttl: 2592000                 # 30-day state retention
```

## Event Types

The monitor detects three types of object events:

- **`created`** - New objects (within 24 hours)
- **`updated`** - Modified objects (ETag or timestamp changed)
- **`detected`** - Existing objects without stored state

## State Management

Object state is stored in Redis with the following structure:

```json
{
  "etag": "d41d8cd98f00b204e9800998ecf8427e",
  "last_modified": 1640995200.0,
  "size": 1024,
  "last_processed": 1640995260.0
}
```


## Security Features

- **JWT Authentication**: API access control with configurable secrets
- **Infisical Integration**: Secure credential storage in external vault
- **Redis Security**: Support for password authentication
- **Credential Separation**: S3 credentials stored in vault, config in Redis

## Monitoring & Observability

### Metrics Exposed

- Scan statistics (total scans, objects processed, errors)
- Cache performance (hits, misses, total clients)
- Redis health and queue stats
- Sentinel status and failover information

### Health Checks

- **Liveness**: Basic service health
- **Readiness**: Redis connectivity, Sentinel status, webhook configuration

## Error Handling

- **Redis Failures**: Automatic fallback to direct Redis connection
- **S3 Errors**: Per-bucket error tracking and logging
- **Thread Pool**: Graceful handling of worker thread failures
- **Signal Handling**: Clean shutdown on SIGTERM/SIGINT

## Performance Characteristics

- **Bucket Capacity**: 500+ buckets per instance
- **Scan Rate**: ~1000 objects/second
- **Memory Efficiency**: LRU cache management
- **CPU Optimization**: Regional rate limiting prevents API throttling

## Usage Example

```python
# Initialize and run monitor
monitor = MultiRegionMonitor()
monitor.run()
```

The monitor automatically:
1. Loads configuration from Redis and Infisical
2. Initializes thread pools and caching
3. Begins scanning buckets on schedule
4. Serves health/API endpoints
5. Handles graceful shutdown signals
