# AMPHP redis streams realization

### Installation:
```shell
composer require glider88/amp-redis-streams
```
Start docker:
```shell
bin/re # first time
```
```shell
bin/up # next times
```
Tests:
```shell
bin/unit
```

### Run example:

Producer:
```shell
bin/php examples/producer.php
```
Consumer:
```shell
bin/php examples/consumer.php
```

### Settings:
```php
    // create amphp redis
    $ampRedis = createRedisClient('redis://redis:6379');
    
    // second amphp redis, because Stream has blocking operation 'XREADGROUP GROUP ... BLOCK ...', this is necessary to reduce latency
    $ampRedisBlocked = createRedisClient('redis://redis:6379');
    
    // wrapper for amphp redis that implement missing redis commands
    $advRedis = new AdvancedAmpRedis($ampRedis, $logger);
    $advRedisBlocked = new AdvancedAmpRedis($ampRedisBlocked, $logger);

    $redis = new Redis(
        stream: 's',                    // stream name
        group: 'g',                     // consumer group name
        redis: $advRedis,
        redisBlocked: $advRedisBlocked,
        logger: new NullLogger(),
        maxStreamLength: 1000,          // approximate stream size
        maxDlqStreamLength: 1000,       // approximate stream size for dead letters
        readRetrySetCount: 100,         // how many entities we get at time from redis sorted set
                                        // (redis set is used to implement logic of retries, with name s:g:retry)
        readAutoClaimCount: 100,        // how many autoclaim entities we get at time
        blockRead: new Sec(1),          // how long we wait first data from stream
        deduplicationTtl: new Sec(3),   // for deduplication logic used `SET $streamMessageId . '-' . $this->group` with this ttl
        autoClaimMinIdle: new Sec(1),   // after this time we get message from PEL by autoclaim
        consumer: 'c',                  // optional consumer name, or generate: 'c-'.gethostname().'-'.getmypid()
    );

    $stream = new Stream(
        stream: 's',                               // stream name
        group: 'g',                                // consumer group name
        redis: $redis,
        maxRetries: 3,                             // after we send message to dead letters stream (with s:dql name)
        logger: new NullLogger(),
        retry: new MultiplyRetry(                  // retry with incremental increase time: 0 1 2 3... seconds wait before retry
            firstOffsetDelay: new Milli(0),
            baseDelay: new Sec(1),
        ),
        scaling: new PiecewiseLinearScaling([ // complex scaling number of workers, for empty stream (< 500) use 16 worker, next 32
            16 => 0,
            32 => 500,
        ]),
        retryInterval: new Milli(100),  // how often we launch retry logic
        claimInterval: new Milli(100),  // how often we launch autoclaim logic
        timeoutJob: new Sec(100),       // timeout for job, after which we cancel it by TimeoutCancellation exception
    );
```
