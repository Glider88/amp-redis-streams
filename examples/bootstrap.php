<?php declare(strict_types=1);

use Glider88\AmpRedisStreams\Backpressure\PiecewiseLinearScaling;
use Glider88\AmpRedisStreams\Redis\AdvancedAmpRedis;
use Glider88\AmpRedisStreams\Retry\MultiplyRetry;
use Glider88\AmpRedisStreams\Stream;
use Glider88\AmpRedisStreams\Redis\Redis;
use Glider88\AmpRedisStreams\TimeInterval\Milli;
use Glider88\AmpRedisStreams\TimeInterval\Sec;
use Psr\Log\NullLogger;
use function Amp\Redis\createRedisClient;

require  __DIR__ . '/../vendor/autoload.php';

function mkStream(): Stream
{
    $logger = new NullLogger();
    $ampRedis = createRedisClient('redis://redis:6379');
    $ampRedisBlocked = createRedisClient('redis://redis:6379');
    $advRedis = new AdvancedAmpRedis($ampRedis, $logger);
    $advRedisBlocked = new AdvancedAmpRedis($ampRedisBlocked, $logger);
    $redis = new Redis(
        stream: 's',
        group: 'g',
        redis: $advRedis,
        redisBlocked: $advRedisBlocked,
        logger: $logger,
        maxStreamLength: 1000,
        maxDlqStreamLength: 1000,
        readRetrySetCount: 100,
        readAutoClaimCount: 100,
        blockRead: new Sec(1),
        deduplicationTtl: new Sec(3),
        autoClaimMinIdle: new Sec(1),
        consumer: 'c',
    );

    $stream = new Stream(
        stream: 's',
        group: 'g',
        redis: $redis,
        maxRetries: 3,
        logger: $logger,
        retry: new MultiplyRetry(
            firstOffsetDelay: new Milli(0),
            baseDelay: new Sec(1),
        ),
        scaling: new PiecewiseLinearScaling([
            16 => 0,
            32 => 500,
        ]),
        retryInterval: new Milli(100),
        claimInterval: new Milli(100),
        timeoutJob: new Sec(100),
    );

    return $stream;
}

