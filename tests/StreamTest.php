<?php declare(strict_types=1);

namespace Tests\Glider88\AmpRedisStreams;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\Redis\RedisClient;
use Exception;
use Glider88\AmpRedisStreams\Backpressure\ConstantScaling;
use Glider88\AmpRedisStreams\Backpressure\ScalingInterface;
use Glider88\AmpRedisStreams\Helpers\Arr;
use Glider88\AmpRedisStreams\Helpers\Fun;
use Glider88\AmpRedisStreams\Helpers\Time;
use Glider88\AmpRedisStreams\Redis\AdvancedAmpRedis;
use Glider88\AmpRedisStreams\Redis\Redis;
use Glider88\AmpRedisStreams\Redis\RedisInterface;
use Glider88\AmpRedisStreams\Retry\ConstantRetry;
use Glider88\AmpRedisStreams\Stream;
use Glider88\AmpRedisStreams\TimeInterval\Milli;
use Glider88\AmpRedisStreams\TimeInterval\Sec;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Revolt\EventLoop;
use Stringable;
use Tests\Glider88\AmpRedisStreams\Utils\TestHandler;
use function Amp\async;
use function Amp\delay;
use function Amp\Redis\createRedisClient;

class StreamTest extends TestCase
{
    private DeferredFuture $deferredFuture;

    public function testSimplePath(): void
    {
        $f = function (array $msg) {
            $this->assertEquals(['one'], $msg);
        };

        $stream = $this->mkStream();
        $stream->push('m1', ['one']);
        $stream->run(new TestHandler($f), 1);
    }

    public function testOverflow(): void
    {
        $redis = $this->mkRedis(['maxStreamLength' => 1]);
        $stream = $this->mkStream($redis);
        foreach (range(1, 200) as $i) {
            $stream->push("m$i", [$i]);
        }

        $len = $this->mkAmpRedis()->execute('XLEN', 's');
        $this->assertTrue($len < 200);
    }

    public function testManyGroups(): void
    {
        $redis2 = $this->mkRedis(['group' => 'g2']);
        $redis1 = $this->mkRedis(['group' => 'g1']);

        $stream1 = $this->mkStream($redis1, ['group' => 'g1']);
        $stream2 = $this->mkStream($redis2, ['group' => 'g2']);

        $stream1->push("1", ['one']);
        $stream1->push("2", ['two']);
        $stream1->push("3", ['three']);
        $stream1->push("4", ['four']);
        $stream1->push("5", ['five']);

        $h1 = new TestHandler(Fun::void());
        $h2 = new TestHandler(Fun::void());

        Future\awaitAll([
            async(static fn() => $stream1->run($h1, 2)),
            async(static fn() => $stream2->run($h2, 3))
        ]);

        $r1 = Arr::flatten($h1->results);
        $r2 = Arr::flatten($h2->results);

        $this->assertEquals(['one', 'two'], $r1);
        $this->assertEquals(['one', 'two', 'three'], $r2);
    }

    public function testManyConsumers(): void
    {
        $redis1 = $this->mkRedis(['consumer' => 'c1']);
        $redis2 = $this->mkRedis(['consumer' => 'c2']);

        $stream1 = $this->mkStream($redis1, ['scaling' => new ConstantScaling(1)]);
        $stream2 = $this->mkStream($redis2, ['scaling' => new ConstantScaling(1)]);

        foreach (range(1, 9) as $i) {
            $stream1->push("m$i", [$i]);
        }

        $h1 = new TestHandler(Fun::void());
        $h2 = new TestHandler(Fun::void());

        Future\awaitAll([
            async(static fn() => $stream1->run($h1, 4)),
            async(static fn() => $stream2->run($h2, 4))
        ]);

        $r1 = array_map(intval(...), Arr::flatten($h1->results));
        $r2 = array_map(intval(...), Arr::flatten($h2->results));

        $this->assertCount(4, $r1);
        $this->assertCount(4, $r2);

        $all = Arr::flatten([$r1, $r2]);
        sort($all);
        $this->assertEquals(range(1, 8), $all);

        $this->assertNotSame([1, 2, 3, 4], $r1);
        $this->assertNotSame([5, 6, 7, 8], $r2);
    }

    public function testRetryTask(): void
    {
        $red = $this->mkRedis([
            'stream' => 's',
            'group' => 'g1',
            'autoClaimMinIdle' => new Milli(1),
            'deduplicationTtl' => new Sec(5),
        ]);

        $stream = $this->mkStream($red, [
            'stream' => 's',
            'group' => 'g1',
            'retryInterval' => new Milli(10),
            'claimInterval' => new Milli(10),
        ]);

        $redis = new AdvancedAmpRedis($this->mkAmpRedis(), new NullLogger());
        $redis->xGroupCreate('s', 'g2');

        $stream->push('m1', ['one']);
        $r1 = $redis->xReadGroup('g2', 'c', 10, 100, 's', '>');
        foreach ($r1['s'] as $item) {
            $this->assertEquals(0, (int) $item['_service_data_message_retries']);
        }

        $f = static function (array $msg) {
            throw new Exception('COROUTINE FAILED');
        };

        $stream->run(new TestHandler($f), 3);

        $r2 = $redis->xReadGroup('g2', 'c', 10, 100, 's', '>');

        $rs = [];
        foreach ($r2['s'] as $item) {
            $rs[] = (int) $item['_service_data_message_retries'];
        }

        $this->assertEquals([1, 2], $rs);
    }

    public function testDeduplication(): void
    {
        $stream = $this->mkStream();

        $stream->push('m1', ['one']);
        $stream->push('m1', ['two']);
        $stream->push('m2', ['three']);

        $h = new TestHandler(Fun::void());
        $stream->run($h, 2);

        $this->assertEquals([['one'], ['three']], $h->results);
    }

    public function testConcurrency(): void
    {
        $stream = $this->mkStream($this->mkRedis(), [
            'scaling' => new ConstantScaling(3)
        ]);

        foreach (range(1, 10) as $i) {
            $stream->push("m$i", [$i]);
        }

        $start = Time::nowMs();
        $rs = [];
        $f = function (array $_) use (&$rs, $start) {
            $rs[] = Time::nowMs() - $start;
            delay(0.02);
        };
        $h = new TestHandler($f);

        $stream->run($h, 10);

        $isNearEqualFn = static fn(array $p) => ($p[1] - $p[0]) >= 15;
        $arr = array_map($isNearEqualFn, Arr::slidingWindow($rs, 2));
        foreach (array_chunk($arr, 3) as $trio) {
            $this->assertEquals([false, false, true], $trio);
        }
    }

    public function testTimeout(): void
    {
        $redis = $this->mkRedis();
        $stream = $this->mkStream($redis, ['timeoutJob' => new Milli(50)]);

        $stream->push("m1", ['one']);

        $neverReach = true;
        $f = static function (array $msg) use (&$neverReach) {
            delay(0.1);
            $neverReach = false;
        };
        $h = new TestHandler($f);

        $stream->run($h, 1);

        $this->assertTrue($neverReach);
    }

    public function testAutoScaling(): void
    {
        $scale = new class() implements ScalingInterface {
            public function numberOfWorkers(int $lag): int
            {
                if ($lag <= 5) {
                    return 1;
                }

                return 3;
            }
        };

        $stream = $this->mkStream($this->mkRedis(), ['scaling' => $scale]);

        foreach (range(1, 10) as $i) {
            $stream->push("m$i", [$i]);
        }

        $start = Time::nowMs();
        $rs = [];
        $f = static function (array $_) use (&$rs, $start) {
            $rs[] = Time::nowMs() - $start;
            delay(0.02);
        };
        $h = new TestHandler($f);

        $stream->run($h, 10);

        $isNearEqualFn = static fn(array $p) => ($p[1] - $p[0]) >= 15 ? 1 : 0;
        $arr = array_map($isNearEqualFn, Arr::slidingWindow($rs, 2));

        $this->assertEquals([0, 0, 1, 0, 0, 1, 1, 1, 1], $arr);
    }

    public function testDeadLetterQueue(): void
    {
        $red = $this->mkRedis([
            'stream' => 's',
            'group' => 'g1',
            'autoClaimMinIdle' => new Milli(1),
            'deduplicationTtl' => new Sec(5),
        ]);

        $stream = $this->mkStream($red, [
            'stream' => 's',
            'group' => 'g1',
            'maxRetries' => 2,
            'retryInterval' => new Milli(10),
            'claimInterval' => new Milli(10),
        ]);

        $redis = new AdvancedAmpRedis($this->mkAmpRedis(), new NullLogger());
        $redis->xGroupCreate('s', 'g2');

        $stream->push('m1', ['one']);

        $f = static function (array $msg) {
            throw new Exception('COROUTINE FAILED');
        };

        $stream->run(new TestHandler($f), 3);

        $r1 = $redis->xReadGroup('g2', 'c', 10, 100, 's', '>');
        $this->assertCount(3, $r1['s']);

        $stream->push('m2', ['two']); // only for call autoclame by interval
        $stream->run(new TestHandler($f), 1);

        $q2 = $redis->xReadGroup('g1', 'c', 10, 100, 's:dlq', '>');
        $this->assertCount(1, $q2['s:dlq']);
    }

    /**
     * copied from amphp/phpunit-util
     */
    protected function setUp(): void
    {
        $this->deferredFuture = new DeferredFuture();

        EventLoop::setErrorHandler(function (\Throwable $exception): void {
            if ($this->deferredFuture->isComplete()) {
                return;
            }

            $this->deferredFuture->error($exception);
        });
    }

    protected function tearDown(): void
    {
        $this->mkAmpRedis()->flushAll();
    }

    private function mkRedis(array $options = []): RedisInterface
    {
        $logger = new NullLogger();
//        $logger = $this->mkConsoleLogger();

        $default = [
            'stream' => 's',
            'group' => 'g',
            'redis' => new AdvancedAmpRedis($this->mkAmpRedis(), $logger),
            'redisBlocked' => new AdvancedAmpRedis($this->mkAmpRedis(), $logger),
            'logger' => $logger,
            'maxStreamLength' => 100,
            'maxDlqStreamLength' => 100,
            'readRetrySetCount' => 100,
            'readAutoClaimCount' => 100,
            'blockRead' => new Sec(1),
            'deduplicationTtl' => new Sec(1),
            'autoClaimMinIdle' => new Sec(60),
            'consumer' => 'c',
        ];

        $args = array_merge($default, $options);

        return new Redis(...$args);
    }

    private function mkStream(?RedisInterface $redis = null, array $options = []): Stream
    {
        $logger = new NullLogger();
//        $logger = $this->mkConsoleLogger();

        if (is_null($redis)) {
            $redis = $this->mkRedis();
        }

        $default = [
            'stream' => 's',
            'group' => 'g',
            'redis' => $redis,
            'maxRetries' => 3,
            'logger' => $logger,
            'retry' => new ConstantRetry(),
            'scaling' => new ConstantScaling(10),
            'retryInterval' => new Sec(1),
            'claimInterval' => new Sec(1),
            'timeoutJob' => new Sec(50),
        ];

        $args = array_merge($default, $options);

        return new Stream(...$args);
    }

    private function mkConsoleLogger(): LoggerInterface
    {
        return new class extends AbstractLogger
        {
            public function log($level, Stringable|string $message, array $context = []): void
            {
                echo 'log: ' . $message . PHP_EOL;
            }
        };
    }

    private function mkAmpRedis(): RedisClient
    {
        return createRedisClient('redis://redis:6379');
    }
}
