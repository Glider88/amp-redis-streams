<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Redis;

use Amp\Redis\RedisException;
use Glider88\AmpRedisStreams\Helpers\Time;
use Glider88\AmpRedisStreams\TimeInterval\TimeIntervalInterface;
use Psr\Log\LoggerInterface;

readonly class Redis implements RedisInterface
{
    private string $consumer;

    public function __construct(
        private string                    $stream,
        private string                    $group,
        private AdvancedAmpRedisInterface $redis,
        private AdvancedAmpRedisInterface $redisBlocked,
        private LoggerInterface           $logger,
        private int                       $maxStreamLength,
        private int                       $maxDlqStreamLength,
        private int                       $readRetrySetCount,
        private int                       $readAutoClaimCount,
        private TimeIntervalInterface     $blockRead,
        private TimeIntervalInterface     $deduplicationTtl,
        private TimeIntervalInterface     $autoClaimMinIdle,
        ?string                           $consumer,
    ) {
        if (is_null($consumer)) {
            $this->consumer = 'c-'.gethostname().'-'.getmypid();
        } else {
            $this->consumer = $consumer;
        }
    }

    public function createConsumerGroupIfNotExist(): void
    {
        $this->createConsumerGroup($this->stream);
        $this->createConsumerGroup($this->stream . ':dlq');
    }

    private function createConsumerGroup(string $stream): void
    {
        try {
            $this->redis->xGroupCreate($stream, $this->group, '$', true);
            $this->logger->info("Consumer group: $this->group for stream: $stream created");
        } catch (RedisException $e) {
            if (str_contains($e->getMessage(), 'BUSYGROUP')) {
                $this->logger->info("Consumer group: $this->group for stream: $stream already exists");
            } else {
                throw $e;
            }
        }
    }

    /**  @return string redis stream message id */
    public function add(array $message): string
    {
        return $this->redis->xAdd(
            $this->stream,
            $message,
            '*',
            [
                'limit' => ['MAXLEN', '~', $this->maxStreamLength]
            ]
        );
    }

    /**  @return string redis stream message id */
    public function addToDlq(array $message): string
    {
        return $this->redis->xAdd(
            $this->stream . ':dlq',
            $message,
            '*',
            [
                'limit' => ['MAXLEN', '~', $this->maxDlqStreamLength]
            ]
        );
    }

    public function read(int $count): array
    {
        return $this->redisBlocked->xReadGroup(
            $this->group,
            $this->consumer,
            $count,
            $this->blockRead->milli(),
            $this->stream,
            '>',
        );
    }

    /** @param string $id redis stream message */
    public function ack(string $id): void
    {
        $this->redis->xAck($this->stream, $this->group, $id);
    }

    /** @param string $id message id */
    public function passThroughGuard(string $id): bool
    {
        return $this->redis->set($id . '-' . $this->group, '1', $this->deduplicationTtl->sec(), true);
    }

    /** @param string $id message id */
    public function revokeGuard(string $id): void
    {
        $this->redis->del($id . '-' . $this->group);
    }

    public function addToRetry(int $startAt, array $message): void
    {
        $this->redis->zAdd(
            $this->stream . ':' . $this->group . ':retry',
            $startAt,
            $message
        );
    }

    public function retryRange(): array
    {
        $now = Time::nowMs();

        return $this->redis->zRange(
            $this->stream . ':' . $this->group . ':retry',
            0,
            $now,
            $this->readRetrySetCount
        );
    }

    public function removeFromRetry(array $message): void
    {
        $this->redis->zRem($this->stream . ':' . $this->group . ':retry', $message);
    }

    /** @param string $cursorId current redis stream message id */
    public function autoClaim(string $cursorId): array
    {
        return $this->redis->xAutoClaim(
            $this->stream,
            $this->group,
            $this->consumer,
            $this->autoClaimMinIdle->milli(),
            $cursorId,
            $this->readAutoClaimCount,
        );
    }

    public function info(): array
    {
        return $this->redis->xInfoGroups($this->stream);
    }
}
