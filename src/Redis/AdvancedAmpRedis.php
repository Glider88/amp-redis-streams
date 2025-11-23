<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Redis;

use Amp\Redis\RedisClient;
use Glider88\AmpRedisStreams\Helpers\Arr;
use Glider88\AmpRedisStreams\Helpers\Json;
use Psr\Log\LoggerInterface;

readonly class AdvancedAmpRedis implements AdvancedAmpRedisInterface
{
    public function __construct(
        private RedisClient $redis,
        private LoggerInterface $logger,
    ) {}

    public function xGroupCreate(string $key, string $group, string $id = '$', bool $mkStream = false): bool
    {
        $args = ['XGROUP', 'CREATE', $key, $group, $id];
        if ($mkStream) {
            $args[] = 'MKSTREAM';
        }

        // auto id = $
        return $this->execute(...$args) === AdvancedAmpRedisInterface::OK;
    }

    /** @return int<0,1> */
    public function xGroupDestroy(string $key, string $group): int
    {
        return $this->execute('XGROUP', 'DESTROY', $key, $group);
    }

    /** @return string message id */
    public function xAdd(string $key, array $dictionary, string $id = '*', array $options = null): string
    {
        $args = ['XADD', $key];

        if (isset($options['limit'])) {
            $args[] = $options['limit'][0];
            $args[] = $options['limit'][1];
            $args[] = $options['limit'][2];
        }

        $args[] = $id;

        foreach ($dictionary as $k => $v) {
            $args[] = $k;
            $args[] = $v;
        }

        return $this->execute(...$args);
    }

    public function xAck(string $key, string $group, string ...$id): int
    {
        return $this->execute('XACK', $key, $group, ...$id);
    }

    public function xPending(string $key, string $group, string $startId, string $endId, int $count): array
    {
        return $this->execute('XPENDING', $key, $group, $startId, $endId, $count);
    }

    public function xAutoClaim(string $key, string $group, string $consumer, int $minIdleMs, string $start, ?int $count = null, bool $justId = false): array
    {
        $args = ['XAUTOCLAIM', $key, $group, $consumer, $minIdleMs, $start];

        if (isset($count)) {
            $args[] = 'COUNT';
            $args[] = $count;
        }

        if ($justId) {
            $args[] = 'JUSTID';
        }

        $raw = $this->execute(...$args);

        $cursorId = $raw[0];
        $rows = $raw[1];

        $result = [];
        foreach ($rows as $row) {
            foreach (Arr::listPairsToArray($row, stub: []) as $messageId => $rowValues) {
                $result[$messageId] = Arr::listPairsToArray($rowValues);
            }
        }

        return [$cursorId, $result];
    }

    public function xReadGroup(string $group, string $consumer, ?int $count = null, ?int $blockMs = null, string ...$keyAndId): array
    {
        $args = ['XREADGROUP', 'GROUP', $group, $consumer];

        if (isset($count)) {
            $args[] = 'COUNT';
            $args[] = $count;
        }

        if (isset($blockMs)) {
            $args[] = 'BLOCK';
            $args[] = $blockMs;
        }

        $args[] = 'STREAMS';

        $raw = $this->execute(...$args, ...$keyAndId);

        if (is_null($raw)) {
            $raw = [];
        }

        $result = [];
        foreach ($raw as $row) {
            foreach (Arr::listPairsToArray($row, stub: []) as $stream => $rowValues) {
                foreach ($rowValues as $rowValue) {
                    foreach (Arr::listPairsToArray($rowValue, stub: []) as $id => $fields) {
                        $result[$stream][$id] = Arr::listPairsToArray($fields);
                    }
                }
            }
        }

        return $result;
    }

    public function xInfoGroups(string $key): array
    {
        $raw = $this->execute('XINFO', 'GROUPS', $key);

        return array_map(static fn($row) => Arr::listPairsToArray($row), $raw);
    }

    public function del(string $key): int
    {
        return $this->execute('DEL', $key);
    }

    public function zAdd(string $key, int $score, array $data): int
    {
        $dataStr = Json::encode($data);

        return $this->execute('ZADD', $key, $score, $dataStr);
    }

    public function zRange(string $key, int $scoreMin, int $scoreMax, int $limit = 0): array
    {
        $args = ['ZRANGE', $key, $scoreMin, $scoreMax, 'BYSCORE'];
        if ($limit > 0) {
            $args[] = 'LIMIT';
            $args[] = '0';
            $args[] = $limit;
        }

        $raw = $this->execute(...$args);
        $serializeFn = static fn($row) => Json::decode($row);

        return array_map($serializeFn, $raw);
    }

    public function zRem(string $key, array $data): int
    {
        $dataStr = Json::encode($data);

        return $this->execute('ZREM', $key, $dataStr);
    }

    public function set(string $key, string $value, int $ttl = 0, ?bool $nx = false): bool
    {
        $args = ['SET', $key, $value];
        if ($ttl !== 0) {
            $args[] = 'EX';
            $args[] = $ttl;
        }

        if ($nx) {
            $args[] = 'NX';
        }

        return $this->execute(...$args) === AdvancedAmpRedisInterface::OK;
    }

    public function execute(...$arguments): mixed
    {
        $args = implode(' ', $arguments);
        $this->logger->debug($args);

        return $this->redis->execute(...$arguments);
    }
}
