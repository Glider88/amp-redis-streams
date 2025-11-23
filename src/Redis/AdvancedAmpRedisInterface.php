<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Redis;

interface AdvancedAmpRedisInterface
{
    public const OK = 'OK';

    public function xGroupCreate(string $key, string $group, string $id, bool $mkStream = false): bool;
    public function xGroupDestroy(string $key, string $group): int;
    public function xAdd(string $key, array $dictionary, string $id = '*', array $options = null): string;
    public function xReadGroup(string $group, string $consumer, ?int $count = null, ?int $blockMs = null, string ...$keyAndId): array;
    public function xPending(string $key, string $group, string $startId, string $endId, int $count): array;
    public function xAck(string $key, string $group, string ...$id): int;
    public function xAutoClaim(string $key, string $group, string $consumer, int $minIdleMs, string $start, ?int $count = null, bool $justId = false): array;
    public function xInfoGroups(string $key): array;
    public function zAdd(string $key, int $score, array $data): int;
    public function zRange(string $key, int $scoreMin, int $scoreMax, int $limit): array;
    public function zRem(string $key, array $data): int;
    public function del(string $key): int;
    public function set(string $key, string $value, int $ttl = 0, ?bool $nx = false): bool;
}
