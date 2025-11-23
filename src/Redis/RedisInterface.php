<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Redis;

interface RedisInterface
{
    public function createConsumerGroupIfNotExist(): void;
    public function add(array $message): string;
    public function addToDlq(array $message): string;
    public function read(int $count): array;
    public function ack(string $id): void;
    public function passThroughGuard(string $id): bool;
    public function revokeGuard(string $id): void;
    public function addToRetry(int $startAt, array $message): void;
    public function retryRange(): array;
    public function removeFromRetry(array $message): void;
    public function autoClaim(string $cursorId): array;
    public function info(): array;
}
