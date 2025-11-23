<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\TimeInterval;

readonly class Sec implements TimeIntervalInterface
{
    public function __construct(
        private int $seconds,
    ) {}

    public function milli(): int
    {
        return $this->seconds * 1000;
    }

    public function sec(): int
    {
        return $this->seconds;
    }
}
