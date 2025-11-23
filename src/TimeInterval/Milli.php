<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\TimeInterval;

readonly class Milli implements TimeIntervalInterface
{
    public function __construct(
        private int $milliseconds,
    ) {}

    public function milli(): int
    {
        return $this->milliseconds;
    }

    public function sec(): float
    {
        return $this->milliseconds / 1000;
    }
}
