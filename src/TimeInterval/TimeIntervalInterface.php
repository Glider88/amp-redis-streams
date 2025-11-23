<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\TimeInterval;

interface TimeIntervalInterface
{
    public function milli(): int|float;
    public function sec(): int|float;
}
