<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Retry;

use Glider88\AmpRedisStreams\TimeInterval\TimeIntervalInterface;

interface RetryInterface
{
    public function delay(int $step): TimeIntervalInterface;
}
