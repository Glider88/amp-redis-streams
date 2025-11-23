<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Retry;

use Glider88\AmpRedisStreams\TimeInterval\Milli;
use Glider88\AmpRedisStreams\TimeInterval\TimeIntervalInterface;

readonly class ConstantRetry implements RetryInterface
{
    private TimeIntervalInterface $delay;

    public function __construct(TimeIntervalInterface $delay = null) {
        $this->delay = is_null($delay) ? new Milli(0) : $delay;
    }

    public function delay(int $step): TimeIntervalInterface
    {
        return $this->delay;
    }
}
