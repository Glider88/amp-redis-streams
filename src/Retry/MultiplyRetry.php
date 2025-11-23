<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Retry;

use Glider88\AmpRedisStreams\TimeInterval\Milli;
use Glider88\AmpRedisStreams\TimeInterval\TimeIntervalInterface;

readonly class MultiplyRetry implements RetryInterface
{
    public function __construct(
        private TimeIntervalInterface $firstOffsetDelay,
        private TimeIntervalInterface $baseDelay,
    ) {}

    public function delay(int $step): TimeIntervalInterface
    {
        if ($step === 0) {
            return new Milli(0);
        }

        if ($step === 1) {
            return $this->firstOffsetDelay;
        }

        $millis = $this->firstOffsetDelay->milli() + ($this->baseDelay->milli() * ($step - 1));

        return new Milli($millis);
    }
}
