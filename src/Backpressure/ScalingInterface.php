<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Backpressure;

interface ScalingInterface
{
    public function numberOfWorkers(int $lag): int;
}
