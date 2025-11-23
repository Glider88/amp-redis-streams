<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Backpressure;

readonly class ConstantScaling implements ScalingInterface
{
    public function __construct(private int $workers) {}

    public function numberOfWorkers(int $lag): int
    {
        return $this->workers;
    }
}
