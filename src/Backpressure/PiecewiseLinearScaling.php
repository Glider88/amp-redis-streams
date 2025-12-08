<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Backpressure;

use Glider88\AmpRedisStreams\Helpers\Arr;
use InvalidArgumentException;

readonly class PiecewiseLinearScaling implements ScalingInterface
{
    /** @var non-empty-array<int, int> */
    private array $extendedPoints;

    /** @param non-empty-array<int, int> $workerToLagPoints */
    public function __construct(array $workerToLagPoints)
    {
        if (count($workerToLagPoints) < 2) {
            throw new InvalidArgumentException('Incorrect number of configuration array');
        }

        $ks = array_keys($workerToLagPoints);
        if (min($ks) <= 0) {
            throw new InvalidArgumentException('At least one worker is needed');
        }

        if (max($ks) - min($ks) === 0) {
            throw new InvalidArgumentException('There is no delta for worker numbers');
        }

        $vs = array_values($workerToLagPoints);
        if (min($vs) < 0) {
            throw new InvalidArgumentException('Lag must be positive');
        }

        if (max($vs) - min($vs) === 0) {
            throw new InvalidArgumentException('There is no delta for lag');
        }

        $pairs = Arr::slidingWindow($workerToLagPoints, 2, true);
        $tmp = [];
        foreach ($pairs as $yx) {
            $y1 = array_key_first($yx);
            $y2 = array_key_last($yx);
            $dy = $y2 - $y1;
            if ($dy === 1) {
                $tmp[] = $yx;
                continue;
            }

            $x1 = $yx[$y1];
            $x2 = $yx[$y2];
            $dx = $x2 - $x1;
            $step = $dx / $dy;
            $tmp[] = array_combine(range($y1, $y2), range($x1, $x2, $step));
        }

        $flatten = [];
        foreach ($tmp as $yx) {
            foreach ($yx as $y => $x) {
                $flatten[$y] = $x;
            }
        }

        $rounded = [];
        foreach ($flatten as $y => $x) {
            $rounded[$y] = round($x);
        }

        $this->extendedPoints = $rounded;
    }

    public function numberOfWorkers(int $lag): int
    {
        $result = array_key_first($this->extendedPoints);
        foreach ($this->extendedPoints as $y => $x) {
            if ($lag < $x) {
                break;
            }

            $result = $y;
        }

        return $result;
    }
}
