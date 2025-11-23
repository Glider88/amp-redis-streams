<?php declare(strict_types=1);

namespace Tests\Glider88\AmpRedisStreams\Retry;

use Glider88\AmpRedisStreams\Retry\MultiplyRetry;
use Glider88\AmpRedisStreams\TimeInterval\Sec;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

class MultiplyRetryTest extends TestCase
{
    #[TestWith([ 0, 0])]
    #[TestWith([ 1, 1])]
    #[TestWith([11, 2])]
    #[TestWith([21, 3])]
    public function testDelay($expected, $step): void
    {
        $retry = new MultiplyRetry(new Sec(1), new Sec(10));

        $this->assertEquals($expected, $retry->delay($step)->sec());
    }

    #[TestWith([ 0, 0])]
    #[TestWith([ 0, 1])]
    #[TestWith([10, 2])]
    #[TestWith([20, 3])]
    public function testDelayWithZeroOffset($expected, $step): void
    {
        $retry = new MultiplyRetry(new Sec(0), new Sec(10));

        $this->assertEquals($expected, $retry->delay($step)->sec());
    }
}
