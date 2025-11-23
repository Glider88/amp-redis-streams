<?php declare(strict_types=1);

namespace Tests\Glider88\AmpRedisStreams\Backpressure;

use Glider88\AmpRedisStreams\Backpressure\PiecewiseLinearScaling;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

class ConstantScalingTest extends TestCase
{
    #[TestWith([ 1,    0])]
    #[TestWith([ 5,   50])]
    #[TestWith([11,  699])]
    #[TestWith([12,  700])]
    #[TestWith([13, 2000])]
    public function testNumberOfWorkers($expected, $lag): void
    {
        $scaling = new PiecewiseLinearScaling([
             1 =>   10,
            10 =>  100,
            13 => 1000,
        ]);

        $this->assertEquals($expected, $scaling->numberOfWorkers($lag));
    }

    #[TestWith([[]], 'zero points')]
    #[TestWith([[16 => 100]], 'only one point')]
    #[TestWith([[ 0 =>  1, 1 =>  1]], 'zero workers')]
    #[TestWith([[-1 =>  1, 1 =>  1]], 'negative workers')]
    #[TestWith([[ 1 => -1, 2 =>  1]], 'negative lag')]
    #[TestWith([[ 1 => 10, 2 => 10]], 'many workers for constant lag')]
    public function testInvalidSettings($settings): void
    {
        $this->expectException(InvalidArgumentException::class);
        new PiecewiseLinearScaling($settings);
    }
}
