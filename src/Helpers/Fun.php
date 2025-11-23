<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Helpers;

use Closure;

readonly class Fun
{
    public static function id(): Closure
    {
        return static fn (...$_) => $_;
    }

    public static function void(): Closure
    {
        return static function (...$_) {};
    }
}
