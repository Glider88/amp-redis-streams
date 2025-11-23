<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Helpers;

readonly class Time
{
    public static function nowMs(): int
    {
        return (int) (microtime(true) * 1000);
    }
}
