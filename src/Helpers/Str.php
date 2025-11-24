<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Helpers;

readonly class Str
{
    public static function println(mixed $value): void
    {
        echo print_r($value, true) . PHP_EOL;
    }
}
