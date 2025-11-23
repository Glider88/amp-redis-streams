<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Helpers;

readonly class Php
{
    /** @param int|null  $line  __LINE__ */
    public static function mem(?int $line = null): void
    {
        $prefix = $line === null ? '' : $line . ': ';
        echo $prefix . memory_get_peak_usage() / 1024 / 1024 . PHP_EOL;
    }
}
