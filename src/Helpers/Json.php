<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Helpers;

readonly class Json
{
    public static function encode(array $value): string
    {
        return json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
    }

    public static function decode(string $json): array
    {
        return json_decode($json, true, 512, JSON_THROW_ON_ERROR);
    }
}
