<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Helpers;

readonly class Arr
{
    /**
     * @template K
     * @template V
     * @param list<K|V> $array
     * @return array<K, V>
     */
    public static function listPairsToArray(array $array, $stub = null): array
    {
        if (count($array) % 2 !== 0) {
            $array[] = $stub;
        }

        $result = [];
        foreach (array_chunk($array, 2) as [$k, $v]) {
            $result[$k] = $v;
        }

        return $result;
    }

    public static function flatten(array $array): array
    {
        return array_merge(...$array);
    }

    /**
     * @template K
     * @template T
     * @param array<K,T> $array
     * @return array<array<K,T>>
     */
    public static function slidingWindow(array $array, int $size, bool $preserveKeys = false, int $step = 1): array
    {
        $result = [];
        $length = count($array);
        foreach (range(0, $length, $step) as $offset) {
            $window = array_slice($array, $offset, $size, $preserveKeys);
            $result[] = $window;
            if ($offset + $size >= $length) {
                break;
            }
        }

        return $result;
    }
}
