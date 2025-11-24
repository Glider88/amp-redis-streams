<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams;

use Glider88\AmpRedisStreams\Helpers\Str;
use Glider88\AmpRedisStreams\Helpers\Time;

require  __DIR__ . '/bootstrap.php';
require  __DIR__ . '/../vendor/autoload.php';

$arg1 = $argv[1] ?? null;
$arg2 = $argv[2] ?? null;

$stop = PHP_INT_MAX;
if ($arg1 === 'times') {
    $stop = (int) $arg2;
}

$stream = mkStream();

$startMs = Time::nowMs();
$i = 1;
$j = 1;
$sleep = 0;
while (true) {
    $stream->push(uniqid('', true), ['hello' => 'world']);
    if ($sleep > 0) {
        usleep($sleep);
    }

    if ($stop !== null && $j >= $stop) {
        Str::println("processed: $j");
        break;
    }

    $i += 1;
    $j += 1;
    $nowMs = Time::nowMs();
    if (($nowMs - $startMs) >= 1000) {
        Str::println("rpc: $i");

        $i = 1;
        $startMs = $nowMs;
    }
}

