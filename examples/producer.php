<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams;

use Glider88\AmpRedisStreams\Helpers\Time;

require  __DIR__ . '/bootstrap.php';
require  __DIR__ . '/../vendor/autoload.php';

$stop = $argv[1] ?? null;
$stop = $stop !== null ? (int) $stop : null;
$stream = mkStream();

$startMs = Time::nowMs();
$i = 1;
$j = 1;
while (true) {
    $stream->push(uniqid('', true), ['hello' => 'world']);

    if ($stop !== null && $j >= $stop) {
        echo "processed: $j\n";
        break;
    }

    $i += 1;
    $j += 1;
    $nowMs = Time::nowMs();
    if (($nowMs - $startMs) >= 1000) {
        echo 'rpc: ' . $i . PHP_EOL;
        $i = 1;
        $startMs = $nowMs;
    }
}

