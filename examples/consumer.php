<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams;

use Closure;
use Glider88\AmpRedisStreams\Helpers\Php;
use Glider88\AmpRedisStreams\Helpers\Str;
use Glider88\AmpRedisStreams\Helpers\Time;
use Glider88\AmpRedisStreams\Job\MessageHandlerInterface;

require  __DIR__ . '/bootstrap.php';
require  __DIR__ . '/../vendor/autoload.php';

$stream = mkStream();

$startMs = Time::nowMs();
$i = 1;
$f = static function () use (&$i, &$startMs) {
    $nowMs = Time::nowMs();
    if (($nowMs - $startMs) >= 1000) {
//        Php::mem(__LINE__);
        Str::println("rpc: $i");
        $i = 1;
        $startMs = $nowMs;
    }

    $i += 1;
};

$handler = new class($f) implements MessageHandlerInterface
{
    public function __construct(
        private readonly Closure $run
    ) {}

    public function handle(array $message): void
    {
        $this->run->__invoke($message);
    }
};


$stream->run($handler);

