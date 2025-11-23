<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams\Job;


interface MessageHandlerInterface
{
    public function handle(array $message): void;
}
