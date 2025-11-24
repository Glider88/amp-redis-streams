<?php declare(strict_types=1);

namespace Glider88\AmpRedisStreams;

use Amp\CancelledException;
use Amp\Future;
use Amp\TimeoutCancellation;
use Glider88\AmpRedisStreams\Backpressure\ScalingInterface;
use Glider88\AmpRedisStreams\Helpers\Time;
use Glider88\AmpRedisStreams\Job\MessageHandlerInterface;
use Glider88\AmpRedisStreams\Redis\RedisInterface;
use Glider88\AmpRedisStreams\Retry\RetryInterface;
use Glider88\AmpRedisStreams\TimeInterval\TimeIntervalInterface;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;
use Throwable;
use function Amp\async;

readonly class Stream
{
    private const FIRST_ID = '0-0';

    public function __construct(
        private string                $stream,
        private string                $group,
        private RedisInterface        $redis,
        private int                   $maxRetries,
        private LoggerInterface       $logger,
        private RetryInterface        $retry,
        private ScalingInterface      $scaling,
        private TimeIntervalInterface $retryInterval,
        private TimeIntervalInterface $claimInterval,
        private TimeIntervalInterface $timeoutJob,
    ) {
        $this->redis->createConsumerGroupIfNotExist();
    }

    public function push(string $id, array $data): string
    {
        $data['_service_data_message_retries'] = 0;
        $data['_service_data_message_id'] = $id;

        return $this->redis->add($data);
    }

    public function run(MessageHandlerInterface $handler, ?int $times = null): void
    {
        $this->autoClaim();
        $this->autoRetry();

        /** @var list<Future> $futures */
        $futures = [];
        $concurrency = $this->scaling->numberOfWorkers(0);
        $isStop = false;

        if (extension_loaded('pcntl')){
            EventLoop::onSignal(SIGINT, static function () use (&$isStop) {
                $isStop = true;
            });
        }

//        $sem = new \Amp\Sync\LocalSemaphore(16);   //  > 2x faster, but no scaling and timeouts
        while (true) {
            $this->logger->debug('new loop tick');
            if ($isStop) {
                $this->logger->info("event loop stopped");
                break;
            }

            $lag = $this->lag();
            if ($lag !== null) {
                $concurrency = $this->scaling->numberOfWorkers($lag);
            }

            $this->logger->debug('read');
            $batch = $this->redis->read($concurrency);
            if (empty($batch) || !array_key_exists($this->stream, $batch)) {
                $this->logger->debug('stream is empty');
                continue;
            }
            $this->logger->debug('messages count: ' . count($batch[$this->stream]));

            foreach ($batch[$this->stream] as $id => $fields) {
                $messageId = $fields['_service_data_message_id'];
                $ok = $this->redis->passThroughGuard($messageId);
                if (!$ok) {
                    $this->logger->warning("duplicate message#$messageId for group: $this->group, stream: $this->stream");
                    continue;
                }

//                $lock = $sem->acquire();
                $futures[$id] = async(function () use ($handler, $id, $fields) {
                    try {
                        unset($fields['_service_data_message_retries'], $fields['_service_data_message_id']);
                        $this->logger->debug("start handle message: " . $id);
                        $handler->handle($fields);
                        $this->logger->debug("stop handle message: " . $id);
                        $this->redis->ack($id);
                    } catch (Throwable $e) {
                        $this->logger->error("FAILED handle message: " . $e->getMessage());
                        // not ack -> left in PEL
                    }
//                    finally {
//                        $lock->release();
//                    }
                });

                if (! is_null($times)) {
                    $this->logger->debug('times: ' . $times);
                    $times -= 1;
                    if ($times <= 0) {
                        $this->await($futures);
                        break 2;
                    }
                }
            }

            $this->await($futures);
            $futures = [];
        }
    }

    /** @param array<string, Future> $futures */
    private function await(array $futures): void
    {
        try {
            Future\awaitAll($futures, new TimeoutCancellation($this->timeoutJob->sec()));
        } catch (CancelledException $e) {
            $ids = '[' . implode(', ', array_keys($futures)) . ']';
            $error = $e->getMessage();
            $prevErr = $e->getPrevious()?->getMessage();
            $this->logger->warning(
                "Canceled by timeout messages: $ids" . PHP_EOL . $error . PHP_EOL . $prevErr
            );
        }
    }

    private function autoRetry(): void
    {
        EventLoop::repeat($this->retryInterval->sec(), function () {
            while (true) {
                $this->logger->debug('new auto retry loop tick');
                $items = $this->redis->retryRange();

                $this->logger->debug('count retry: ' . count($items));

                if (empty($items)) {
                    return;
                }

                foreach ($items as $item) {
                    $this->logger->debug("put from retry to stream");
                    $this->redis->add($item);
                    $this->redis->removeFromRetry($item);
                }
            }
        });
    }

    private function autoClaim(): void
    {
        EventLoop::repeat($this->claimInterval->sec(), function () {
            $start = self::FIRST_ID;
            while (true) {
                $this->logger->debug('new auto claim loop tick');
                $res = $this->redis->autoClaim($start);

                $start = $res[0] ?? self::FIRST_ID;
                $entries = $res[1] ?? [];
                $this->logger->debug('count claim: ' . count($entries));

                $now = Time::nowMs();
                foreach ($entries as $id => $fields) {
                    $fields['_service_data_message_retries'] += 1;
                    if ($fields['_service_data_message_retries'] > $this->maxRetries) {
                        $fields['_service_data_reason'] = 'max retries reached';
                        $this->redis->addToDlq($fields);
                        $this->redis->ack($id);
                        $this->logger->error(
                            'message#' . $fields['_service_data_message_id'] . 'go to dead letter queue'
                        );

                        continue;
                    }

                    $this->logger->debug("put from stream to retry: " . $id);
                    $this->redis->revokeGuard($fields['_service_data_message_id']);
                    $startAt = $now + $this->retry->delay($fields['_service_data_message_retries'])->milli();
                    $this->redis->addToRetry($startAt, $fields);
                    $this->redis->ack($id);
                }

                if ($start === self::FIRST_ID) {
                    $this->logger->debug('claim meet 0-0');

                    return;
                }
            }
        });
    }

    private function lag(): ?int
    {
        $infos = $this->redis->info();
        foreach ($infos as $info) {
            $group = $info['name'] ?? '';
            if ($group === $this->group) {
                return $info['lag'] ?? null;
            }
        }

        return null;
    }
}
