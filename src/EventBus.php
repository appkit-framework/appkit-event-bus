<?php

namespace AppKit\EventBus;

use AppKit\StartStop\StartStopInterface;
use AppKit\Health\HealthIndicatorInterface;
use AppKit\Health\HealthCheckResult;
use AppKit\Json\Json;
use AppKit\Amqp\AmqpNackReject;
use AppKit\Amqp\AmqpNackRequeue;
use function AppKit\Async\async;

use Throwable;

class EventBus implements StartStopInterface, HealthIndicatorInterface {
    const AMQP_PREFIX = 'appkit_eventbus';

    private $appId;
    private $amqp;

    private $log;
    private $instanceId;
    private $isStarted = false;
    private $subRestoreData = [];
    private $subUnsubData = [];
    
    function __construct($appId, $log, $amqp) {
        $this -> appId = $appId;
        $this -> amqp = $amqp;

        $this -> log = $log -> withModule($this);
        $this -> instanceId = bin2hex(random_bytes(8));
    }

    public function start() {
        try {
            $this -> amqp -> declareExchange(
                self::AMQP_PREFIX,
                'direct',
                false, // passive
                true, // durable
                false // autoDelete
            );
            $this -> log -> debug('Declared event bus exchange');
        } catch(Throwable $e) {
            $error = 'Failed to declare event bus exchange';
            $this -> log -> error($error, $e);
            throw new EventBusException(
                $error,
                previous: $e
            );
        }

        $this -> amqp -> on('connect', async(function() {
            return $this -> onAmqpReconnect();
        }));

        $this -> log -> info('Event bus is ready, instance ID: '. $this -> instanceId);

        $this -> isStarted = true;
    }

    public function stop() {
        $this -> isStarted = false;

        foreach($this -> subRestoreData as $tag => $_) {
            $this -> log -> warning("Subscription $tag is still active at shutdown");
            try {
                $this -> unsub($tag);
                $this -> log -> info("Canceled subscription $tag");
            } catch(Throwable $e) {
                $this -> log -> error("Failed to cancel subscription $tag", $e);
            }
        }
    }

    public function checkHealth() {
        return new HealthCheckResult([
            'AMQP client' => $this -> amqp,
            'Started' => $this -> isStarted
        ]);
    }

    public function emit($event, $body = [], $headers = [], $ttl = 0) {
        $headers['delivery_mode'] = $ttl == 0 ? 2 : 1; // 1=transient, 2=persistent
        if($ttl)
            $headers['expiration'] = (string)($ttl * 1000);

        try {
            $bodyJson = Json::encode($body);
        } catch(Throwable $e) {
            throw new EventBusException(
                'Failed to encode message: ' . $e -> getMessage(),
                previous: $e
            );
        }

        try {
            return $this -> amqp -> publish(
                $bodyJson,
                $headers,
                self::AMQP_PREFIX,
                $this -> appId . '_' . $event,
                confirm: true
            );
        } catch(Throwable $e) {
            $error = 'Failed to publish AMQP message';
            $this -> log -> error($error, $e);
            throw new EventBusException(
                $error,
                previous: $e
            );
        }
    }

    public function sub(
        $appId,
        $event,
        $handler,
        $headers = [],
        $context = 'default',
        $broadcast = true,
        $persistent = false,
        $concurrency = 1,
        $prefetchCount = null
    ) {
        $tag = "${appId}_${event}_" . $this -> appId . "_$context";
        if($broadcast)
            $tag .= '_' . $this -> instanceId;

        if(isset($this -> subRestoreData[$tag]))
            throw new EventBusException("Context $context already in use for event $appId/$event");

        if($broadcast && $persistent)
            throw new EventBusException('Broadcast and persistent cannot be used together');

        $subRestoreData = [
            $tag,
            $appId,
            $event,
            $handler,
            $headers,
            $broadcast,
            $persistent,
            $concurrency,
            $prefetchCount
        ];

        $this -> subInternal(...$subRestoreData);
        $this -> subRestoreData[$tag] = $subRestoreData;

        $this -> log -> debug("New subscription: $appId/$event => $context, tag: $tag");

        return $tag;
    }

    public function unsub($tag) {
        if(!isset($this -> subRestoreData[$tag]))
            throw new EventBusException("Invalid subscription tag $tag");

        if(isset($this -> subUnsubData[$tag])) {
            $this -> unsubInternal($this -> subUnsubData[$tag]);
            unset($this -> subUnsubData[$tag]);
        }

        unset($this -> subRestoreData[$tag]);
        $this -> log -> debug("Cleaned up subscription $tag");

        return $this;
    }

    private function subInternal(
        $subTag,
        $appId,
        $event,
        $handler,
        $headers,
        $broadcast,
        $persistent,
        $concurrency,
        $prefetchCount
    ) {
        $unsubData = [];
        try {
            $exchange = self::AMQP_PREFIX . "_${appId}_$event";

            try {
                $this -> amqp -> declareExchange(
                    $exchange,
                    'headers',
                    false, // passive
                    true, // durable
                    true, // autoDelete
                    true // internal
                );
                $this -> log -> debug("Declared exchange $exchange");
            } catch(Throwable $e) {
                $error = 'Failed to declare exchange';
                $this -> log -> error("$error $exchange", $e);
                throw new EventBusException($error, previous: $e);
            }

            try {
                $routingKey = "${appId}_$event";
                $logMsg = "$exchange to " . self::AMQP_PREFIX . " by routing key $routingKey";
                $this -> amqp -> bindExchange(
                    $exchange,
                    self::AMQP_PREFIX,
                    $routingKey
                );
                $this -> log -> debug("Bound exchange $logMsg");
            } catch(Throwable $e) {
                $error = 'Failed to bind exchange';
                $this -> log -> error("$error $logMsg", $e);
                throw new EventBusException($error, previous: $e);
            }

            $queue = self::AMQP_PREFIX . "_$subTag";
            if(strlen($queue) > 255)
                $queue = self::AMQP_PREFIX . '_' . hash('sha256', $subTag);

            try {
                $this -> amqp -> declareQueue(
                    $queue,
                    false, // passive
                    $persistent, // durable
                    $broadcast, // exclusive
                    ! $persistent // autoDelete
                );
                $this -> log -> debug("Declared queue $queue");
            } catch(Throwable $e) {
                $error = 'Failed to declare queue';
                $this -> log -> error("$error $queue", $e);
                throw new EventBusException($error, previous: $e);
            }

            if(! $persistent)
                $unsubData['queue'] = $queue;

            try {
                $logMsg = "$queue to exchange $exchange";
                $this -> amqp -> bindQueue(
                    $queue,
                    $exchange,
                    arguments: $headers
                );
                $this -> log -> debug("Bound queue $logMsg");
            } catch(Throwable $e) {
                $error = 'Failed to bind queue';
                $this -> log -> error("$error $logMsg", $e);
                throw new EventBusException($error, previous: $e);
            }

            try {
                $ctag = $this -> amqp -> consume(
                    $queue,
                    function($body, $headers) use($handler, $appId, $event) {
                        return $this -> handleMessage(
                            $body,
                            $headers,
                            $handler,
                            $appId,
                            $event
                        );
                    },
                    exclusive: $broadcast,
                    concurrency: $concurrency,
                    prefetchCount: $prefetchCount
                );
                $this -> log -> debug("Consumed queue $queue, consumer tag: $ctag");
            } catch(Throwable $e) {
                $error = 'Failed to consume queue';
                $this -> log -> error("$error $queue");
                throw new EventBusException($error, previous: $e);
            }

            $unsubData['ctag'] = $ctag;
        } catch(EventBusException $e) {
            if(!empty($unsubData)) {
                try {
                    $this -> unsubInternal($unsubData);
                    $this -> log -> debug("Rolled back subscription $subTag");
                } catch(EventBusException $e) {
                    $this -> log -> error("Failed to rollback subscription $subTag", $e);
                }
            }
            throw $e;
        }

        $this -> subUnsubData[$subTag] = $unsubData;
    }

    private function unsubInternal($unsubData) {
        if(isset($unsubData['ctag'])) {
            $ctag = $unsubData['ctag'];
            try {
                $this -> amqp -> cancelConsumer($ctag);
                $this -> log -> debug("Canceled consumer $ctag");
            } catch(Throwable $e) {
                $error = "Failed to cancel consumer";
                $this -> log -> error("$error $ctag", $e);
                throw new EventBusException($error, previous: $e);
            }
        }

        if(isset($unsubData['queue'])) {
            $queue = $unsubData['queue'];
            try {
                $this -> amqp -> deleteQueue($queue);
                $this -> log -> debug("Deleted queue $queue");
            } catch(Throwable $e) {
                $error = "Failed to delete queue";
                $this -> log -> error("$error $queue", $e);
                throw new EventBusException($error, previous: $e);
            }
        }
    }

    private function onAmqpReconnect() {
        $this -> log -> warning("Detected AMQP client reconnect, restoring all subscriptions...");

        foreach($this -> subRestoreData as $tag => $restoreData) {
            try {
                $this -> subInternal(...$restoreData);
                $this -> log -> info("Restored subscription $tag");
            } catch(Throwable $e) {
                $this -> log -> error("Failed to restore subscription $tag", $e);
            }
        }
    }

    private function handleMessage($bodyJson, $headers, $handler, $appId, $event) {
        $messageIdStr = $headers['message-id'] ?? 'missing message-id';

        try {
            $body = Json::decode($bodyJson);
        } catch(Throwable $e) {
            $this -> log -> warning(
                "Rejecting corrupted event $appId/${event}[$messageIdStr]",
                $e
            );
            throw new AmqpNackReject(
                "Failed to decode message",
                previous: $e
            );
        }

        try {
            $handler($body, $headers);
        } catch(Throwable $e) {
            $this -> log -> error(
                "Requeuing event $appId/${event}[$messageIdStr] due to exception",
                $e
            );
            throw new AmqpNackRequeue(
                "Exception in event handler",
                previous: $e
            );
        }
    }
}
