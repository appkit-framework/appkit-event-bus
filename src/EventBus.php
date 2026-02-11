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
    const AMQP_PREFIX = 'ak_eb';

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

        $this -> log = $log -> withModule(static::class);
        $this -> instanceId = bin2hex(random_bytes(8));
    }

    public function start() {
        $mainExchange = self::AMQP_PREFIX;

        try {
            $this -> amqp -> declareExchange(
                $mainExchange,
                'direct',
                false, // passive
                true, // durable
                false // autoDelete
            );
            $this -> log -> debug(
                'Declared main exchange',
                [ 'exchange' => $mainExchange ]
            );
        } catch(Throwable $e) {
            $error = 'Failed to declare main exchange';
            $this -> log -> error(
                $error,
                [ 'exchange' => $mainExchange ],
                $e
            );
            throw new EventBusException(
                $error,
                previous: $e
            );
        }

        $this -> amqp -> on('connect', async(function() {
            return $this -> onAmqpReconnect();
        }));

        $this -> log -> info(
            'Event bus is ready',
            [ 'instanceId' => $this -> instanceId ]
        );

        $this -> isStarted = true;
    }

    public function stop() {
        $this -> isStarted = false;

        foreach($this -> subRestoreData as $subId => $_) {
            $this -> log -> warning(
                'Subscription is still active at shutdown',
                [ 'subId' => $subId ]
            );
            try {
                $this -> unsub($subId);
                $this -> log -> info(
                    'Canceled subscription',
                    [ 'subId' => $subId ]
                );
            } catch(Throwable $e) {
                $this -> log -> error(
                    'Failed to cancel subscription',
                    [ 'subId' => $subId ],
                    $e
                );
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

        $exchange = self::AMQP_PREFIX;
        $routingKey = $this -> appId . '_' . $event;
        try {
            return $this -> amqp -> publish(
                $bodyJson,
                $headers,
                $exchange,
                $routingKey,
                confirm: true
            );
        } catch(Throwable $e) {
            $error = 'Failed to publish AMQP message';
            $this -> log -> error(
                $error,
                [
                    'event' => $event,
                    'headers' => $headers,
                    'exchange' => $exchange,
                    'routingKey' => $routingKey
                ],
                $e
            );
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
        $subId = $appId . '/' . $event . '@' . $context;

        if(isset($this -> subRestoreData[$subId]))
            throw new EventBusException("Context $context already in use for event $appId/$event");

        if($broadcast && $persistent)
            throw new EventBusException('Broadcast and persistent cannot be used together');

        $subRestoreData = [
            $appId,
            $event,
            $handler,
            $headers,
            $context,
            $broadcast,
            $persistent,
            $concurrency,
            $prefetchCount
        ];

        $this -> subInternal($subId, ...$subRestoreData);
        $this -> subRestoreData[$subId] = $subRestoreData;

        $this -> log -> debug(
            'Created subscription',
            [
                'subId' => $subId,
                'headers' => $headers,
                'broadcast' => $broadcast,
                'persistent' => $persistent,
                'concurrency' => $concurrency,
                'prefetchCount' => $prefetchCount
            ]
        );

        return $subId;
    }

    public function unsub($subId) {
        if(!isset($this -> subRestoreData[$subId]))
            throw new EventBusException("Invalid subscription ID $subId");

        if(isset($this -> subUnsubData[$subId])) {
            $this -> unsubInternal($this -> subUnsubData[$subId], $subId);
            unset($this -> subUnsubData[$subId]);
        }

        unset($this -> subRestoreData[$subId]);
        $this -> log -> debug(
            'Cleaned up subscription',
            [ 'subId' => $subId ]
        );

        return $this;
    }

    private function subInternal(
        $subId,
        $appId,
        $event,
        $handler,
        $headers,
        $context,
        $broadcast,
        $persistent,
        $concurrency,
        $prefetchCount
    ) {
        $unsubData = [];

        try {
            $eventExchange = self::AMQP_PREFIX . '_' . $appId . '_' . $event;

            try {
                $this -> amqp -> declareExchange(
                    $eventExchange,
                    'headers',
                    false, // passive
                    true, // durable
                    true, // autoDelete
                    true // internal
                );
                $this -> log -> debug(
                    'Declared event exchange',
                    [
                        'exchange' => $eventExchange,
                        'subId' => $subId
                    ]
                );
            } catch(Throwable $e) {
                $error = 'Failed to declare event exchange';
                $this -> log -> error(
                    $error,
                    [
                        'exchange' => $eventExchange,
                        'subId' => $subId
                    ],
                    $e
                );
                throw new EventBusException($error, previous: $e);
            }

            $mainExchange = self::AMQP_PREFIX;
            $routingKey = $appId . '_' . $event;

            try {
                $this -> amqp -> bindExchange(
                    $eventExchange,
                    $mainExchange,
                    $routingKey
                );
                $this -> log -> debug(
                    'Bound event exchange to main exchange',
                    [
                        'eventExchange' => $eventExchange,
                        'mainExchange' => $mainExchange,
                        'routingKey' => $routingKey,
                        'subId' => $subId
                    ]
                );
            } catch(Throwable $e) {
                $error = 'Failed to bind event exchange to main exchange';
                $this -> log -> error(
                    $error,
                    [
                        'eventExchange' => $eventExchange,
                        'mainExchange' => $mainExchange,
                        'routingKey' => $routingKey,
                        'subId' => $subId
                    ],
                    $e
                );
                throw new EventBusException($error, previous: $e);
            }

            $queue = $eventExchange . '_' . $this -> appId . '_' . $context;
            if($broadcast)
                $queue .= '_' . $this -> instanceId;
            if(strlen($queue) > 255)
                $queue = self::AMQP_PREFIX . '_' . hash('sha256', $queue);

            $durable = $persistent;
            $exclusive = $broadcast;
            $autoDelete = ! $persistent;

            try {
                $this -> amqp -> declareQueue(
                    $queue,
                    false, // passive
                    $durable, // durable
                    $exclusive, // exclusive
                    $autoDelete // autoDelete
                );
                $this -> log -> debug(
                    'Declared subscription queue',
                    [
                        'queue' => $queue,
                        'durable' => $durable,
                        'exclusive' => $exclusive,
                        'autoDelete' => $autoDelete,
                        'subId' => $subId
                    ]
                );
            } catch(Throwable $e) {
                $error = 'Failed to declare subscription queue';
                $this -> log -> error(
                    $error,
                    [
                        'queue' => $queue,
                        'durable' => $durable,
                        'exclusive' => $exclusive,
                        'autoDelete' => $autoDelete,
                        'subId' => $subId
                    ],
                    $e
                );
                throw new EventBusException($error, previous: $e);
            }

            if(! $persistent)
                $unsubData['queue'] = $queue;

            try {
                $this -> amqp -> bindQueue(
                    $queue,
                    $eventExchange,
                    arguments: $headers
                );
                $this -> log -> debug(
                    'Bound subscription queue to event exchange',
                    [
                        'queue' => $queue,
                        'exchange' => $eventExchange,
                        'headers' => $headers,
                        'subId' => $subId
                    ]
                );
            } catch(Throwable $e) {
                $error = 'Failed to bind subscription queue to event exchange';
                $this -> log -> error(
                    $error,
                    [
                        'queue' => $queue,
                        'exchange' => $eventExchange,
                        'headers' => $headers,
                        'subId' => $subId
                    ],
                    $e
                );
                throw new EventBusException($error, previous: $e);
            }

            $exclusive = $broadcast;

            try {
                $consumerTag = $this -> amqp -> consume(
                    $queue,
                    function($body, $headers) use($handler, $subId) {
                        return $this -> handleMessage(
                            $body,
                            $headers,
                            $handler,
                            $subId
                        );
                    },
                    exclusive: $exclusive,
                    concurrency: $concurrency,
                    prefetchCount: $prefetchCount
                );
                $this -> log -> debug(
                    'Started consumer',
                    [
                        'consumerTag' => $consumerTag,
                        'queue' => $queue,
                        'exclusive' => $exclusive,
                        'concurrency' => $concurrency,
                        'prefetchCount' => $prefetchCount,
                        'subId' => $subId
                    ]
                );
            } catch(Throwable $e) {
                $error = 'Failed to start consumer';
                $this -> log -> error(
                    $error,
                    [
                        'queue' => $queue,
                        'exclusive' => $exclusive,
                        'concurrency' => $concurrency,
                        'prefetchCount' => $prefetchCount,
                        'subId' => $subId
                    ],
                    $e
                );
                throw new EventBusException($error, previous: $e);
            }

            $unsubData['consumerTag'] = $consumerTag;
        } catch(EventBusException $e) {
            if(!empty($unsubData)) {
                try {
                    $this -> unsubInternal($unsubData, $subId);
                    $this -> log -> debug(
                        'Rolled back subscription',
                        [
                            'unsubData' => $unsubData,
                            'subId' => $subId
                        ]
                    );
                } catch(EventBusException $e) {
                    $this -> log -> error(
                        'Failed to rollback subscription',
                        [
                            'unsubData' => $unsubData,
                            'subId' => $subId
                        ],
                        $e
                    );
                }
            }
            throw $e;
        }

        $this -> subUnsubData[$subId] = $unsubData;
    }

    private function unsubInternal($unsubData, $subId) {
        if(isset($unsubData['consumerTag'])) {
            $consumerTag = $unsubData['consumerTag'];

            try {
                $this -> amqp -> cancelConsumer($consumerTag);
                $this -> log -> debug(
                    'Canceled consumer',
                    [
                        'consumerTag' => $consumerTag,
                        'subId' => $subId
                    ]
                );
            } catch(Throwable $e) {
                $error = "Failed to cancel consumer";
                $this -> log -> error(
                    $error,
                    [
                        'consumerTag' => $consumerTag,
                        'subId' => $subId
                    ],
                    $e
                );
                throw new EventBusException($error, previous: $e);
            }
        }

        if(isset($unsubData['queue'])) {
            $queue = $unsubData['queue'];

            try {
                $this -> amqp -> deleteQueue($queue);
                $this -> log -> debug(
                    'Deleted queue',
                    [
                        'queue' => $queue,
                        'subId' => $subId
                    ]
                );
            } catch(Throwable $e) {
                $error = "Failed to delete queue";
                $this -> log -> error(
                    $error,
                    [
                        'queue' => $queue,
                        'subId' => $subId
                    ],
                    $e
                );
                throw new EventBusException($error, previous: $e);
            }
        }
    }

    private function onAmqpReconnect() {
        $this -> log -> warning('Detected AMQP client reconnect, restoring all subscriptions');

        foreach($this -> subRestoreData as $subId => $restoreData) {
            try {
                $this -> subInternal($subId, ...$restoreData);
                $this -> log -> info(
                    'Restored subscription',
                    [ 'subId' => $subId ]
                );
            } catch(Throwable $e) {
                $this -> log -> error(
                    'Failed to restore subscription',
                    [ 'subId' => $subId ],
                    $e
                );
            }
        }
    }

    private function handleMessage($bodyJson, $headers, $handler, $subId) {
        $this -> log -> setContext('ebSubId', $subId);

        try {
            $body = Json::decode($bodyJson);
        } catch(Throwable $e) {
            $error = 'Failed to decode message';
            $this -> log -> warning($error, $e);
            throw new AmqpNackReject($error, previous: $e);
        }

        try {
            $handler($body, $headers);
        } catch(Throwable $e) {
            $this -> log -> error(
                'Uncaught exception in event handler, requeuing event',
                $e
            );
            throw new AmqpNackRequeue(
                'Exception in event handler',
                previous: $e
            );
        }
    }
}
