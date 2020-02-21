<?php

/** @noinspection PhpRedundantCatchClauseInspection */

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue;

use ErrorException;
use Exception;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\InvalidPayloadException;
use Illuminate\Queue\Queue;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use VladimirYuldashev\LaravelQueueRabbitMQ\Contracts\Jobs\RpcConfigurable;
use VladimirYuldashev\LaravelQueueRabbitMQ\Contracts\Jobs\RpcJob;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob;

class RabbitMQQueue extends Queue implements QueueContract
{
    /**
     * The RabbitMQ connection instance.
     *
     * @var AbstractConnection
     */
    protected $connection;

    /**
     * The RabbitMQ channel instance.
     *
     * @var AMQPChannel
     */
    protected $channel;

    /**
     * The name of the default queue.
     *
     * @var string
     */
    protected $default;

    /**
     * List of already declared exchanges.
     *
     * @var array
     */
    protected $exchanges = [];

    /**
     * List of already declared queues.
     *
     * @var array
     */
    protected $queues = [];

    /**
     * List of already bound queues to exchanges.
     *
     * @var array
     */
    protected $boundQueues = [];

    /**
     * Current job being processed.
     *
     * @var RabbitMQJob
     */
    protected $currentJob;

    /**
     * @var array
     */
    protected $options;

    /**
     * RabbitMQQueue constructor.
     *
     * @param AbstractConnection $connection
     * @param string $default
     * @param array $options
     */
    public function __construct(
        AbstractConnection $connection,
        string $default,
        array $options = []
    ) {
        $this->connection = $connection;
        $this->channel = $connection->channel();
        $this->default = $default;
        $this->options = $options;
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function size($queue = null): int
    {
        $queue = $this->getQueue($queue);

        if (! $this->isQueueExists($queue)) {
            return 0;
        }

        // create a temporary channel, so the main channel will not be closed on exception
        $channel = $this->connection->channel();
        [, $size] = $channel->queue_declare($queue, true);
        $channel->close();

        return $size;
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $queue, $data), $queue, ['job' => $job]);
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        [$destination, $exchange, $exchangeType, $replyTo, $attempts] = $this->publishProperties($queue, $options);

        $this->declareDestination($destination, $exchange, $exchangeType);

        [$message, $correlationId] = $this->createMessage($payload, $attempts, $replyTo);

        // Publish the message
        $this->channel->basic_publish($message, $exchange, $destination, true, false);

        return $correlationId;
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->laterRaw(
            $delay,
            $this->createPayload($job, $queue, $data),
            $queue
        );
    }

    /**
     * @param $delay
     * @param $payload
     * @param null $queue
     * @param int $attempts
     * @return mixed
     *
     * @throws AMQPProtocolChannelException
     */
    public function laterRaw($delay, $payload, $queue = null, $attempts = 0)
    {
        $ttl = $this->secondsUntil($delay) * 1000;

        // When no ttl just publish a new message to the exchange or queue
        if ($ttl <= 0) {
            return $this->pushRaw($payload, $queue, ['delay' => $delay, 'attempts' => $attempts]);
        }

        $destination = $this->getQueue($queue).'.delay.'.$ttl;

        $this->declareQueue($destination, true, false, $this->getDelayQueueArguments($this->getQueue($queue), $ttl));

        [$message, $correlationId] = $this->createMessage($payload, $attempts);

        // Publish directly on the delayQueue, no need to publish trough an exchange.
        $this->channel->basic_publish($message, null, $destination, true, false);

        return $correlationId;
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function bulk($jobs, $data = '', $queue = null): void
    {
        foreach ((array) $jobs as $job) {
            $this->bulkRaw($this->createPayload($job, $queue, $data), $queue, ['job' => $job]);
        }

        $this->channel->publish_batch();
    }

    /**
     * @param string $payload
     * @param null $queue
     * @param array $options
     *
     * @return mixed
     * @throws AMQPProtocolChannelException
     */
    public function bulkRaw(string $payload, $queue = null, array $options = [])
    {
        [$destination, $exchange, $exchangeType, $replyTo, $attempts] = $this->publishProperties($queue, $options);

        $this->declareDestination($destination, $exchange, $exchangeType);

        [$message, $correlationId] = $this->createMessage($payload, $attempts, $replyTo);

        $this->channel->batch_basic_publish($message, $exchange, $destination);

        return $correlationId;
    }

    /**
     * {@inheritdoc}
     *
     * @throws Exception
     */
    public function pop($queue = null)
    {
        try {
            $queue = $this->getQueue($queue);

            /** @var AMQPMessage|null $message */
            if ($message = $this->channel->basic_get($queue)) {
                return $this->currentJob = new RabbitMQJob(
                    $this->container,
                    $this,
                    $message,
                    $this->connectionName,
                    $queue
                );
            }
        } catch (AMQPProtocolChannelException $exception) {
            // If there is not exchange or queue AMQP will throw exception with code 404
            // We need to catch it and return null
            if ($exception->amqp_reply_code === 404) {

                // Because of the channel exception the channel was closed and removed.
                // We have to open a new channel. Because else the worker(s) are stuck in a loop, without processing.
                $this->channel = $this->connection->channel();

                return null;
            }

            throw $exception;
        }

        return null;
    }

    /**
     * Create a payload string from the given job and data.
     *
     * @param  string|object  $job
     * @param  string  $queue
     * @param  mixed  $data
     * @return string
     *
     * @throws \Illuminate\Queue\InvalidPayloadException
     */
    protected function createPayload($job, $queue, $data = '')
    {
        $payload = parent::createPayload($job, $queue, $data);

        if (! $this->isRpcJob($job)) {
            return $payload;
        }

        return $this->createRpcPayload($job, $payload);
    }

    /**
     * @return AbstractConnection
     */
    public function getConnection(): AbstractConnection
    {
        return $this->connection;
    }

    /**
     * @return AMQPChannel
     */
    public function getChannel(): AMQPChannel
    {
        return $this->channel;
    }

    /**
     * Gets a queue/destination, by default the queue option set on the connection.
     * @param null $queue
     * @return string
     */
    public function getQueue($queue = null)
    {
        return $queue ?: $this->default;
    }

    /**
     * Checks if the given exchange already present/defined in RabbitMQ.
     * Returns false when when the exchange is missing.
     *
     * @param string $exchange
     * @return bool
     * @throws AMQPProtocolChannelException
     */
    public function isExchangeExists(string $exchange): bool
    {
        try {
            // create a temporary channel, so the main channel will not be closed on exception
            $channel = $this->connection->channel();
            $channel->exchange_declare($exchange, '', true);
            $channel->close();

            return true;
        } catch (AMQPProtocolChannelException $exception) {
            if ($exception->amqp_reply_code === 404) {
                return false;
            }

            throw $exception;
        }
    }

    /**
     * Declare a exchange in rabbitMQ, when not already declared or exchange does not already exists in rabbitMQ.
     *
     * @param string $name
     * @param string $type
     * @param bool $durable
     * @param bool $autoDelete
     * @param array $arguments
     *
     * @throws AMQPProtocolChannelException
     */
    public function declareExchange(string $name, string $type = AMQPExchangeType::DIRECT, bool $durable = true, bool $autoDelete = false, array $arguments = []): void
    {
        if ($this->isExchangeDeclared($name)) {
            return;
        }

        if ($this->isExchangeExists($name)) {
            // Add the exchange to the declared exchanges
            $this->exchanges[] = $name;

            return;
        }

        $this->channel->exchange_declare(
            $name,
            $type,
            false,
            $durable,
            $autoDelete,
            false,
            true,
            new AMQPTable($arguments)
        );

        // Add the exchange to the declared exchanges
        $this->exchanges[] = $name;
    }

    /**
     * Checks if the given queue already present/defined in RabbitMQ.
     * Returns false when when the queue is missing.
     *
     * @param string $name
     * @return bool
     * @throws AMQPProtocolChannelException
     */
    public function isQueueExists(?string $name = null): bool
    {
        try {
            $name = $this->getQueue($name);

            // create a temporary channel, so the main channel will not be closed on exception
            $channel = $this->connection->channel();
            $channel->queue_declare($name, true);
            $channel->close();

            return true;
        } catch (AMQPProtocolChannelException $exception) {
            if ($exception->amqp_reply_code === 404) {
                return false;
            }

            throw $exception;
        }
    }

    /**
     * Declare a queue in rabbitMQ, when not already declared or queue does not already exists in rabbitMQ.
     *
     * @param string $name
     * @param bool $durable
     * @param bool $autoDelete
     * @param array $arguments
     *
     * @throws AMQPProtocolChannelException
     */
    public function declareQueue(string $name, bool $durable = true, bool $autoDelete = false, array $arguments = []): void
    {
        if ($this->isQueueDeclared($name)) {
            return;
        }

        if ($this->isQueueExists($name)) {
            // Add the queue to the declared queues
            $this->queues[] = $name;

            return;
        }

        $this->channel->queue_declare(
            $name,
            false,
            $durable,
            false,
            $autoDelete,
            false,
            new AMQPTable($arguments)
        );

        // Add the queue to the declared queues
        $this->queues[] = $name;
    }

    /**
     * @param string $queue
     * @param string $exchange
     * @param string $routingKey
     */
    public function bindQueue(string $queue, string $exchange, string $routingKey = ''): void
    {
        if (in_array(
            implode('', compact('queue', 'exchange', 'routingKey')),
            $this->boundQueues,
            true
        )) {
            return;
        }

        $this->channel->queue_bind($queue, $exchange, $routingKey);
    }

    /**
     * @param null $queue
     */
    public function purge($queue = null): void
    {
        // create a temporary channel, so the main channel will not be closed on exception
        $channel = $this->connection->channel();
        $channel->queue_purge($this->getQueue($queue));
        $channel->close();
    }

    /**
     * @param RabbitMQJob $job
     */
    public function ack(RabbitMQJob $job): void
    {
        $this->channel->basic_ack($job->getRabbitMQMessage()->getDeliveryTag());
    }

    /**
     * Reject current Job.
     *
     * @param RabbitMQJob $job
     * @param bool $requeue
     */
    public function reject(RabbitMQJob $job, bool $requeue = false): void
    {
        $this->channel->basic_reject($job->getRabbitMQMessage()->getDeliveryTag(), $requeue);
    }

    /**
     * Create a AMQP message.
     *
     * @param $payload
     * @param int $attempts
     * @param string|null $replyTo
     *
     * @return array
     */
    protected function createMessage($payload, int $attempts = 0, ?string $replyTo = null): array
    {
        $properties = [
            'content_type' => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ];

        if ($correlationId = json_decode($payload, true)['id'] ?? null) {
            $properties['correlation_id'] = $correlationId;
        }

        if ($this->isPrioritizeDelayed()) {
            $properties['priority'] = $attempts;
        }

        if ($replyTo) {
            $properties['reply-to'] = $replyTo;
        }

        $message = new AMQPMessage($payload, $properties);

        $message->set('application_headers', new AMQPTable([
            'laravel' => [
                'attempts' => $attempts,
            ],
        ]));

        return [
            $message,
            $correlationId,
        ];
    }

    /**
     * Create a payload array from the given job and data.
     *
     * @param object|string $job
     * @param string $queue
     * @param string $data
     * @return array
     */
    protected function createPayloadArray($job, $queue, $data = '')
    {
        return array_merge(parent::createPayloadArray($job, $queue, $data), [
            'id' => $this->getRandomId(),
        ]);
    }

    /**
     * Get a random ID string.
     *
     * @return string
     */
    protected function getRandomId(): string
    {
        return Str::uuid();
    }

    /**
     * @throws Exception
     */
    public function close(): void
    {
        if ($this->currentJob && ! $this->currentJob->isDeletedOrReleased()) {
            $this->reject($this->currentJob, true);
        }

        try {
            $this->connection->close();
        } catch (ErrorException $exception) {
            // Ignore the exception
        }
    }

    /**
     * Get the Queue arguments.
     *
     * @param string $destination
     *
     * @return array
     */
    protected function getQueueArguments(string $destination): array
    {
        $arguments = [];

        // Messages without a priority property are treated as if their priority were 0.
        // Messages with a priority which is higher than the queue's maximum, are treated as if they were
        // published with the maximum priority.
        if ($this->isPrioritizeDelayed()) {
            $arguments['x-max-priority'] = $this->getQueueMaxPriority();
        }

        if ($this->isRerouteFailed()) {
            $arguments['x-dead-letter-exchange'] = $this->getFailedExchange() ?? '';
            $arguments['x-dead-letter-routing-key'] = $this->getFailedRoutingKey($destination);
        }

        return $arguments;
    }

    /**
     * Get the Delay queue arguments.
     *
     * @param string $destination
     * @param int $ttl
     * @return array
     */
    protected function getDelayQueueArguments(string $destination, int $ttl): array
    {
        return [
            'x-dead-letter-exchange' => $this->getExchange() ?? '',
            'x-dead-letter-routing-key' => $this->getRoutingKey($destination),
            'x-message-ttl' => $ttl,
            'x-expires' => $ttl * 2,
        ];
    }

    /**
     * Returns &true;, if delayed messages should be prioritized.
     *
     * @return bool
     */
    protected function isPrioritizeDelayed(): bool
    {
        return boolval(Arr::get($this->options, 'prioritize_delayed') ?: false);
    }

    /**
     * Returns a integer with a default of '2' for when using prioritization on delayed messages.
     * If priority queues are desired, we recommend using between 1 and 10.
     * Using more priority layers, will consume more CPU resources and would affect runtimes.
     *
     * @see https://www.rabbitmq.com/priority.html
     * @return int
     */
    protected function getQueueMaxPriority(): int
    {
        return intval(Arr::get($this->options, 'queue_max_priority') ?: 2);
    }

    /**
     * Get the exchange name, or &null; as default value.
     *
     * @param string $exchange
     * @return string|null
     */
    protected function getExchange(string $exchange = null): ?string
    {
        return $exchange ?: Arr::get($this->options, 'exchange') ?: null;
    }

    /**
     * Get the routing-key for when you use exchanges
     * The default routing-key is the given destination.
     *
     * @param string $destination
     * @return string
     */
    protected function getRoutingKey(string $destination): string
    {
        return ltrim(sprintf(Arr::get($this->options, 'exchange_routing_key') ?: '%s', $destination), '.');
    }

    /**
     * Get the exchangeType, or AMQPExchangeType::DIRECT as default.
     *
     * @param string|null $type
     * @return string
     */
    protected function getExchangeType(?string $type = null): string
    {
        return @constant(AMQPExchangeType::class.'::'.Str::upper($type ?: Arr::get($this->options, 'exchange_type') ?: 'direct')) ?: AMQPExchangeType::DIRECT;
    }

    /**
     * Returns &true;, if failed messages should be rerouted.
     *
     * @return bool
     */
    protected function isRerouteFailed(): bool
    {
        return boolval(Arr::get($this->options, 'reroute_failed') ?: false);
    }

    /**
     * Get the exchange for failed messages.
     *
     * @param string|null $exchange
     * @return string|null
     */
    protected function getFailedExchange(string $exchange = null): ?string
    {
        return $exchange ?: Arr::get($this->options, 'failed_exchange') ?: null;
    }

    /**
     * Get the routing-key for failed messages
     * The default routing-key is the given destination substituted by '.failed'.
     *
     * @param string $destination
     * @return string
     */
    protected function getFailedRoutingKey(string $destination): string
    {
        return ltrim(sprintf(Arr::get($this->options, 'failed_routing_key') ?: '%s.failed', $destination), '.');
    }

    /**
     * @param string $name
     *
     * @return bool
     */
    protected function isExchangeDeclared(string $name): bool
    {
        return in_array($name, $this->exchanges, true);
    }

    /**
     * @param string $name
     *
     * @return bool
     */
    protected function isQueueDeclared(string $name): bool
    {
        return in_array($name, $this->queues, true);
    }

    /**
     *
     * @param string $destination
     * @param string|null $exchange
     *
     * @param string|null $exchangeType
     *
     * @throws AMQPProtocolChannelException
     */
    private function declareDestination(string $destination, ?string $exchange = null, string $exchangeType = AMQPExchangeType::DIRECT): void
    {
        // When a exchange is defined and no exchange is present in RabbitMQ, create an exchange.
        if ($exchange && ! $this->isExchangeExists($exchange)) {
            $this->declareExchange($exchange, $exchangeType);
        }
        // When no exchange is defined, create a queue for amq.direct publishing, but only if it not already present.
        if (! $exchange && ! $this->isQueueExists($destination)) {
            $this->declareQueue($destination, true, false, $this->getQueueArguments($destination));
        }
    }

    /**
     * @param $queue
     * @param array $options
     *
     * @return array
     */
    private function publishProperties($queue, array $options = []): array
    {
        $queue = $this->getQueue($queue);
        $replyTo = null;
        $attempts = Arr::get($options, 'attempts') ?: 0;

        $destination = $this->getRoutingKey($queue);
        $exchange = $this->getExchange();
        $exchangeType = $this->getExchangeType();

        if (($job = Arr::get($options, 'job')) && $this->isRpcJob($job)) {
            [$replyTo, $destination, $exchangeType, $exchange] = $this->rpcPublishProperties($queue, $destination, $exchange, $job);
        }

        return [$destination, $exchange, $exchangeType, $replyTo, $attempts];
    }

    /**
     * @param RpcJob $job
     *
     * @return array
     */
    protected function createRpcPayloadArray(RpcJob $job): array
    {
        return [
            "jsonrpc" => "2.0",
            "method" => $job->rpcMethod() ?: null,
            "params" => $job->rpcParams() ?: null,
        ];
    }

    /**
     * @param $job
     * @param string $payload
     *
     * @return false|string
     */
    protected function createRpcPayload($job, string $payload)
    {
        $payload = json_encode(array_merge([], json_decode($payload, true), $this->createRpcPayloadArray($job)));

        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new InvalidPayloadException(
                'Unable to JSON encode payload. Error code: '.json_last_error()
            );
        }

        return $payload;
    }

    /**
     * @param $job
     *
     * @return bool
     */
    protected function isRpcJob($job)
    {
        if (! is_object($job)) {
            return false;
        }

        return in_array(RpcJob::class, class_uses_recursive($job));
    }

    /**
     * @param $job
     *
     * @return bool
     */
    protected function isRpcConfigurable($job)
    {
        if (! is_object($job)) {
            return false;
        }

        return in_array(RpcConfigurable::class, class_uses_recursive($job));
    }

    /**
     * @param $queue
     * @param string $destination
     * @param string|null $exchange
     * @param $job
     *
     * @return array
     */
    private function rpcPublishProperties($queue, string $destination, ?string $exchange, $job): array
    {
        $replyTo = $this->getRpcCallbackQueue($destination, $exchange);

        if ($this->isRpcConfigurable($job)) {
            /** @var RpcConfigurable $job */
            $destination = $this->getRpcRoutingKey($job->rpcDestination($queue), $job->rpcRoutingKey());
            $exchangeType = $this->getRpcExchangeType($job->rpcExchangeType());
            $exchange = $this->getRpcExchange($job->rpcExchange());
        } else {
            $destination = $this->getRpcRoutingKey($queue);
            $exchangeType = $this->getRpcExchangeType();
            $exchange = $this->getRpcExchange();
        }

        return array($replyTo, $destination, $exchangeType, $exchange);
    }

    /**
     * @param string $destination
     *
     * @param string|null $exchange
     *
     * @return string
     */
    protected function getRpcCallbackQueue(string $destination, ?string $exchange = null): string
    {
        return trim(sprintf('%s%s', $destination, $exchange ? '@'.$exchange : null), '-.');
    }

    /**
     *
     * @param string $destination
     * @param string|null $routingKey
     *
     * @return string
     */
    protected function getRpcRoutingKey(string $destination, ?string $routingKey = null): string
    {
        return trim(sprintf($routingKey ?: Arr::get($this->options, 'rpc_routing_key') ?: 'rpc-%s', $destination), '.-');
    }

    /**
     * Get the Rpc exchange name, or &null; as default value.
     *
     * @param string|null $exchange
     * @return string|null
     */
    protected function getRpcExchange(?string $exchange = null): ?string
    {
        return $exchange ?: Arr::get($this->options, 'rpc_exchange') ?: null;
    }

    /**
     * Get the Rpc exchangeType, or AMQPExchangeType::DIRECT as default.
     *
     * @param string|null $type
     * @return string
     */
    protected function getRpcExchangeType(?string $type = null): string
    {
        return $this->getExchangeType($type ?: Arr::get($this->options, 'rpc_exchange_type'));
    }
}
