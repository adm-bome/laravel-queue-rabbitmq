<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Tools;

use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpMessage;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob;
use Interop\Amqp\AmqpDestination;
use Enqueue\AmqpTools\RabbitMqDlxDelayStrategy;

class DlxDelayStrategy extends RabbitMqDlxDelayStrategy implements BackoffStrategyAware
{
    use BackoffStrategyAwareTrait;

    /**
     * Delay is in milliseconds.
     *
     * @param AmqpContext $context
     * @param AmqpDestination $dest
     * @param AmqpMessage $message
     * @param int $delay
     * @throws \Interop\Queue\Exception\InvalidDestinationException
     */
    public function delayMessage(AmqpContext $context, AmqpDestination $dest, AmqpMessage $message, int $delay): void
    {
        if ($this->backoffStrategy) {
            $delay = $this->backoffStrategy->backoffDelayTime(
                $delay,
                $message->getProperty(RabbitMQJob::ATTEMPT_COUNT_HEADERS_KEY, 1)
            );
        }

        parent::delayMessage($context, $dest, $message, $delay);
    }

}
