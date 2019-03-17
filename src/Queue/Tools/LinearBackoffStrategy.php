<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Tools;


class LinearBackoffStrategy extends AbstractBackoffStrategy
{
    /**
     * Delay is in milliseconds.
     *
     * @param int $delay
     * @param int $attempt
     * @return int
     */
    public function backoffDelayTime(int $delay, int $attempt): int
    {
        return $attempt * $delay;
    }

}
