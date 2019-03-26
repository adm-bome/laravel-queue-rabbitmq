<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Tools;

interface prioritizeAware
{
    public function setPrioritize(?bool $prioritize = null);
}
