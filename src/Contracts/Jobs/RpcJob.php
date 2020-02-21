<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Contracts\Jobs;

interface RpcJob
{
    /**
     * Return the method for the rpc call
     *
     * @return string|null
     */
    public function rpcMethod(): ?string;

    /**
     * Return the parameters for the rpc call
     *
     * @return array|null
     */
    public function rpcParams(): ?array;
}
