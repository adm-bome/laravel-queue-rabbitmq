<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Contracts\Jobs;

interface RpcConfigurable
{
    /**
     * Return the method for the rpc call
     *
     * @param string $queue
     *
     * @return string
     */
    public function rpcDestination(string $queue): string;

    /**
     * Return the parameters for the rpc call
     *
     * @return string|null
     */
    public function rpcExchange(): ?string;

    /**
     * Return the parameters for the rpc call
     *
     * @return string|null
     */
    public function rpcExchangeType(): ?string;

    /**
     * Return the parameters for the rpc call
     *
     * @return string|null
     */
    public function rpcRoutingKey(): ?string;
}
