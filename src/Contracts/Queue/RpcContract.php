<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Contracts\Queue;

interface RpcContract
{
    /** @return bool */
    public function isProcessedRpc(): bool;

    /** @return mixed */
    public function rpcResult();

    /** @return mixed */
    public function rpcError();

    /** @return string|null */
    public function replyTo(): ?string;
}
