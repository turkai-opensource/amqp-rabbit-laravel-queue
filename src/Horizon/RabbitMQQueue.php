<?php

namespace AvtoDev\AmqpRabbitLaravelQueue\Horizon;

use AvtoDev\AmqpRabbitLaravelQueue\Job;
use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\Str;
use Laravel\Horizon\Events\JobDeleted;
use Laravel\Horizon\Events\JobPushed;
use Laravel\Horizon\Events\JobReserved;
use Laravel\Horizon\JobPayload;
use AvtoDev\AmqpRabbitLaravelQueue\Job as RabbitMQJob;
use AvtoDev\AmqpRabbitLaravelQueue\Queue as BaseRabbitMQQueue;

class RabbitMQQueue extends BaseRabbitMQQueue
{
    /**
     * The job that last pushed to queue via the "push" method.
     *
     * @var object|string
     */
    protected $lastPushed;

    /**
     * Get the number of queue jobs that are ready to process.
     *
     * @param string|null $queue
     * @return int
     *
     */
    public function readyNow(string $queue = null): int
    {
        return $this->size($queue);
    }

    /**
     * {@inheritdoc}
     */
    public function push($job, $data = '', $queue = null, ?int $priority = null): void
    {
        $this->lastPushed = $job;

        parent::push($job, $data, $queue, $priority);
    }

    /**
     * {@inheritdoc}
     * @throws BindingResolutionException
     */
    public function pushRaw($payload, $queue = null, array $options = []): void
    {
        $payload = (new JobPayload($payload))->prepare($this->lastPushed)->value;

        parent::pushRaw($payload, $queue, $options);
        $this->event($this->getQueue($queue), new JobPushed($payload));
    }

    /**
     * {@inheritdoc}
     * @throws BindingResolutionException
     */
    public function later($delay, $job, $data = '', $queue = null, ?int $priority = null)
    {
        $payload = (new JobPayload($this->createPayload($job, $data)))->prepare($job)->value;

        parent::later($delay, $job, $data, $queue, $priority);
        $this->event($this->getQueue($queue), new JobPushed($payload));
    }

    /**
     * {@inheritdoc}
     */
    public function pop($queue = null): ?Job
    {
        return tap(parent::pop($queue), function ($result) use ($queue): void {
            if (is_a($result, RabbitMQJob::class, true)) {
                $this->event($this->getQueue($queue), new JobReserved($result->getRawBody()));
            }
        });
    }

    /**
     * {@inheritdoc}
     */
    public function release($delay, $job, $data, $queue, $attempts = 0)
    {
        $this->lastPushed = $job;

        return parent::release($delay, $job, $data, $this->getQueue($queue), $attempts);
    }

    /**
     * Fire the job deleted event.
     *
     * @param string $queue
     * @param RabbitMQJob $job
     * @return void
     *
     * @throws BindingResolutionException
     */
    public function deleteReserved($queue, $job): void
    {
        $this->event($this->getQueue($queue), new JobDeleted($job, $job->getRawBody()));
    }

    /**
     * Fire the given event if a dispatcher is bound.
     *
     * @param string $queue
     * @param mixed $event
     * @return void
     *
     * @throws BindingResolutionException
     */
    protected function event($queue, $event): void
    {
        if ($this->container && $this->container->bound(Dispatcher::class)) {
            $this->container->make(Dispatcher::class)->dispatch(
                $event->connection($this->getConnectionName())->queue($this->getQueue($queue))
            );
        }
    }

    /**
     * Get the connection name for the queue.
     *
     * @return string
     */
    public function getConnectionName()
    {
        return $this->connectionName;
    }
}
