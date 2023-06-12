<?php

namespace AvtoDev\AmqpRabbitLaravelQueue\Horizon\Listeners;

use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\Events\JobFailed as LaravelJobFailed;
use Laravel\Horizon\Events\JobFailed as HorizonJobFailed;
use AvtoDev\AmqpRabbitLaravelQueue\Job as RabbitMQJob;

class RabbitMQFailedEvent
{
    /**
     * The event dispatcher implementation.
     *
     * @var Dispatcher
     */
    public $events;

    /**
     * Create a new listener instance.
     *
     * @param  Dispatcher  $events
     * @return void
     */
    public function __construct(Dispatcher $events)
    {
        $this->events = $events;
    }

    /**
     * Handle the event.
     *
     * @param  LaravelJobFailed|HorizonJobFailed $event
     * @return void
     */
    public function handle(LaravelJobFailed|HorizonJobFailed $event): void
    {
        if (! $event->job instanceof RabbitMQJob) {
            return;
        }

        $this->events->dispatch((new HorizonJobFailed(
            $event->exception,
            $event->job,
            $event->job->getRawBody()
        ))->connection($event->connectionName)->queue($event->job->getQueue()));
    }
}
