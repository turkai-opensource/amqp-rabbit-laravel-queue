<?php

declare(strict_types = 1);

namespace AvtoDev\AmqpRabbitLaravelQueue;

use AvtoDev\AmqpRabbitLaravelQueue\Horizon\RabbitMQQueue;
use AvtoDev\AmqpRabbitManager\ConnectionsFactoryInterface;
use AvtoDev\AmqpRabbitManager\ExchangesFactoryInterface;
use AvtoDev\AmqpRabbitManager\QueuesFactoryInterface;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use InvalidArgumentException;

class Connector implements \Illuminate\Queue\Connectors\ConnectorInterface
{
    /**
     * Connector (driver) name.
     */
    public const NAME = 'rabbitmq';

    /**
     * @var ConnectionsFactoryInterface
     */
    protected $connections;

    /**
     * @var QueuesFactoryInterface
     */
    protected $queues;

    /**
     * @var Container
     */
    protected $container;

    /**
     * @var ExchangesFactoryInterface
     */
    protected $exchanges;

    /**
     * Connector constructor.
     *
     * @param Container                   $container
     * @param ConnectionsFactoryInterface $connections
     * @param QueuesFactoryInterface      $queues
     * @param ExchangesFactoryInterface   $exchanges
     */
    public function __construct(Container $container,
                                ConnectionsFactoryInterface $connections,
                                QueuesFactoryInterface $queues,
                                ExchangesFactoryInterface $exchanges)
    {
        $this->container   = $container;
        $this->connections = $connections;
        $this->queues      = $queues;
        $this->exchanges   = $exchanges;
    }

    /**
     * Establish a queue connection.
     *
     * @param array<string, string|int|null> $config
     *
     * @throws InvalidArgumentException
     *
     * @return QueueContract
     */
    public function connect(array $config): QueueContract
    {
        if (! isset($config['connection'])) {
            throw new InvalidArgumentException('RabbitMQ connection name was not passed');
        }

        if (! isset($config['queue_id'])) {
            throw new InvalidArgumentException('RabbitMQ queue ID was not passed');
        }

        $connection = $this->connections->make((string) $config['connection']);
        $queue      = $this->queues->make((string) $config['queue_id']);
        $timeout    = (int) ($config['timeout'] ?? 0);
        $resume     = (bool) ($config['resume'] ?? false);

        $delayed_exchange = isset($config['delayed_exchange_id'])
            ? $this->exchanges->make((string) $config['delayed_exchange_id'])
            : null;

        if (isset($config['worker']) && $config['worker'] === 'horizon')
            return new RabbitMQQueue($this->container, $connection, $queue, $timeout, $resume, $delayed_exchange);

        return new Queue($this->container, $connection, $queue, $timeout, $resume, $delayed_exchange);
    }
}
