package middleware

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"tp1/common/message"
)

type Consumer struct {
	conn           *amqp.Connection
	ch             *amqp.Channel
	msgChannel     <-chan amqp.Delivery
	eofsReceived   int
	config         ConsumerConfig
	sigtermChannel chan os.Signal
}

type ConsumerConfig struct {
	connectionString       string
	exchangeName           string
	instanceID             string
	previousStageInstances int
	routeByID              bool
	routingKey             string
}

func newConsumerConfig(configID string) (ConsumerConfig, error) {
	v := viper.New()
	v.SetConfigFile("./middleware_config.yaml")
	if err := v.ReadInConfig(); err != nil {
		return ConsumerConfig{}, fmt.Errorf("Configuration for consumer %s could not be read from config file: %w\n", configID, err)
	}
	exchangeName := v.GetString(fmt.Sprintf("%s.exchange_name", configID))
	routeByID := v.GetBool(fmt.Sprintf("%s.route_by_id", configID))
	routingKeyEnv := v.GetString(fmt.Sprintf("%s.routing_key_env", configID))
	previousStageInstancesEnv := v.GetString(fmt.Sprintf("%s.prev_stage_instances_env", configID))
	previousStageInstances, err := strconv.Atoi(os.Getenv(previousStageInstancesEnv))
	if err != nil {
		previousStageInstances = 1
	}
	routingKey := ""
	if routingKeyEnv != "" {
		routingKey = os.Getenv(routingKeyEnv)
	}
	instanceID := os.Getenv("ID")
	connectionString := os.Getenv("RABBITMQ_CONNECTION_STRING")

	return ConsumerConfig{
		connectionString:       connectionString,
		exchangeName:           exchangeName,
		instanceID:             instanceID,
		previousStageInstances: previousStageInstances,
		routeByID:              routeByID,
		routingKey:             routingKey,
	}, nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func NewConsumer(configID string) (*Consumer, error) {
	config, err := newConsumerConfig(configID)
	if err != nil {
		return nil, err
	}
	conn, err := amqp.Dial(config.connectionString)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = ch.ExchangeDeclare(
		config.exchangeName, // name
		"direct",            // type
		false,               // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	queueName := config.exchangeName + config.routingKey + config.instanceID
	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		true,      // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	routingKey := config.routingKey
	if config.routeByID {
		routingKey += config.instanceID
	}

	err = ch.QueueBind(
		q.Name,              // queue name
		routingKey,          // routing key
		config.exchangeName, // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	err = ch.QueueBind(
		q.Name,              // queue name
		"eof",               // routing key
		config.exchangeName, // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	sigtermChannel := make(chan os.Signal, 1)
	signal.Notify(sigtermChannel, syscall.SIGTERM)
	return &Consumer{
		conn:           conn,
		ch:             ch,
		msgChannel:     msgs,
		eofsReceived:   0,
		config:         config,
		sigtermChannel: sigtermChannel,
	}, nil
}

func (c *Consumer) Consume(processMessage func(message.Message)) {
	for {
		select {
		case <-c.sigtermChannel:
			return
		case delivery := <-c.msgChannel:
			msg := message.Deserialize(string(delivery.Body))
			if msg.IsEOF() {
				fmt.Printf("Received eof %v\n", c.eofsReceived)
				c.eofsReceived++
				if c.eofsReceived == c.config.previousStageInstances {
					fmt.Println("Received all eofs")
					processMessage(msg)
					return
				}
				continue
			}
			processMessage(msg)
			delivery.Ack(false)
		}

	}
}

func (c *Consumer) Close() {
	c.ch.Close()
	c.conn.Close()
}
