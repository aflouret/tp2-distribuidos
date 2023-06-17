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
	eofsReceived   map[string]int
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
	routingKey := v.GetString(fmt.Sprintf("%s.routing_key", configID))
	previousStageInstancesEnv := v.GetString(fmt.Sprintf("%s.prev_stage_instances_env", configID))
	previousStageInstances, err := strconv.Atoi(os.Getenv(previousStageInstancesEnv))
	if err != nil {
		previousStageInstances = 1
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

	queueName := config.exchangeName + config.instanceID
	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		true,      // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	if config.routingKey != "" {
		err = ch.QueueBind(
			q.Name,              // queue name
			config.routingKey,   // routing key
			config.exchangeName, // exchange
			false,
			nil)
		failOnError(err, "Failed to bind a queue")
	}

	err = ch.QueueBind(
		q.Name,              // queue name
		config.instanceID,   // routing key
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

	eofsReceived := map[string]int{
		message.TripsEOF:    0,
		message.StationsEOF: 0,
		message.WeatherEOF:  0,
	}
	return &Consumer{
		conn:           conn,
		ch:             ch,
		msgChannel:     msgs,
		eofsReceived:   eofsReceived,
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
				c.eofsReceived[msg.MsgType] += 1
				fmt.Printf("Received %s %v of %v \n", msg.MsgType, c.eofsReceived[msg.MsgType], c.config.previousStageInstances)

				if c.eofsReceived[msg.MsgType] == c.config.previousStageInstances {
					fmt.Printf("Received all %s eofs\n", msg.MsgType)
					processMessage(msg)
				}
				delivery.Ack(false)
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
