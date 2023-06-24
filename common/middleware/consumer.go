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
	"tp1/common/recovery"
)

type Consumer struct {
	conn           *amqp.Connection
	ch             *amqp.Channel
	msgChannel     <-chan amqp.Delivery
	eofsReceived   map[string]map[string]int
	config         ConsumerConfig
	sigtermChannel chan os.Signal
}

type ConsumerConfig struct {
	connectionString       string
	exchangeName           string
	instanceID             string
	previousStageInstances int
	messageTypesToStore    []string
}

func newConsumerConfig(configID string) (ConsumerConfig, error) {
	v := viper.New()
	v.SetConfigFile("./middleware_config.yaml")
	if err := v.ReadInConfig(); err != nil {
		return ConsumerConfig{}, fmt.Errorf("Configuration for consumer %s could not be read from config file: %w\n", configID, err)
	}
	exchangeName := v.GetString(fmt.Sprintf("%s.exchange_name", configID))
	previousStageInstancesEnv := v.GetString(fmt.Sprintf("%s.prev_stage_instances_env", configID))
	messageTypesToStore := v.GetStringSlice(fmt.Sprintf("%s.messages_to_store", configID))
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
		messageTypesToStore:    messageTypesToStore,
	}, nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func NewConsumer(configID string, routingKey string) (*Consumer, error) {
	config, err := newConsumerConfig(configID)
	if err != nil {
		return nil, err
	}
	fmt.Println("Connecting to RabbitMQ")

	conn, err := amqp.Dial(config.connectionString)
	failOnError(err, "Failed to connect to RabbitMQ")

	fmt.Println("Connected to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	fmt.Println("Channel opened")

	err = ch.ExchangeDeclare(
		config.exchangeName, // name
		"direct",            // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	fmt.Println("Exchange declared")

	queueName := config.exchangeName + "_" + routingKey + "_" + config.instanceID
	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		amqp.Table{ // queue args
			amqp.QueueMaxLenBytesArg: int64(5_000_000_000), // 5 Gb
			amqp.QueueMaxLenArg:      100000,
		}, // arguments
	)
	failOnError(err, "Failed to declare a queue")

	fmt.Println("Queue declared")

	if routingKey != "" {
		err = ch.QueueBind(
			q.Name,              // queue name
			routingKey,          // routing key
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

	fmt.Println("Queue bound")

	err = ch.Qos(
		1000,  // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	fmt.Println("QOS set")

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

	fmt.Println("Consumer registered")

	sigtermChannel := make(chan os.Signal, 1)
	signal.Notify(sigtermChannel, syscall.SIGTERM)

	eofsReceived := make(map[string]map[string]int)
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
	fmt.Println("Recovering state")
	recovery.Recover("recovery_files", func(msg message.Message) {
		if msg.IsEOF() {
			c.registerEOF(msg)
			if c.isLastEOF(msg) {
				processMessage(msg)
			}
		} else {
			processMessage(msg)
		}
	})
	fmt.Println("Finished recovering")
	//storageManager, err := recovery.NewStorageManager("recovery_files")
	//failOnError(err, "Failed to create storage manager")
	for {
		select {
		case <-c.sigtermChannel:
			return
		case delivery := <-c.msgChannel:
			msg := message.Deserialize(string(delivery.Body))
			if msg.IsEOF() {
				// Register EOF in map
				c.registerEOF(msg)
				fmt.Printf("[Client %s] Received %s %v of %v \n", msg.ClientID, msg.MsgType, c.eofsReceived[msg.ClientID][msg.MsgType], c.config.previousStageInstances)

				// If it is the last EOF then process it
				if c.isLastEOF(msg) {
					fmt.Printf("[Client %s] Received all %s eofs\n", msg.ClientID, msg.MsgType)
					processMessage(msg)
				}

				// Store message if necessary
				if c.shouldStore(msg) {
					//err := storageManager.Store(msg)
					//failOnError(err, fmt.Sprintf("error storing message %v", msg))
				}

				// ACK message
				delivery.Ack(false)

				// Return if it is the last results EOF
				if c.isLastEOF(msg) && msg.MsgType == message.ResultsEOF {
					return
				}
			} else {
				// Process message
				processMessage(msg)

				// Store message if necessary
				if c.shouldStore(msg) {
					//err := storageManager.Store(msg)
					//failOnError(err, fmt.Sprintf("error storing message %v", msg))
				}

				// ACK message
				delivery.Ack(false)
			}
		}
	}
}

func (c *Consumer) Close() {
	c.ch.Close()
	c.conn.Close()
}

func (c *Consumer) registerEOF(msg message.Message) {
	if _, ok := c.eofsReceived[msg.ClientID]; !ok {
		c.eofsReceived[msg.ClientID] = make(map[string]int)
		c.eofsReceived[msg.ClientID][msg.MsgType] = 1
	} else {
		c.eofsReceived[msg.ClientID][msg.MsgType] += 1
	}
}

func (c *Consumer) isLastEOF(msg message.Message) bool {
	return c.eofsReceived[msg.ClientID][msg.MsgType] == c.config.previousStageInstances
}

func (c *Consumer) shouldStore(msg message.Message) bool {
	for _, t := range c.config.messageTypesToStore {
		if t == msg.MsgType {
			return true
		}
	}
	return false
}
