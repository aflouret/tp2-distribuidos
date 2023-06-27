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
	eofsReceived   map[string]map[string]map[string]bool
	msgIDsReceived map[string]map[string]bool
	config         ConsumerConfig
	sigtermChannel chan os.Signal
	queueName      string
}

type ConsumerConfig struct {
	connectionString       string
	exchangeName           string
	instanceID             string
	previousStageInstances int
	messageTypesToStore    []string
	filterDuplicates       bool
	autoDelete             bool
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
	filterDuplicates := v.GetBool(fmt.Sprintf("%s.filter_duplicate_batches", configID))
	autoDelete := v.GetBool(fmt.Sprintf("%s.auto_delete", configID))
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
		filterDuplicates:       filterDuplicates,
		autoDelete:             autoDelete,
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
		false,               // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	fmt.Println("Exchange declared")

	queueName := config.exchangeName + "_" + routingKey + "_" + config.instanceID
	q, err := ch.QueueDeclare(
		queueName,         // name
		false,             // durable
		config.autoDelete, // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	failOnError(err, "Failed to declare a queue")

	fmt.Printf("Queue declared, autoDelete=%v\n", config.autoDelete)

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
		10000, // prefetch count
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

	eofsReceived := make(map[string]map[string]map[string]bool)
	msgIDsReceived := make(map[string]map[string]bool)
	return &Consumer{
		conn:           conn,
		ch:             ch,
		msgChannel:     msgs,
		eofsReceived:   eofsReceived,
		msgIDsReceived: msgIDsReceived,
		config:         config,
		sigtermChannel: sigtermChannel,
		queueName:      queueName,
	}, nil
}

func (c *Consumer) Consume(processMessage func(message.Message)) {
	if !c.isResultsConsumer() {
		fmt.Println("Recovering state")
		recovery.Recover("recovery_files", func(msg message.Message) {
			if msg.IsEOF() {
				c.processEOF(msg, processMessage)
			} else {
				c.processBatchMessage(msg, processMessage)
			}
		})
		fmt.Println("Finished recovering")
	}

	storageManager, err := recovery.NewStorageManager("recovery_files")
	failOnError(err, "Failed to create storage manager")
	for {
		select {
		case <-c.sigtermChannel:
			return
		case delivery := <-c.msgChannel:
			// Deserialize message
			msg := message.Deserialize(string(delivery.Body))

			// Process message and check for duplicates
			var isDuplicateMessage bool
			if msg.IsEOF() {
				isDuplicateMessage = c.processEOF(msg, processMessage)
			} else {
				isDuplicateMessage = c.processBatchMessage(msg, processMessage)
			}

			// Store message if necessary
			if c.shouldStore(msg) && !isDuplicateMessage {
				err := storageManager.Store(msg)
				failOnError(err, fmt.Sprintf("error storing message %v", msg))
			}

			// ACK message
			delivery.Ack(false)

			// Return if it is the last Result EOF
			if c.isResultsConsumer() && c.receivedAllEOFs(msg.ClientID, msg.MsgType) {
				return
			}

			if msg.MsgType == message.ClientEOF && c.receivedAllEOFs(msg.ClientID, msg.MsgType) {
				// Delete files and maps allocated for client
				storageManager.Delete(msg.ClientID)
				c.deleteResources(msg.ClientID)
			}
		}
	}
}

func (c *Consumer) Close() {
	c.ch.Close()
	c.conn.Close()
}

func (c *Consumer) processEOF(msg message.Message, processMessage func(message.Message)) (isDuplicateMessage bool) {
	// Allocate map for client and type if it does not exist
	if _, ok := c.eofsReceived[msg.ClientID]; !ok {
		c.eofsReceived[msg.ClientID] = make(map[string]map[string]bool)
	}
	if _, ok := c.eofsReceived[msg.ClientID][msg.MsgType]; !ok {
		c.eofsReceived[msg.ClientID][msg.MsgType] = make(map[string]bool)
	}

	// If the message was already received, do nothing
	if received := c.eofsReceived[msg.ClientID][msg.MsgType][msg.ID]; received {
		fmt.Printf("[Client %s] Received duplicate %v EOF: %v\n", msg.ClientID, msg.MsgType, msg.ID)
		return true
	}

	// Register EOF in map
	c.eofsReceived[msg.ClientID][msg.MsgType][msg.ID] = true
	fmt.Printf("[Client %s] Received %s %v of %v \n", msg.ClientID, msg.MsgType, len(c.eofsReceived[msg.ClientID][msg.MsgType]), c.config.previousStageInstances)

	// If it is the last EOF then process it
	if c.receivedAllEOFs(msg.ClientID, msg.MsgType) {
		fmt.Printf("[Client %s] Received all %s eofs\n", msg.ClientID, msg.MsgType)
		processMessage(msg)
	}
	return false
}

func (c *Consumer) processBatchMessage(msg message.Message, processMessage func(message.Message)) (isDuplicateMessage bool) {
	if c.shouldFilterDuplicates() {
		// Allocate map for client if it does not exist
		if _, ok := c.msgIDsReceived[msg.ClientID]; !ok {
			c.msgIDsReceived[msg.ClientID] = make(map[string]bool)
		}

		// If the message was already received, do nothing
		if received := c.msgIDsReceived[msg.ClientID][msg.ID]; received {
			fmt.Printf("[Client %s] Received duplicate message: %v\n", msg.ClientID, msg.ID)
			return true
		}

		// Register and process message
		c.msgIDsReceived[msg.ClientID][msg.ID] = true
	}
	processMessage(msg)
	return false
}

func (c *Consumer) receivedAllEOFs(clientID string, msgType string) bool {
	return len(c.eofsReceived[clientID][msgType]) == c.config.previousStageInstances
}

func (c *Consumer) shouldStore(msg message.Message) bool {
	for _, t := range c.config.messageTypesToStore {
		if t == msg.MsgType {
			return true
		}
	}
	return false
}

func (c *Consumer) shouldFilterDuplicates() bool {
	return c.config.filterDuplicates
}

func (c *Consumer) isResultsConsumer() bool {
	for _, t := range c.config.messageTypesToStore {
		if t == message.ResultsBatch {
			return true
		}
	}
	return false
}

func (c *Consumer) deleteResources(clientID string) {
	if clientID == message.AllClients {
		c.eofsReceived = make(map[string]map[string]map[string]bool)
		c.msgIDsReceived = make(map[string]map[string]bool)
	} else {
		delete(c.eofsReceived, clientID)
		delete(c.msgIDsReceived, clientID)
	}
}
