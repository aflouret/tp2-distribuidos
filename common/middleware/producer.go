package middleware

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"
	"os"
	"strconv"
	"strings"
	"tp1/common/message"
)

type Producer struct {
	conn   *amqp.Connection
	ch     *amqp.Channel
	config ProducerConfig
}

type ProducerConfig struct {
	connectionString   string
	exchangeName       string
	nextStageInstances int
	routeByID          bool
	instanceID         string
}

func newProducerConfig(configID string) (ProducerConfig, error) {
	v := viper.New()
	v.SetConfigFile("./middleware_config.yaml")
	if err := v.ReadInConfig(); err != nil {
		return ProducerConfig{}, fmt.Errorf("Configuration for producer %s could not be read from config file\n", configID)
	}
	exchangeName := v.GetString(fmt.Sprintf("%s.exchange_name", configID))
	routeByID := v.GetBool(fmt.Sprintf("%s.route_by_id", configID))
	nextStageInstancesEnv := v.GetString(fmt.Sprintf("%s.next_stage_instances_env", configID))
	nextStageInstances, err := strconv.Atoi(os.Getenv(nextStageInstancesEnv))
	if err != nil {
		nextStageInstances = 1
	}
	connectionString := os.Getenv("RABBITMQ_CONNECTION_STRING")
	instanceID := os.Getenv("ID")
	return ProducerConfig{
		connectionString:   connectionString,
		exchangeName:       exchangeName,
		nextStageInstances: nextStageInstances,
		routeByID:          routeByID,
		instanceID:         instanceID,
	}, nil
}

func NewProducer(configID string) (*Producer, error) {
	config, err := newProducerConfig(configID)
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
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	return &Producer{
		conn:   conn,
		ch:     ch,
		config: config,
	}, nil
}

func (p *Producer) PublishMessage(msg message.Message, routingKey string) error {
	var err error
	if routingKey == "" {
		routingKey, err = p.getRoutingKey(msg)
		if err != nil {
			return fmt.Errorf("error getting routing key for message %v: %w", msg, err)
		}
	}
	if msg.IsEOF() {
		msg.ID = p.config.instanceID
	}

	fmt.Printf("Publishing with routing key: %s, message: %s\n", routingKey, msg)
	err = p.ch.PublishWithContext(context.TODO(),
		p.config.exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(message.Serialize(msg)),
		},
	)
	if err != nil {
		return fmt.Errorf("error publishing message %v: %w", msg, err)
	}
	fmt.Printf("Finishe publishing with routing key: %s, message: %s\n", routingKey, msg)
	return nil
}

func (p *Producer) Close() error {
	err := p.ch.Close()
	if err != nil {
		return err
	}
	err = p.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

func (p *Producer) getRoutingKey(msg message.Message) (string, error) {
	var routingKey string
	if msg.IsEOF() {
		routingKey = "eof"
	} else if p.config.routeByID {
		var msgID int
		var err error
		if strings.Contains(msg.ID, ".") {
			splitID := strings.Split(msg.ID, ".")
			msgID, err = strconv.Atoi(splitID[len(splitID)-1])
			if err != nil {
				return "", err
			}
		} else {
			msgID, err = strconv.Atoi(msg.ID)
			if err != nil {
				return "", err
			}
		}
		consumerID := msgID % p.config.nextStageInstances
		routingKey = fmt.Sprintf("%v", consumerID)
	}
	return routingKey, nil
}
