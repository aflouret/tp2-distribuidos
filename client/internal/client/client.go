package client

import (
	"bufio"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"tp1/client/internal/utils"
	"tp1/common/protocol"
)

var cities = []string{"montreal", "toronto", "washington"}
var ErrClientTerminated = errors.New("client terminated")

type Config struct {
	ServerAddress string
	TripsFile     string
	BatchSize     int
}

type Client struct {
	config          Config
	conn            net.Conn
	startTime       time.Time
	sigtermNotifier chan os.Signal
}

func NewClient(config Config) *Client {
	sigtermNotifier := make(chan os.Signal, 1)
	signal.Notify(sigtermNotifier, syscall.SIGTERM)
	client := &Client{
		config:          config,
		sigtermNotifier: sigtermNotifier,
	}
	return client
}

func (c *Client) connectToServer() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) StartClient() {
	c.startTime = time.Now()
	err := c.connectToServer()
	if err != nil {
		log.Errorf("action: connect | result: fail | error: %v", err)
		return
	}
	err = c.sendStationsToServer()
	if err != nil {
		log.Errorf("action: send_stations | result: fail | error: %v", err)
		return
	}
	err = c.sendWeatherToServer()
	if err != nil {
		log.Errorf("action: send_weather | result: fail | error: %v", err)
		return
	}
	err = c.sendTripsToServer()
	if err != nil {
		log.Errorf("action: send_trips | result: fail | error: %v", err)
		return
	}
	err = c.getResults()
	if err != nil {
		log.Errorf("action: request_results | result: fail | error: %v", err)
		return
	}
	err = c.conn.Close()
	if err != nil {
		log.Errorf("action: request_results | result: fail | error: %v", err)
		return
	}
	log.Infof("action: exit_client | result: success | time: %s", time.Since(c.startTime).String())
}

func (c *Client) sendStationsToServer() error {

	for _, city := range cities {
		file, err := os.Open(fmt.Sprintf("data/%s/stations.csv", city))
		if err != nil {
			log.Panic(err)
		}

		err = c.notifyBeginStations(city)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(file)
		scanner.Scan()
		for {
			stations, err := utils.ReadBatch(scanner, c.config.BatchSize)
			if err != nil {
				return err
			}
			if len(stations) == 0 {
				err = c.notifyEndStations(city)
				if err != nil {
					return err
				}
				break
			}

			if err = c.sendDataMessage(stations); err != nil {
				return err
			}
			select {
			case <-c.sigtermNotifier:
				log.Debugf("action: terminate_client | result: success")
				return ErrClientTerminated
			default:
			}
		}
		err = file.Close()
		if err != nil {
			log.Panic(err)
		}
	}

	return nil
}

func (c *Client) sendWeatherToServer() error {
	for _, city := range cities {
		file, err := os.Open(fmt.Sprintf("data/%s/weather.csv", city))
		if err != nil {
			log.Panic(err)
		}

		err = c.notifyBeginWeather(city)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(file)
		scanner.Scan()
		for {
			weather, err := utils.ReadBatch(scanner, c.config.BatchSize)
			if err != nil {
				return err
			}
			if len(weather) == 0 {
				err = c.notifyEndWeather(city)
				if err != nil {
					return err
				}
				break
			}

			if err = c.sendDataMessage(weather); err != nil {
				return err
			}
			select {
			case <-c.sigtermNotifier:
				log.Debugf("action: terminate_client | result: success ")
				return ErrClientTerminated
			default:
			}
		}
		err = file.Close()
		if err != nil {
			log.Panic(err)
		}
	}

	return nil
}

func (c *Client) sendTripsToServer() error {
	if err := c.notifyEndStaticData(); err != nil {
		return err
	}

	for _, city := range cities {
		file, err := os.Open(fmt.Sprintf("data/%s/%s", city, c.config.TripsFile))
		if err != nil {
			log.Panic(err)
		}

		err = c.notifyBeginTrips(city)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(file)
		scanner.Scan()
		for {
			trips, err := utils.ReadBatch(scanner, c.config.BatchSize)
			if err != nil {
				return err
			}
			if len(trips) == 0 {
				err = c.notifyEndTrips(city)
				if err != nil {
					return err
				}
				break
			}

			if err = c.sendDataMessage(trips); err != nil {
				return err
			}

			select {
			case <-c.sigtermNotifier:
				log.Debugf("action: terminate_client | result: success")
				return ErrClientTerminated
			default:
			}
		}
		err = file.Close()
		if err != nil {
			log.Panic(err)
		}
	}

	return nil
}

func (c *Client) getResults() error {
	err := c.sendResultsRequest()
	if err != nil {
		return err
	}

	result1, err := c.getResult()
	if err != nil {
		return err
	}
	fmt.Println(result1)

	result2, err := c.getResult()
	if err != nil {
		return err
	}
	fmt.Println(result2)

	result3, err := c.getResult()
	if err != nil {
		return err
	}
	fmt.Println(result3)

	logResults([]string{result1, result2, result3})
	return nil
}

func (c *Client) notifyBeginStations(city string) error {
	return utils.SendControlMessage(c.conn, protocol.BeginStations, city)
}

func (c *Client) notifyEndStations(city string) error {
	return utils.SendControlMessage(c.conn, protocol.EndStations, city)
}

func (c *Client) notifyBeginWeather(city string) error {
	return utils.SendControlMessage(c.conn, protocol.BeginWeather, city)
}

func (c *Client) notifyEndWeather(city string) error {
	return utils.SendControlMessage(c.conn, protocol.EndWeather, city)
}

func (c *Client) notifyBeginTrips(city string) error {
	return utils.SendControlMessage(c.conn, protocol.BeginTrips, city)
}

func (c *Client) notifyEndTrips(city string) error {
	return utils.SendControlMessage(c.conn, protocol.EndTrips, city)
}

func (c *Client) notifyEndStaticData() error {
	return utils.SendControlMessage(c.conn, protocol.EndStaticData, "")
}

func (c *Client) sendResultsRequest() error {
	return utils.SendControlMessage(c.conn, protocol.GetResults, "")
}

func (c *Client) getResult() (string, error) {
	msg, err := protocol.Recv(c.conn)
	if err != nil {
		return "", err
	}
	return msg.Payload, nil
}

func (c *Client) sendDataMessage(payload string) error {
	return utils.SendDataMessage(c.conn, payload)
}

func logResults(results []string) {
	for _, r := range results {
		lines := strings.Split(r, "\n")
		header := strings.TrimSpace(lines[0])
		switch header {
		case "Average duration of trips during >30mm precipitation days:":
			f, _ := os.OpenFile("recovery_files/result_1.txt", os.O_CREATE|os.O_WRONLY, 0666)
			res := strings.Join(lines[2:], "\n")
			f.Write([]byte(res))
			f.Close()
		case "Stations that doubled the number of trips between 2016 and 2017:":
			f, _ := os.OpenFile("recovery_files/result_2.txt", os.O_CREATE|os.O_WRONLY, 0666)
			res := strings.Join(lines[2:], "\n")
			f.Write([]byte(res))
			f.Close()
		case "Stations with more than 6 km average to arrive at them:":
			f, _ := os.OpenFile("recovery_files/result_3.txt", os.O_CREATE|os.O_WRONLY, 0666)
			res := strings.Join(lines[2:], "\n")
			f.Write([]byte(res))
			f.Close()
		}
	}
}
