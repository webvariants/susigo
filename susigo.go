package susigo

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"log"
	"net"
	"regexp"
	"strconv"
	"time"
)

type Callback func(*Event)

type Susi struct {
	cert             tls.Certificate
	addr             string
	connected        bool
	conn             net.Conn
	encoder          *json.Encoder
	decoder          *json.Decoder
	callbacks        map[string]Callback
	consumers        map[string]map[int64]Callback
	processors       map[string]map[int64]Callback
	publishProcesses map[string][]Callback
}

type Event struct {
	Topic     string              `json:"topic"`
	Payload   interface{}         `json:"payload"`
	Headers   []map[string]string `json:"headers"`
	ID        string              `json:"id"`
	SessionID string              `json:"sessionid"`
}

type Message struct {
	Type string `json:"type"`
	Data *Event `json:"data"`
}

func NewSusi(addr, certFile, keyFile string) (*Susi, error) {
	susi := new(Susi)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	susi.cert = cert
	susi.callbacks = make(map[string]Callback)
	susi.addr = addr
	susi.consumers = make(map[string]map[int64]Callback)
	susi.processors = make(map[string]map[int64]Callback)
	susi.publishProcesses = make(map[string][]Callback)
	susi.connected = false
	go susi.backend()
	return susi, nil
}

func (susi *Susi) Publish(event Event, callback Callback) error {
	if !susi.connected {
		return errors.New("susi not connected")
	}
	var id = event.ID
	if event.ID == "" {
		id = strconv.FormatInt(time.Now().UnixNano(), 10)
		event.ID = id
	}
	event.ID = id
	packet := map[string]interface{}{
		"type": "publish",
		"data": event,
	}
	susi.callbacks[id] = callback
	return susi.encoder.Encode(packet)
}

func (susi *Susi) RegisterConsumer(topic string, callback Callback) (int64, error) {
	if susi.consumers[topic] == nil {
		susi.consumers[topic] = make(map[int64]Callback)
	}
	consumers := susi.consumers[topic]
	id := time.Now().UnixNano()
	consumers[id] = callback
	if len(consumers) == 1 {
		packet := map[string]interface{}{
			"type": "registerConsumer",
			"data": map[string]interface{}{
				"topic": topic,
			},
		}
		if susi.connected {
			return id, susi.encoder.Encode(packet)
		}
		return id, nil
	}
	return -1, nil
}

func (susi *Susi) RegisterProcessor(topic string, callback Callback) (int64, error) {
	if susi.processors[topic] == nil {
		susi.processors[topic] = make(map[int64]Callback)
	}
	processors := susi.processors[topic]
	id := time.Now().UnixNano()
	processors[id] = callback
	if len(processors) == 1 {
		packet := map[string]interface{}{
			"type": "registerProcessor",
			"data": map[string]interface{}{
				"topic": topic,
			},
		}
		if susi.connected {
			return id, susi.encoder.Encode(packet)
		}
		return id, nil
	}
	return -1, nil
}

func (susi *Susi) UnregisterConsumer(id int64) error {
	for _, consumers := range susi.consumers {
		if topic, ok := consumers[id]; ok {
			delete(consumers, id)
			if len(consumers) == 0 {
				packet := map[string]interface{}{
					"type": "unregisterConsumer",
					"data": map[string]interface{}{
						"topic": topic,
					},
				}
				if susi.connected {
					return susi.encoder.Encode(packet)
				}
				return nil
			}
			return nil
		}
	}
	return errors.New("no such consumer")
}

func (susi *Susi) UnregisterProcessor(id int64) error {
	for _, processors := range susi.processors {
		if topic, ok := processors[id]; ok {
			delete(processors, id)
			if len(processors) == 0 {
				packet := map[string]interface{}{
					"type": "unregisterProcessor",
					"data": map[string]interface{}{
						"topic": topic,
					},
				}
				if susi.connected {
					return susi.encoder.Encode(packet)
				}
				return nil
			}
			return nil
		}
	}
	return errors.New("no such processor")
}

func (susi *Susi) connect() error {
	conn, err := tls.Dial("tcp", susi.addr, &tls.Config{
		Certificates:       []tls.Certificate{susi.cert},
		InsecureSkipVerify: true,
	})
	if err != nil {
		log.Printf("failed connecting susi-core (%v), retry...", err)
		return err
	}
	susi.conn = conn
	susi.connected = true
	susi.encoder = json.NewEncoder(susi.conn)
	susi.decoder = json.NewDecoder(susi.conn)
	for consumerTopic := range susi.consumers {
		packet := map[string]interface{}{
			"type": "registerConsumer",
			"data": map[string]interface{}{
				"topic": consumerTopic,
			},
		}
		susi.encoder.Encode(packet)
	}
	for processorTopic := range susi.processors {
		packet := map[string]interface{}{
			"type": "registerProcessor",
			"data": map[string]interface{}{
				"topic": processorTopic,
			},
		}
		susi.encoder.Encode(packet)
	}
	return nil
}

func (susi *Susi) backend() {
	packet := Message{}
	for {
		if !susi.connected {
			err := susi.connect()
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
		}
		err := susi.decoder.Decode(&packet)
		if err != nil {
			log.Println("Error reading from susi json decoder: ", err)
			susi.connected = false
			continue
		}
		switch packet.Type {
		case "ack":
			{
				event := packet.Data
				id := event.ID
				callback := susi.callbacks[id]
				callback(event)
				delete(susi.callbacks, id)
			}
		case "consumerEvent":
			{
				event := packet.Data
				topic := event.Topic
				var matchingConsumers []Callback
				for pattern, consumers := range susi.consumers {
					if matched, err := regexp.MatchString(pattern, topic); err == nil && matched {
						for _, consumer := range consumers {
							matchingConsumers = append(matchingConsumers, consumer)
						}
					}
				}
				for _, consumer := range matchingConsumers {
					consumer(event)
				}
			}
		case "processorEvent":
			{
				event := packet.Data
				topic := event.Topic
				var matchingProcessors []Callback
				for pattern, processors := range susi.processors {
					if matched, err := regexp.MatchString(pattern, topic); err == nil && matched {
						for _, processor := range processors {
							matchingProcessors = append(matchingProcessors, processor)
						}
					}
				}
				susi.publishProcesses[event.ID] = matchingProcessors
				susi.Ack(event)
			}
		}
	}
}

func (susi *Susi) Ack(event *Event) error {
	if process, ok := susi.publishProcesses[event.ID]; ok {
		if len(process) == 0 {
			packet := map[string]interface{}{
				"type": "ack",
				"data": event,
			}
			delete(susi.publishProcesses, event.ID)
			if susi.connected {
				return susi.encoder.Encode(packet)
			}
			return nil
		}
		cb := process[0]
		process = process[1:]
		susi.publishProcesses[event.ID] = process
		cb(event)
		return nil
	}
	return errors.New("no publish process found")
}

func (susi *Susi) Dismiss(event *Event) error {
	if _, ok := susi.publishProcesses[event.ID]; ok {
		packet := map[string]interface{}{
			"type": "dismiss",
			"data": event,
		}
		delete(susi.publishProcesses, event.ID)
		if susi.connected {
			return susi.encoder.Encode(packet)
		}
		return nil
	}
	return errors.New("no publish process found")
}
