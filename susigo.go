package susigo

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"net"
	"regexp"
	"time"
)

// Callback function
type Callback func(*Event)

// Susi structure
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

// Message structure
type Message struct {
	Type string `json:"type"`
	Data *Event `json:"data"`
}

// NewSusi creates a new insance
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

// connect susi to the core
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

// backend to manage the events
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
