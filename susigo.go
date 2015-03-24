package susigo

import (
	"encoding/json"
	"log"
	"net"
	"strconv"
	"time"
)

type Callback func(map[string]interface{})

type Susi struct {
	conn       net.Conn
	encoder    *json.Encoder
	decoder    *json.Decoder
	callbacks  map[string]Callback
	consumers  map[string][]Callback
	processors map[string][]Callback
}

func NewSusi(addr string) (*Susi, error) {
	susi := new(Susi)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	susi.conn = conn
	susi.callbacks = make(map[string]Callback)
	susi.consumers = make(map[string][]Callback)
	susi.processors = make(map[string][]Callback)
	susi.encoder = json.NewEncoder(susi.conn)
	susi.decoder = json.NewDecoder(susi.conn)
	go susi.backend()
	return susi, nil
}

func (susi *Susi) Publish(topic string, payload interface{}, callback Callback) error {
	id := strconv.FormatInt(time.Now().UnixNano(), 10)
	packet := map[string]interface{}{
		"type": "publish",
		"data": map[string]interface{}{
			"topic":   topic,
			"payload": payload,
			"id":      id,
		},
	}
	susi.callbacks[id] = callback
	return susi.encoder.Encode(packet)
}

func (susi *Susi) RegisterConsumer(topic string, callback Callback) error {
	susi.consumers[topic] = append(susi.consumers[topic], callback)
	if len(susi.consumers[topic]) == 1 {
		packet := map[string]interface{}{
			"type": "registerConsumer",
			"data": map[string]interface{}{
				"topic": topic,
			},
		}
		return susi.encoder.Encode(packet)
	}
	return nil
}

func (susi *Susi) RegisterProcessor(topic string, callback Callback) error {
	susi.processors[topic] = append(susi.processors[topic], callback)
	if len(susi.processors[topic]) == 1 {
		packet := map[string]interface{}{
			"type": "registerProcessor",
			"data": map[string]interface{}{
				"topic": topic,
			},
		}
		return susi.encoder.Encode(packet)
	}
	return nil
}

func (susi *Susi) backend() {
	packet := make(map[string]interface{})
	for {
		err := susi.decoder.Decode(&packet)
		if err != nil {
			log.Println("Error reading from susi json decoder: ", err)
			continue
		}
		switch packet["type"] {
		case "ack":
			{
				event := packet["data"].(map[string]interface{})
				id := event["id"].(string)
				callback := susi.callbacks[id]
				callback(event)
				delete(susi.callbacks, id)
			}
		case "consumerEvent":
			{
				event := packet["data"].(map[string]interface{})
				topic := event["topic"].(string)
				consumers := susi.consumers[topic]
				for _, consumer := range consumers {
					consumer(event)
				}
			}
		case "processorEvent":
			{
				event := packet["data"].(map[string]interface{})
				topic := event["topic"].(string)
				processors := susi.processors[topic]
				for _, processor := range processors {
					processor(event)
				}
				packet["type"] = "ack"
				packet["data"] = event
				if err = susi.encoder.Encode(packet); err != nil {
					log.Println("can not ack event back to susi: ", err)
				}
			}
		}
	}
}
