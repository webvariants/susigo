package susigo

import (
	"errors"
	"strconv"
	"time"
)

// Event structure
type Event struct {
	Topic     string              `json:"topic"`
	Payload   interface{}         `json:"payload"`
	Headers   []map[string]string `json:"headers"`
	ID        string              `json:"id"`
	SessionID string              `json:"sessionid"`
	susi      *Susi
}

// CreateEvent returns a new event structure with susi reference
func (susi *Susi) CreateEvent(topic string) Event {
	event := Event{
		susi:  susi,
		Topic: topic,
	}
	return event
}

// SetTopic set the topic of the event
func (event *Event) SetTopic(val string) {
	event.Topic = val
}

// AddHeader add a key: value pair to the header
func (event *Event) AddHeader(key, val string) {
	event.Headers = append(event.Headers, map[string]string{key: val})
}

// SetPayload set the payload of the event
func (event *Event) SetPayload(data interface{}) {
	event.Payload = data
}

// Ack send a acknoledge for the event
func (event *Event) Ack() {
	event.susi.Ack(event)
}

// Dismiss a event
func (event *Event) Dismiss() {
	// event.Headers = event.Headers[:0]
	event.AddHeader("Event-Control", "No-Processor")
	event.susi.Dismiss(event)
}

// RegisterConsumer to the core
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

// RegisterProcessor to the core
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

// UnregisterConsumer to the core
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

// UnregisterProcessor to the core
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

// Publish a new event
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

// Ack -nolege information for the client
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

// Dismiss information for the client
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
