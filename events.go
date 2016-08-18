package susigo

// Event structure
type Event struct {
	Topic     string              `json:"topic"`
	Payload   interface{}         `json:"payload"`
	Headers   []map[string]string `json:"headers"`
	ID        string              `json:"id"`
	SessionID string              `json:"sessionid"`
	susi      *Susi
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

// Publish send a acknoledge for the event
func (event *Event) Publish(cb func(*Event)) error {
	return event.susi.Publish(*event, cb)
}

// Dismiss a event
func (event *Event) Dismiss() {
	event.AddHeader("Event-Control", "No-Processor")
	event.AddHeader("Event-Control", "No-Consumer")
	event.susi.Ack(event)
}
