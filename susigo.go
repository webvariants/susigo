package susigo

import (
    "crypto/tls"
    "encoding/json"
    "errors"
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
    consumers  map[string]map[int64]Callback
    processors map[string]map[int64]Callback
}

func NewSusi(addr, certFile, keyFile string) (*Susi, error) {
    susi := new(Susi)
    cert, err := tls.LoadX509KeyPair(certFile, keyFile)
    if err != nil {
        return nil, err
    }
    conn, err := tls.Dial("tcp", addr, &tls.Config{
        Certificates:       []tls.Certificate{cert},
        InsecureSkipVerify: true,
    })
    if err != nil {
        return nil, err
    }
    susi.conn = conn
    susi.callbacks = make(map[string]Callback)
    susi.consumers = make(map[string]map[int64]Callback)
    susi.processors = make(map[string]map[int64]Callback)
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
        return id, susi.encoder.Encode(packet)
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
        return id, susi.encoder.Encode(packet)
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
                return susi.encoder.Encode(packet)
            } else {
                return nil
            }
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
                return susi.encoder.Encode(packet)
            } else {
                return nil
            }
        }
    }
    return errors.New("no such processor")
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
