package internal

import (
	"time"

	"github.com/GirishBhutiya/mqtt-kafka-bridge/pkg/kafka/shared"
	"github.com/goccy/go-json"
)

func AddSHeader(msg *shared.KafkaMessage, key string, value string) {
	if msg.Headers == nil {
		msg.Headers = make(map[string]string)
	}
	msg.Headers[key] = value
}

func GetSHeader(msg *shared.KafkaMessage, key string) (bool, string) {
	if msg.Headers == nil {
		return false, ""
	}
	if value, ok := msg.Headers[key]; ok {
		return true, string(value)
	}
	return false, ""
}

func IsSameOrigin(msg *shared.KafkaMessage) bool {
	ok, origin := GetSXOrigin(msg)
	if !ok {
		return false
	}
	return origin == SerialNumber
}
func GetSXOrigin(msg *shared.KafkaMessage) (bool, string) {
	return GetSHeader(msg, "x-origin")
}

func AddSXOrigin(msg *shared.KafkaMessage) {
	AddSHeader(msg, "x-origin", SerialNumber)
}

func IsInTrace(msg *shared.KafkaMessage) bool {
	identifier := MicroserviceName + "-" + SerialNumber

	ok, trace := GetSXTrace(msg)
	if !ok {
		return false
	}
	for _, s := range trace.Traces {
		if s == identifier {
			return true
		}
	}
	return false
}

func GetSXTrace(msg *shared.KafkaMessage) (bool, TraceValue) {
	ok, traceS := GetSHeader(msg, "x-trace")
	var traceValue TraceValue
	if !ok {
		return false, traceValue
	} else {
		err := json.Unmarshal([]byte(traceS), traceValue)
		if err != nil {
			return false, traceValue
		}
	}
	return true, traceValue
}

func AddSXTrace(msg *shared.KafkaMessage) error {
	identifier := MicroserviceName + "-" + SerialNumber
	ok, trace := GetSXTrace(msg)
	if !ok {
		trace = TraceValue{
			Traces: map[int64]string{},
		}
		trace.Traces = make(map[int64]string)
	}

	t := time.Now().UnixNano()
	trace.Traces[t] = identifier

	j, err := json.Marshal(trace)
	if err != nil {
		return err
	}
	AddSHeader(msg, "x-trace", string(j))

	return nil
}
