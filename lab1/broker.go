package main

import (
	"sync"
	"time"
)

// A message contains its topic sequence number and the published data.
type Message struct {
	Sequence uint64
	Data     []byte
}

// Each subscriber has its own queue and a channel to signal unsubscribe.
type Subscriber struct {
	ID    uint64
	Queue chan Message
	Done  chan struct{}
}

// A topic keeps track of its subscribers and the next message number.
type Topic struct {
	Subscribers  map[uint64]*Subscriber
	NextSequence uint64
}

// The broker stores all topics and assigns unique subscriber IDs.
type Broker struct {
	Topics map[string]*Topic
	NextID uint64
	mu     sync.Mutex
}

// Create an empty broker.
func NewBroker() *Broker {
	broker := new(Broker)
	broker.Topics = make(map[string]*Topic)
	broker.NextID = 1
	broker.mu = sync.Mutex{}
	return broker
}

// Register a new subscriber and return its ID.
func (broker *Broker) Subscribe(topicName string) uint64 {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	topic, exists := broker.Topics[topicName]

	// The first subscriber also creates the topic.
	if !exists {
		topic = new(Topic)
		topic.Subscribers = make(map[uint64]*Subscriber)
		topic.NextSequence = 1
		broker.Topics[topicName] = topic
	}

	sub := new(Subscriber)
	sub.ID = broker.NextID

	// Buffer up to 10 messages per subscriber.
	sub.Queue = make(chan Message, 10)
	sub.Done = make(chan struct{})

	topic.Subscribers[sub.ID] = sub
	broker.NextID++

	return sub.ID

}

// Remove a subscriber, or return false if the topic or ID is unknown.
func (broker *Broker) Unsubscribe(topicName string, id uint64) bool {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	topic, exists := broker.Topics[topicName]

	if !exists {
		return false
	}

	sub, exists := topic.Subscribers[id]

	if !exists {
		return false
	}

	// Closing Done wakes up any Events request waiting for this subscriber.
	delete(topic.Subscribers, id)
	close(sub.Done)

	return true
}

// Add a published message to the queue of each subscriber on this topic.
func (broker *Broker) Publish(topicName string, data []byte) {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	topic, exists := broker.Topics[topicName]

	if !exists {
		return
	}

	// One sequence number is used for the same message in every queue.
	message := Message{}
	message.Sequence = topic.NextSequence
	message.Data = data

	topic.NextSequence++

	// Do not block a publisher when a subscriber has a full queue.
	for _, sub := range topic.Subscribers {
		select {
		case sub.Queue <- message:
		default:

		}

	}

}

// Find the subscriber that owns the queue requested by a client.
func (broker *Broker) GetSubscriber(topicName string, id uint64) (*Subscriber, bool) {

	broker.mu.Lock()
	defer broker.mu.Unlock()

	topic, exists := broker.Topics[topicName]

	if !exists {
		return nil, false
	}

	sub, exists := topic.Subscribers[id]

	if !exists {
		return nil, false
	}
	return sub, true
}

// Wait for a message, an unsubscribe signal, or the long-poll timeout.
func (broker *Broker) Events(topicName string, id uint64) (Message, int) {
	sub, exists := broker.GetSubscriber(topicName, id)

	if !exists {
		return Message{}, 400
	}

	// Do not hold the broker mutex while waiting here.
	select {
	case message := <-sub.Queue:
		return message, 200

	case <-sub.Done:
		return Message{}, 400

	case <-time.After(20 * time.Second):
		return Message{}, 204
	}
}
