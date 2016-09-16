package main

import (
        "fmt"
        "github.com/streadway/amqp"
        "log"
        "time"
        "strings"
)

type MessageProcessor func(Job)

func failOnError(err error, msg string) {
        if err != nil {
                log.Fatalf("%s: %s", msg, err)
                panic(fmt.Sprintf("%s: %s", msg, err))
        }
}

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func startConsumer(host string, queueName string, messageProcessor func(Job) (Job, error), notifyCatalouged chan Job) (*Consumer, error) {
  fmt.Printf("Connecting to Queue {%s} on %s\n", queueName, host)
  c := &Consumer{
    conn:    nil,
    channel: nil,
    done:    make(chan error),
  }

  var err error

  c.conn, err = amqp.Dial(host)
  if (err != nil) {
    return nil, fmt.Errorf("Connecting to queue: %s", err)
  }

  c.channel, err = c.conn.Channel()
  if (err != nil) {
    return nil, fmt.Errorf("Opening Channel: %s", err)
  }

  msgs, err := c.channel.Consume(
           queueName, // queue
           "",     // consumer
           false,  // auto-ack
           false,  // exclusive
           false,  // no-local
           false,  // no-wait
           nil,    // args
  )
  if (err != nil) {
    return nil, fmt.Errorf("Consuming Messages: %s", err)
  }

  go func(msgs<-chan amqp.Delivery, done chan error, notifyCatalouged chan Job){
    for d := range msgs {
      job := ParseMessageAsJob(d.Body)

      cataloguedJob, err := messageProcessor(job)
      failOnError(err, "Failed while cataloging")

      d.Ack(false)

      notifyCatalouged <- cataloguedJob

      t := time.Duration(1)
      time.Sleep(t * time.Second)
      log.Printf("Done")
    }
  }(msgs, c.done, notifyCatalouged)


  go func() {
     fmt.Printf("closing: %s\n", <-c.conn.NotifyClose(make(chan *amqp.Error)))
     fmt.Printf("Reconnecting in 10 seconds\n")
     t := time.Duration(10)
     time.Sleep(t * time.Second)
     c, err = startConsumer(host,queueName, messageProcessor, notifyCatalouged)
   }()
  return c, err
}

func main() {

  conf, err := GetConfig()
  failOnError(err, "Failed to read in config file")

  notifyProcessingCompleteChannel := make(chan Job)
  go func(notifyChan <-chan Job ) {
    job := <-notifyChan
    err = updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/cataloguingComplete"}, ""), job)
    failOnError(err, "Failed to update cataloguingComplete")
  }(notifyProcessingCompleteChannel)

  consumer, err := startConsumer(conf.AMQPConnectionString, conf.Queue, Catalog, notifyProcessingCompleteChannel)
  defer consumer.conn.Close()
  defer consumer.channel.Close()

   failOnError(err, "Failed to register a consumer")


   forever := make(chan bool)

   log.Printf(" [*] Waiting for messages. To exit press CTRL+C\n")

   <-forever
}
