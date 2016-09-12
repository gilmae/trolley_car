package main

import (
        "fmt"
        "github.com/streadway/amqp"
        "log"
        "time"
        "strings"
)

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

type DeliveriesHandler func(<-chan amqp.Delivery, chan error, chan Job)

func startConsumer(host string, queueName string, handle DeliveriesHandler, notifyCatalouged chan Job) (*Consumer, error) {
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

  go handle(msgs, c.done, notifyCatalouged)

  return c, err
}

func main() {

  conf, err := GetConfig()
  failOnError(err, "Failed to read in config file")

  notifyCatalouged := make(chan Job)

  go func(notifyChan <-chan Job ) {
    job := <-notifyChan
    err = updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/cataloguingComplete"}, ""), job)
    failOnError(err, "Failed to update cataloguingComplete")
  }(notifyCatalouged)

  consumer, err := startConsumer(conf.AMQPConnectionString, conf.Queue, handleCataloging, notifyCatalouged)
  defer consumer.conn.Close()
  defer consumer.channel.Close()

   failOnError(err, "Failed to register a consumer")
   go func() {
   		fmt.Printf("closing: %s", <-consumer.conn.NotifyClose(make(chan *amqp.Error)))
   	}()

   forever := make(chan bool)

   log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

   <-forever
}

func handleCataloging(msgs<-chan amqp.Delivery, done chan error, notifyCatalouged chan Job) {
  for d := range msgs {
    job := ParseMessageAsJob(d.Body)

    err := Catalog(&job)
    failOnError(err, "Failed while cataloging")


    d.Ack(false)

    notifyCatalouged <- job

    t := time.Duration(1)
    time.Sleep(t * time.Second)
    log.Printf("Done")
  }

}
