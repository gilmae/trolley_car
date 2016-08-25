package main

import (
        "fmt"
        "github.com/streadway/amqp"
        "log"
        "time"
)

func failOnError(err error, msg string) {
        if err != nil {
                log.Fatalf("%s: %s", msg, err)
                panic(fmt.Sprintf("%s: %s", msg, err))
        }
}

func main() {

  conf, err := GetConfig()
  failOnError(err, "Failed to read in config file")

   // Shamless copy paste of the worker.go code from RabbitMQ's tutorial on Queues
   conn, err := amqp.Dial(conf.AMQPConnectionString)
   failOnError(err, "Failed to connect to RabbitMQ")
   defer conn.Close()

   ch, err := conn.Channel()
   failOnError(err, "Failed to open a channel")
   defer ch.Close()

   q, err := ch.QueueDeclare(
           conf.Queue, // name
           true,         // durable
           false,        // delete when unused
           false,        // exclusive
           false,        // no-wait
           nil,          // arguments
   )
   failOnError(err, "Failed to declare a queue")

   err = ch.Qos(
           1,     // prefetch count
           0,     // prefetch size
           false, // global
   )
   failOnError(err, "Failed to set QoS")

   msgs, err := ch.Consume(
           q.Name, // queue
           "",     // consumer
           false,  // auto-ack
           false,  // exclusive
           false,  // no-local
           false,  // no-wait
           nil,    // args
   )
   failOnError(err, "Failed to register a consumer")
   go func() {
   		fmt.Printf("closing: %s", <-conn.NotifyClose(make(chan *amqp.Error)))
   	}()

   forever := make(chan bool)

   go func() {
     for d := range msgs {
       job := ParseMessageAsJob(d.Body)

       Catalog(job, conf)

       d.Ack(false)
       t := time.Duration(1)
       time.Sleep(t * time.Second)
       log.Printf("Done")
    }
  }()

  log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
  <-forever
}
