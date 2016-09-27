package main

import (
        "fmt"
        "github.com/streadway/amqp"
        "log"
        "time"
        "strings"
        "net/http"
        "path/filepath"
        "regexp"
        "io/ioutil"
  "os/user"
    "github.com/BurntSushi/toml"
)

const queueName string = "cataloguing"

type Config struct {
  AMQPConnectionString string
  OrchestratorURI string
}

func GetConfig() (Config, error) {
  usr, _ := user.Current()
  dir := usr.HomeDir
  var conf Config
  _, err := toml.DecodeFile(strings.Join([]string{dir, ".trolley", "config.toml"},"/"), &conf)

  return conf,err
}

func failOnError(err error, msg string) {
        if err != nil {
                log.Fatalf("%s: %s\n", msg, err)
                panic(fmt.Sprintf("%s: %s\n", msg, err))
        }
}

type MessageProcessor func(Job)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func startConsumer(host string, queueName string, messageProcessor func(Job) (Job, error), conf Config) (*Consumer, error) {
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

  go func(msgs<-chan amqp.Delivery, done chan error){
    for d := range msgs {
      job := ParseMessageAsJob(d.Body)

      cataloguedJob, err := messageProcessor(job)
      failOnError(err, "Failed while cataloging")

      updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/cataloguingComplete"}, ""), cataloguedJob)

      d.Ack(false)

      t := time.Duration(1)
      time.Sleep(t * time.Second)
      log.Printf("Done\n")
    }
  }(msgs, c.done)


  go func() {
     fmt.Printf("closing: %s\n", <-c.conn.NotifyClose(make(chan *amqp.Error)))
     fmt.Printf("Reconnecting in 10 seconds\n")
     t := time.Duration(10)
     time.Sleep(t * time.Second)
     c, err = startConsumer(host,queueName, messageProcessor, notifyCatalouged, conf)
   }()
  return c, err
}

func main() {

  conf, err := GetConfig()
  failOnError(err, "Failed to read in config file")

  /*notifyProcessingCompleteChannel := make(chan Job)
  go func(job <-chan Job ) {
    log.Printf("Notify chan")
    err = updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/cataloguingComplete"}, ""), job)
    failOnError(err, "Failed to update orchestrator")
  }(notifyProcessingCompleteChannel)*/

  consumer, err := startConsumer(conf.AMQPConnectionString, queueName, Catalog, conf)
  defer consumer.conn.Close()
  defer consumer.channel.Close()

   failOnError(err, "Failed to register a consumer")


   forever := make(chan bool)

   log.Printf(" [*] Waiting for messages. To exit press CTRL+C\n")

   <-forever
}

func Catalog(job Job) (Job, error) {
  log.Printf("Cataloguing %s", job.Path)

  path := job.Path
  filename := filepath.Base(path)

  re := regexp.MustCompile(`([\w\d\s\.]+)[\-_\.\s]+[Ss]?(\d{1,2})[eEx](\d{2}).*\.(\w{3})`)

  if (re.MatchString(filename)) {
    submatch := re.FindStringSubmatch(filename)
    job.Show = strings.Trim(submatch[1], " -._")
    job.Show = strings.Replace(job.Show, ".", " ", -1)
    job.Season = submatch[2]
    job.Episode = submatch[3]
    job.Type = "TV show"

    query := fmt.Sprintf("http://www.omdbapi.com/?t=%s&Season=%s&Episode=%s", strings.Replace(job.Show, " ", "%20", -1), job.Season, job.Episode)
    log.Printf("Retrieving  %s\n", query)

    r, err := http.Get(query)
    defer r.Body.Close()
    if (err != nil) {
      return job, fmt.Errorf("Calling Web Service: %s", err)
    }

    body, err := ioutil.ReadAll(r.Body)
    if (err != nil) {
      return job, fmt.Errorf("reading response: %s", err)
    }


    job.Metadata = fmt.Sprintf("%s", body)
 } else {
   job.Type = "movie"
   job.Show = filename
 }
 return job, nil
}
