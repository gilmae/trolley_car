package main

import (
        "fmt"
        "strings"
        "github.com/streadway/amqp"
        "log"
        "time"
        "encoding/json"
        "os/exec"
        "os/user"
        "github.com/BurntSushi/toml"
)

type Config struct {
  AMQPConnectionString string
  Queue string
  OrchestratorURI string
  MediaLibrary string
}

func GetConfig() (Config, error) {
  usr, _ := user.Current()
  dir := usr.HomeDir
  var conf Config
  _, err := toml.DecodeFile(strings.Join([]string{dir, ".trolley", "shelver.toml"},"/"), &conf)

  return conf,err
}

func failOnError(err error, msg string) {
        if err != nil {
                log.Fatalf("%s: %s", msg, err)
                panic(fmt.Sprintf("%s: %s", msg, err))
        }
}

type MessageProcessor func(Job)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func startConsumer(host string, queueName string, messageProcessor func(Job) (Job, error), notifyDone chan Job) (*Consumer, error) {
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

  go func(msgs<-chan amqp.Delivery, done chan error, notifyDone chan Job){
    for d := range msgs {
      job := ParseMessageAsJob(d.Body)

      cataloguedJob, err := messageProcessor(job)
      failOnError(err, "Failed while cataloging")

      d.Ack(false)

      notifyDone <- cataloguedJob

      t := time.Duration(1)
      time.Sleep(t * time.Second)
      log.Printf("Done")
    }
  }(msgs, c.done, notifyDone)


  go func() {
     fmt.Printf("closing: %s\n", <-c.conn.NotifyClose(make(chan *amqp.Error)))
     fmt.Printf("Reconnecting in 10 seconds\n")
     t := time.Duration(10)
     time.Sleep(t * time.Second)
     c, err = startConsumer(host,queueName, messageProcessor, notifyDone)
   }()
  return c, err
}

func main() {
  conf, err := GetConfig()
  failOnError(err, "Failed to read in config file")

  notifyProcessingCompleteChannel := make(chan Job)
  go func(notifyChan <-chan Job ) {
    job := <-notifyChan
    err = updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/shelvingComplete"}, ""), job)
    failOnError(err, "Failed to update orchestrator")
  }(notifyProcessingCompleteChannel)

  consumer, err := startConsumer(conf.AMQPConnectionString, conf.Queue, Shelve, notifyProcessingCompleteChannel)
  defer consumer.conn.Close()
  defer consumer.channel.Close()

   failOnError(err, "Failed to register a consumer")

   forever := make(chan bool)

   log.Printf(" [*] Waiting for messages. To exit press CTRL+C\n")

   <-forever


  log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
  <-forever
}

func Shelve(job Job) (Job, error) {
  log.Printf("Shelving %s", job.Path)

  // TODO Configurable Handbrake settings
  path := job.Path

  if (job.Type == "") {
    job.Type = "movie"
  }

  cmd := "/usr/bin/osascript"
  args := []string{"-e",
    "tell application \"iTunes\"",
    "-e",
    "launch",
    "-e",
    fmt.Sprintf("set new_file to add POSIX file \"%s\" to playlist \"Library\"", path),
    "-e",
    fmt.Sprintf("set video kind of new_file to %s", job.Type)}

  if (job.Type == "TV show") {
    if job.Season != "" {
      args = append(args, "-e")
      args = append(args, fmt.Sprintf("set season number of new_file to %s", job.Season))
    }

    if job.Show != "" {
      args = append(args, "-e")
      args = append(args, fmt.Sprintf("set show of new_file to \"%s\"", job.Show))
    }

    if job.Episode != "" {
      args = append(args, "-e")
      args = append(args, fmt.Sprintf("set episode number of new_file to %s", job.Episode))
    }

    if job.Metadata != "" {
      var metadata_as_interface interface{}
      err := json.Unmarshal([]byte(job.Metadata), &metadata_as_interface)

      if err == nil {

        metadata := metadata_as_interface.(map[string]interface{})

        if val, ok := metadata["Plot"]; ok {
          args = append(args, "-e")
          args = append(args, fmt.Sprintf("set description of new_file to \"%s\"", val))
        }

        if val, ok := metadata["Title"]; ok {
          args = append(args, "-e")
          args = append(args, fmt.Sprintf("set name of new_file to \"%s\"", val))
        }
      }
    }
  }

  args = append(args, "-e")
  args = append(args, "end tell")

  c := exec.Command(cmd, args...)
  output, err := c.CombinedOutput()
  log.Printf("%s\n", output)
  failOnError(err, "Failed to add to iTunes")

  return job, nil


}
