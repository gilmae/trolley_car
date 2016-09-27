package main

import (
        "fmt"
        "github.com/streadway/amqp"
        "log"
        "time"
        "os"
        "os/exec"
        "os/user"
        "strings"
        "github.com/BurntSushi/toml"
)

const queueName string = "transcodes"

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
      failOnError(err, "Failed while transcoding")

      err = updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/transcodingComplete"}, ""), cataloguedJob)
      failOnError(err, "Failed to update orchestrator")

      d.Ack(false)

      t := time.Duration(1)
      time.Sleep(t * time.Second)
      log.Printf("Done")
    }
  }(msgs, c.done)


  go func() {
     fmt.Printf("closing: %s\n", <-c.conn.NotifyClose(make(chan *amqp.Error)))
     fmt.Printf("Reconnecting in 10 seconds\n")
     t := time.Duration(10)
     time.Sleep(t * time.Second)
     c, err = startConsumer(host,queueName, messageProcessor, conf)
   }()
  return c, err
}

func main() {
  conf, err := GetConfig()
  failOnError(err, "Failed to read in config file")

  consumer, err := startConsumer(conf.AMQPConnectionString, queueName, Transcode, conf)
  defer consumer.conn.Close()
  defer consumer.channel.Close()

   failOnError(err, "Failed to register a consumer")

   forever := make(chan bool)

   log.Printf(" [*] Waiting for messages. To exit press CTRL+C\n")

   <-forever


  log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
  <-forever
}

func Transcode(job Job) (Job, error) {
  log.Printf("Converting %s", job.Path)

  // TODO Configurable Handbrake settings
  path := job.Path
  new_path := strings.Join([]string{path, ".mp4"}, "")
  cmd:= "ffmpeg"
  args := []string{"-i", path, "-vcodec", "libx264", "-r", "24"}

  if job.Show != "" {
    args = append(args, "-metadata")
    args = append(args, fmt.Sprintf(`show="%s"`, job.Show))
  }

  args = append(args, new_path)
  log.Printf("Calling %s %s\n", cmd, args)

  if err := exec.Command(cmd, args...).Run(); err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }

  log.Printf("Successfully converted %s", path)

  job.Path = new_path

  return job, nil
}
