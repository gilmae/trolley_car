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
        "net/http"
        "bytes"
        "github.com/BurntSushi/toml"
)

type Config struct {
  AMQPConnectionString string
  Queue string
  OrchestratorURI string
  MediaLibrary string
}

func failOnError(err error, msg string) {
        if err != nil {
                log.Fatalf("%s: %s", msg, err)
                panic(fmt.Sprintf("%s: %s", msg, err))
        }
}

func main() {
  usr, _ := user.Current()
  dir := usr.HomeDir
  var conf Config
  _, err := toml.DecodeFile(strings.Join([]string{dir, ".trolley", "shelver.toml"},"/"), &conf)
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

  forever := make(chan bool)

  go func() {
          for d := range msgs {
                  log.Printf("Received a message: %s", d.Body)
                  job_as_bytes := []byte(d.Body)
                  var job_as_interface interface{}
                  err := json.Unmarshal(job_as_bytes, &job_as_interface)
                  failOnError(err, "Failed to unmarshal JSON body")

                  job:= job_as_interface.(map[string]interface{})
                  log.Printf("Shelving %s", job["path"])

                  // TODO Configurable Handbrake settings
                  path := job["path"].(string)

                  if _, ok := job["type"]; !ok {
                    job["type"] = "movie"
                  }

                  cmd := "/usr/bin/osascript"
                  args := []string{"-e", "tell application \"iTunes\"", "-e", "launch", "-e", fmt.Sprintf("set new_file to add POSIX file \"%s\" to playlist \"Library\"", path), "-e", fmt.Sprintf("set video kind of new_file to %s", job["type"])}

                  if (job["type"] == "TV show") {
                  if val, ok := job["season"]; ok {
                    args = append(args, "-e")
                    args = append(args, fmt.Sprintf("set season number of new_file to %s", val))
                  }

                  if val, ok := job["show"]; ok {
                    args = append(args, "-e")
                    args = append(args, fmt.Sprintf("set show of new_file to \"%s\"", val))
                  }

                  if val, ok := job["episode"]; ok {
                    args = append(args, "-e")
                    args = append(args, fmt.Sprintf("set episode number of new_file to %s", val))
                  }

                  if val, ok := job["metadata"]; ok {
                    var metadata_as_interface interface{}
                    err = json.Unmarshal([]byte(val.(string)), &metadata_as_interface)

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

                  log.Printf("Calling %s %s\n", cmd, args)

                  c := exec.Command(cmd, args...)
                  output, err := c.CombinedOutput()
                  log.Printf("%s\n", output)
                  failOnError(err, "Failed to add to iTunes")

                  j, err := json.Marshal(job)
                  failOnError(err, "Failed to marshal JSON body")

                  // Tell the Orchestrator the shelving is complete
                  url := strings.Join([]string{conf.OrchestratorURI, "/shelvingComplete"}, "")

                  var jsonStr = []byte(j)
                  req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
                  req.Header.Set("Content-Type", "application/json")

                  client := &http.Client{}
                  resp, err := client.Do(req)
                  failOnError(err, "Failed to update job as shelvingComplete in Orchestrator")

                  defer resp.Body.Close()


                  d.Ack(false)
                  t := time.Duration(1)
                  time.Sleep(t * time.Second)
                  log.Printf("Done")
          }
  }()

  log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
  <-forever

/*
        */
}
