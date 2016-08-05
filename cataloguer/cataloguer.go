package main

import (
        "fmt"
        "path/filepath"
        "regexp"
        "strings"
        "github.com/streadway/amqp"
        "log"
        "time"
        "encoding/json"
        "os"
        "os/user"
        "net/http"
        "bytes"
        "github.com/BurntSushi/toml"
)

type Config struct {
  AMQPConnectionString string
  Queue string
  OrchestratorURI string
  CatalogueRoot string
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
  _, err := toml.DecodeFile(strings.Join([]string{dir, ".trolley", "cataloguer.toml"},"/"), &conf)
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
                  log.Printf("Converting %s", job["path"])

                  // TODO Configurable Handbrake settings
                  path := job["path"].(string)
                  filename := filepath.Base(path)

                  re := regexp.MustCompile(`([\w\d\s\.]+)[\.\s]([Ss]\d{1,2})([eE]\d{1,2}).+\.(\w{3})`)
                  catalog_to_path := conf.CatalogueRoot

                  if (re.MatchString(filename)) {
                     submatch := re.FindStringSubmatch(filename)
                     filename = fmt.Sprintf("%s_%s%s.%s", submatch[1], submatch[2], submatch[3], submatch[4])
                     catalog_to_path = fmt.Sprintf("%s/%s/%s", catalog_to_path, submatch[1], submatch[2])
                  }

                  os.MkdirAll(catalog_to_path, 0700)
                  new_path := strings.Join([]string{catalog_to_path, filename}, "/")
                  os.Rename(path, new_path)
                  fmt.Printf("%s\n", catalog_to_path)

                  job["path"] = new_path
                  j, err := json.Marshal(job)
                  failOnError(err, "Failed to marshal JSON body")

                  // Tell the Orchestrator the cataloguing is complete
                  url := strings.Join([]string{conf.OrchestratorURI, "/cataloguingComplete"}, "")

                  var jsonStr = []byte(j)
                  req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
                  req.Header.Set("Content-Type", "application/json")

                  client := &http.Client{}
                  resp, err := client.Do(req)
                  failOnError(err, "Failed to update job as cataloguingComplete in Orchestrator")

                  defer resp.Body.Close()


                  d.Ack(false)
                  t := time.Duration(10)
                  time.Sleep(t * time.Second)
                  log.Printf("Done")
          }
  }()

  log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
  <-forever

/*
        */
}
