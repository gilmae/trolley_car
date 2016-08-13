package main

import (
        "fmt"
        "path/filepath"
        "regexp"
        "io/ioutil"
        "strings"
        "github.com/streadway/amqp"
        "log"
        "time"
        "encoding/json"
        "net/url"
        "os/user"
        "net/http"
        "bytes"
        "github.com/BurntSushi/toml"
)

type Config struct {
  AMQPConnectionString string
  Queue string
  OrchestratorURI string
}

func failOnError(err error, msg string) {
        if err != nil {
                log.Fatalf("%s: %s", msg, err)
                panic(fmt.Sprintf("%s: %s", msg, err))
        }
}

func getJson(url string, target interface{}) error {
    r, err := http.Get(url)
    if err != nil {
        return err
    }
    defer r.Body.Close()

    return json.NewDecoder(r.Body).Decode(target)
}

func updateOrchestrator(url string, job interface{}) error {
  j, err := json.Marshal(job)
  failOnError(err, "Failed to marshal JSON body")

  // Tell the Orchestrator the cataloguing is complete


  var jsonStr = []byte(j)
  req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
  req.Header.Set("Content-Type", "application/json")

  client := &http.Client{}
  resp, err := client.Do(req)

  defer resp.Body.Close()

  return err
}

func UrlEncoded(str string) (string, error) {
    u, err := url.Parse(str)
    if err != nil {
        return "", err
    }
    return u.String(), nil
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
       log.Printf("Cataloguing %s", job["path"])

       path := job["path"].(string)
       filename := filepath.Base(path)

       re := regexp.MustCompile(`([\w\d\s\.]+)[\-_\.\s]+[Ss]?(\d{1,2})[eEx]?(\d{2}).*\.(\w{3})`)

       if (re.MatchString(filename)) {
         submatch := re.FindStringSubmatch(filename)
         job["show"] = strings.Trim(submatch[1], " -._")
         job["season"] = submatch[2]
         job["episode"] = submatch[3]

         query := fmt.Sprintf("http://www.omdbapi.com/?t=%s&Season=%s&Episode=%s", strings.Replace(job["show"], " ", "%20", -1), job["season"], job["episode"])
         fmt.Printf("%s", query)

         r, err := http.Get(query)
         failOnError(err, "Could not retrieve data from OMDB")
         defer r.Body.Close()

         body, err := ioutil.ReadAll(r.Body)
         failOnError(err, "Failed to retrieve OMDB details")

         job["metadata"] = fmt.Sprintf("%s", body)

         err = updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/cataloguingComplete"}, ""), job)
         failOnError(err, "Failed to update cataloguingComplete")
      } else {
        err = updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/couldNotCatalogue"}, ""), job)
        failOnError(err, "Failed to update couldNotCatalogue")
      }

      d.Ack(false)
      t := time.Duration(1)
      time.Sleep(t * time.Second)
      log.Printf("Done")

    }
  }()

  log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
  <-forever
}
