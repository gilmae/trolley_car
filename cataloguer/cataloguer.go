package main

import (
        "fmt"
        "github.com/aws/aws-sdk-go/aws"
        "github.com/aws/aws-sdk-go/service/sqs"
        "github.com/aws/aws-sdk-go/aws/credentials"
        "github.com/h2ik/go-sqs-poller/worker"
        "log"
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

func main() {

	aws_config := &aws.Config{
		Credentials: credentials.NewStaticCredentials("invalid", "invalid", ""),
    Endpoint:      aws.String("http://localhost:9324"),
    Region: aws.String("invalid"),
  }
  
  svc, url := worker.NewSQSClient("cataloguing", aws_config)
	// set the queue url
	worker.QueueURL = url
	// start the worker
	worker.Start(svc, worker.HandlerFunc(func(msg *sqs.Message) error {
    job := ParseMessageAsJob(aws.StringValue(msg.Body))
    Catalog(job)
    //fmt.Println(aws.StringValue(msg.Body));
    return nil
  }))
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
