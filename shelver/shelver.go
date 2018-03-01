package main

import (
        "fmt"
        "strings"
        "github.com/aws/aws-sdk-go/aws"
        "github.com/aws/aws-sdk-go/service/sqs"
        "github.com/aws/aws-sdk-go/aws/credentials"
        "github.com/h2ik/go-sqs-poller/worker"
        "log"
        "encoding/json"
        "os/exec"
        "os/user"
        "github.com/BurntSushi/toml"
)

const queueName string = "shelving"

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

func main() {
  aws_config := &aws.Config{
		Credentials: credentials.NewStaticCredentials("invalid", "invalid", ""),
    Endpoint:      aws.String("http://localhost:9324"),
    Region: aws.String("invalid"),
  }
  
  svc, url := worker.NewSQSClient("shelving", aws_config)
	// set the queue url
	worker.QueueURL = url
	// start the worker
	worker.Start(svc, worker.HandlerFunc(func(msg *sqs.Message) error {
    job := ParseMessageAsJob(aws.StringValue(msg.Body))
    completedJob, err := Shelve(job)

    failOnError(err, "Failed while cataloging")
    
    updateOrchestrator(strings.Join([]string{"http://localhost:3001/shelvingComplete"}, ""), completedJob)

    //fmt.Println(aws.StringValue(msg.Body));
    return nil
  }))
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
