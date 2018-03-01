package main

import (
        "fmt"
        "github.com/aws/aws-sdk-go/aws"
        "github.com/aws/aws-sdk-go/service/sqs"
        "github.com/aws/aws-sdk-go/aws/credentials"
        "github.com/h2ik/go-sqs-poller/worker"
        "log"
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

func main() {
  aws_config := &aws.Config{
		Credentials: credentials.NewStaticCredentials("invalid", "invalid", ""),
    Endpoint:      aws.String("http://localhost:9324"),
    Region: aws.String("invalid"),
  }
  
  svc, url := worker.NewSQSClient("transcodes", aws_config)
	// set the queue url
	worker.QueueURL = url
	// start the worker
	worker.Start(svc, worker.HandlerFunc(func(msg *sqs.Message) error {
    job := ParseMessageAsJob(aws.StringValue(msg.Body))
    completedJob, err := Transcode(job)
    failOnError(err, "Failed while cataloging")
    
    updateOrchestrator(strings.Join([]string{"http://localhost:3001/transcodingComplete"}, ""), completedJob)
    //fmt.Println(aws.StringValue(msg.Body));
    return nil
  }))
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
