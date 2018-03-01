package main

import (
  "encoding/json"
  "log"
  "time"
  "net/http"
  "bytes"
)

type Job struct {
  Path string `json:"path"`
  Show string `json:"show"`
  Season string `json:"season"`
  Episode string `json:"episode"`
  Metadata string `json:"metadata"`
  Job_id string `json:"job_id"`
  Id int `json:"id"`
  Created_at time.Time `json:"created_at"`
  Updated_at time.Time `json:"updated_at"`
  Status string `json:"status"`
  Title string `json:"title"`
  Type string `json:type`
}

func ParseMessageAsJob(msg string) Job {
  job_as_bytes := []byte(msg)
  var job Job
  err := json.Unmarshal(job_as_bytes, &job)
  failOnError(err, "Failed to unmarshal JSON body")

  return job
}

func updateOrchestrator(url string, job Job) error {  log.Printf("updating orchestrator: %s", url)
  j, err := json.Marshal(job)
  failOnError(err, "Failed to marshal JSON body")

  var jsonStr = []byte(j)
  req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
  req.Header.Set("Content-Type", "application/json")

  client := &http.Client{}
  _, err = client.Do(req)

  return err
}
