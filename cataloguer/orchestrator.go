package main

import (
  "bytes"
  "encoding/json"
  "net/http"
)

func updateOrchestrator(url string, job interface{}) error {
  j, err := json.Marshal(job)
  failOnError(err, "Failed to marshal JSON body")

  var jsonStr = []byte(j)
  req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
  req.Header.Set("Content-Type", "application/json")

  client := &http.Client{}
  resp, err := client.Do(req)

  defer resp.Body.Close()

  return err
}
