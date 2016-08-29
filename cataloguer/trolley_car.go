package main

import (
  "encoding/json"
  "fmt"
  "io/ioutil"
  "log"
  "net/http"
  "path/filepath"
  "regexp"
  "strings"
  "time"
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

func ParseMessageAsJob(msg []byte) Job {
  log.Printf("Received a message: %s", msg)
  job_as_bytes := []byte(msg)
  var job Job
  err := json.Unmarshal(job_as_bytes, &job)
  failOnError(err, "Failed to unmarshal JSON body")

  return job
}

func Catalog(job Job, conf Config) {
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
    fmt.Printf("%s", query)

    r, err := http.Get(query)
    failOnError(err, "Could not retrieve data from OMDB")
    defer r.Body.Close()

    body, err := ioutil.ReadAll(r.Body)
    failOnError(err, "Failed to retrieve OMDB details")

    job.Metadata = fmt.Sprintf("%s", body)

    err = updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/cataloguingComplete"}, ""), job)
    failOnError(err, "Failed to update cataloguingComplete")
 } else {
   job.Type = "movie"
   job.Show = filename
   err := updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/cataloguingComplete"}, ""), job)
   failOnError(err, "Failed to update cataloguingComplete")
 }

  //   err := updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/couldNotCatalogue"}, ""), job)
  //  failOnError(err, "Failed to update couldNotCatalogue")

}
