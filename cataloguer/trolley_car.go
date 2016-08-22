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
  path string
  show string
  season string
  episode string
  metadata string
  job_id string
  id int
  created_at time.Time
  updated_at time.Time
  status string
  title string
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
  log.Printf("Cataloguing %s", job.path)

  path := job.path
  filename := filepath.Base(path)

  re := regexp.MustCompile(`([\w\d\s\.]+)[\-_\.\s]+[Ss]?(\d{1,2})[eEx](\d{2}).*\.(\w{3})`)

  if (re.MatchString(filename)) {
    submatch := re.FindStringSubmatch(filename)
    job.show = strings.Trim(submatch[1], " -._")
    job.season = submatch[2]
    job.episode = submatch[3]

    query := fmt.Sprintf("http://www.omdbapi.com/?t=%s&Season=%s&Episode=%s", strings.Replace(job.show, " ", "%20", -1), job.season, job.episode)
    fmt.Printf("%s", query)

    r, err := http.Get(query)
    failOnError(err, "Could not retrieve data from OMDB")
    defer r.Body.Close()

    body, err := ioutil.ReadAll(r.Body)
    failOnError(err, "Failed to retrieve OMDB details")

    job.metadata = fmt.Sprintf("%s", body)

    err = updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/cataloguingComplete"}, ""), job)
    failOnError(err, "Failed to update cataloguingComplete")
 } else {
   err := updateOrchestrator(strings.Join([]string{conf.OrchestratorURI, "/couldNotCatalogue"}, ""), job)
   failOnError(err, "Failed to update couldNotCatalogue")
 }
}
