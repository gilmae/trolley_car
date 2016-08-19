package main

import (
  "github.com/BurntSushi/toml"
  "os/user"
  "strings"

)

type Config struct {
  AMQPConnectionString string
  Queue string
  OrchestratorURI string
}

func GetConfig() (Config, error) {
  usr, _ := user.Current()
  dir := usr.HomeDir
  var conf Config
  _, err := toml.DecodeFile(strings.Join([]string{dir, ".trolley", "cataloguer.toml"},"/"), &conf)

  return conf,err
}
