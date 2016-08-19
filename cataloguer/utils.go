package main

import (
  "net/url"
)

func UrlEncoded(str string) (string, error) {
    u, err := url.Parse(str)
    if err != nil {
        return "", err
    }
    return u.String(), nil
}
