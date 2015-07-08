package helpers

import (
	"io"
	"log"
	"net/http"
	"os"
)

func Download(file string) error {
	out, err := os.Create("data/" + file + ".zip")
	if err != nil {
		return err
	}
	defer out.Close()

	path := "http://download.cms.gov/nppes/" + file + ".zip"
	log.Print("Downloading", path)
	resp, err := http.Get(path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}
