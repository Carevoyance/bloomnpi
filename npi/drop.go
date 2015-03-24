package npi

import (
	"log"
	"io/ioutil"
	"github.com/Carevoyance/bloomdb"
)

func Drop() {
	bloomdb := bloomdb.CreateDB()

	file, err := ioutil.ReadFile("sql/drop.sql")
	if err != nil {
		log.Fatal("Failed to read file.", err)
	}

	metaSql := string(file[:])
	conn, err := bloomdb.SqlConnection()
	if err != nil {
		log.Fatal("Failed to get database connection.", err)
	}

	_, err = conn.Exec(metaSql)
	if err != nil {
		log.Fatal("Failed to create metadata tables.", err)
	}
}
