package npi

import (
	"fmt"
	"github.com/Carevoyance/bloomdb"
	"github.com/Carevoyance/bloomnpi/helpers"
	"os"
	"sort"
	"time"
)

func Fetch() {
	bdb := bloomdb.CreateDB()
	db, err := bdb.SqlConnection()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	_, err = db.Exec("UPDATE bloom.data_sources SET status = 'RUNNING' WHERE source = 'NPI'")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	monthly, weekly, err := helpers.FilesAvailable()
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	monthlyTodos, err := helpers.NppesUnprocessed(db, []string{monthly})
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	weeklyTodos, err := helpers.NppesUnprocessed(db, weekly)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	if len(monthlyTodos) == 1 {
		err := helpers.Download(monthlyTodos[0])
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		reader, err := helpers.OpenReader("data/" + monthlyTodos[0] + ".zip")
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		defer reader.Close()

		monthlyKey := bloomdb.MakeKey(monthlyTodos[0])

		err = Upsert(reader, monthlyKey)
		if err != nil {
			return
		}

	}

	sort.Strings(weeklyTodos)

	for _, weeklyTodo := range weeklyTodos {
		err := helpers.Download(weeklyTodo)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		reader, err := helpers.OpenReader("data/" + weeklyTodo + ".zip")
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		defer reader.Close()

		weeklyKey := bloomdb.MakeKey(weeklyTodo)

		err = Upsert(reader, weeklyKey)
		if err != nil {
			return
		}
	}

	doneTodos := append(monthlyTodos, weeklyTodos...)
	for _, doneTodo := range doneTodos {
		key := bloomdb.MakeKey(doneTodo)
		_, err := db.Exec("INSERT INTO bloom.npi_files (id, file) VALUES ('" + key + "', '" + doneTodo + "')")
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		os.Remove("data/" + doneTodo + ".zip")
	}

	now := time.Now().Format(time.RFC3339)

	var query string
	if len(doneTodos) > 0 {
		query = "UPDATE bloom.data_sources SET status = 'READY', updated = '" + now + "', checked = '" + now + "' WHERE source = 'NPI'"
	} else {
		query = "UPDATE bloom.data_sources SET status = 'READY', checked = '" + now + "' WHERE source = 'NPI'"
	}

	_, err = db.Exec(query)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
}
