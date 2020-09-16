package main

import (
	"fmt"
	"log"
	"net/http"
)

func responseHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Regatta lives!")
}

func main() {
	http.HandleFunc("/liveness", responseHandler)

	fmt.Printf("Starting server at port 8080\n")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
