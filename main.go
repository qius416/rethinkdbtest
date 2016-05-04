package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/julienschmidt/httprouter"
	r "gopkg.in/dancannon/gorethink.v2"
)

// Doc aaa
type Doc struct {
	Name string    `json:"name" gorethink:"name"`
	Age  int       `json:"age,omitempty" gorethink:"age,omitempty"`
	Time time.Time `json:"time,omitempty" gorethink:"time,omitempty"`
}

// Change xxx
type Change struct {
	NewValue Doc `json:"newValue" gorethink:"new_value"`
	OldValue Doc `json:"oldValue" gorethink:"old_value"`
}

var session *r.Session

func init() {
	var err error
	session, err = r.Connect(r.ConnectOpts{
		Address:  "localhost:28015",
		Database: "test",
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {
	router := httprouter.New()
	router.GET("/get/:name", get)
	router.GET("/change/:table", changefeed)
	router.GET("/get", getAll)
	router.POST("/", create)
	router.DELETE("/:name", delete)
	router.PUT("/:name", update)
	log.Fatal(http.ListenAndServe("127.0.0.1:8000", router))
}

func getAll(resp http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var doc []Doc
	result, err := r.Table("mydoc").Run(session)
	defer result.Close()
	if err != nil {
		http.Error(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	err = result.All(&doc)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(http.StatusOK)
	json.NewEncoder(resp).Encode(doc)
}

func changefeed(resp http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	change, err := r.Table(ps.ByName("table")).Changes().Run(session)
	defer change.Close()
	if err != nil {
		http.Error(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	// get flusher
	conn, ok := resp.(http.Flusher)
	if !ok {
		http.Error(resp, "no flusher", http.StatusInternalServerError)
		return
	}
	cn, ok := resp.(http.CloseNotifier)
	if !ok {
		http.Error(resp, "no notifier for connection", http.StatusInternalServerError)
		return
	}
	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(http.StatusOK)
	var chg map[string]Doc
	// make a channel
	ch := make(chan Doc)
	go func() {
		for change.Next(&chg) {
			ch <- chg["new_val"]
		}
	}()
	for {
		select {
		case <-cn.CloseNotify():
			{
				fmt.Println("connection closed!!")
				return
			}
		case result := <-ch:
			{
				json.NewEncoder(resp).Encode(result)
				conn.Flush()
			}
		}
	}
}

func get(resp http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var key = ps.ByName("name")
	var doc Doc
	result, err := r.Table("mydoc").Get(key).Run(session)
	defer result.Close()
	if err != nil {
		http.Error(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	err = result.One(&doc)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(http.StatusOK)
	json.NewEncoder(resp).Encode(doc)
}

func create(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var payload Doc
	err := json.NewDecoder(req.Body).Decode(&payload)
	defer req.Body.Close()
	if err != nil {
		http.Error(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	if payload.Name == "" {
		http.Error(resp, "must field: name", http.StatusBadRequest)
		return
	}
	result, err := r.Table("mydoc").Insert(payload).RunWrite(session)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.WriteHeader(http.StatusCreated)
	fmt.Fprintf(resp, "result: %v", result)
}

func delete(resp http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var key = ps.ByName("name")
	result, err := r.Table("mydoc").Get(key).Delete().Run(session)
	defer result.Close()
	if err != nil {
		http.Error(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.WriteHeader(http.StatusNoContent)
}

func update(resp http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var payload Doc
	err := json.NewDecoder(req.Body).Decode(&payload)
	defer req.Body.Close()
	if err != nil {
		http.Error(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = r.Table("mydoc").Get(payload.Name).Update(payload).RunWrite(session)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.WriteHeader(http.StatusResetContent)
}
