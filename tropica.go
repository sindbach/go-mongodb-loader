package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
)

type Taxi struct {
	Company                    string  `json:dropoff_census_tract`
	Dropoff_census_tract       string  `json:dropoff_census_tract`
	Dropoff_centroid_latitude  string  `json:dropoff_centroid_latitude`
	Dropoff_centroid_longitude string  `json:dropoff_centroid_longitude`
	Dropoff_community_area     string  `json:dropoff_community_area`
	Extras                     string  `json:extras`
	Fare                       string  `json:fare`
	Payment_type               string  `json:payment_type`
	Pickup_census_tract        string  `json:pickup_census_tract`
	Pickup_centroid_latitude   string  `json:pickup_centroid_latitude`
	Pickup_centroid_longitude  string  `json:pickup_centroid_longitude`
	Pickup_community_area      string  `json:pickup_community_area`
	Taxi_id                    string  `json:taxi_id`
	Tips                       float32 `json:tips`
	Tolls                      string  `json:tolls`
	Trip_end_timestamp         string  `json:trip_end_timestamp`
	Trip_id                    string  `json:trip_id`
	Trip_miles                 string  `json:trip_miles`
	Trip_seconds               string  `json:trip_seconds`
	Trip_start_timestamp       string  `json:trip_start_timestamp`
	Trip_total                 string  `json:trip_total`
}

func (t Taxi) toString() string {
	return toJson(t)
}

func toJson(t interface{}) string {
	bytes, err := json.Marshal(t)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	return string(bytes)
}

func worker(chLines <-chan string, wg *sync.WaitGroup, session *mgo.Session, database string, collection string) {
	defer wg.Done()

	sessionCopy := session.Copy()
	defer sessionCopy.Close()
	mcoll := sessionCopy.DB(database).C(collection)

	var taxiBulk []interface{}

	var taxidata Taxi
	counter := 0
	for line := range chLines {
		json.Unmarshal([]byte(line[1:]), &taxidata)
		counter += 1
		taxiBulk = append(taxiBulk, taxidata)

		counter += 1
		if counter > 999 {
			print(".")
			bulk := mcoll.Bulk()
			bulk.Insert(taxiBulk...)
			bulk.Run()
			counter = 0
			taxiBulk = nil
		}
	}
	if counter > 0 {
		print("last")
		bulk := mcoll.Bulk()
		bulk.Insert(taxiBulk...)
		bulk.Run()
	}
}

func main() {

	argFilePtr := flag.String("input", "", "File JSON input array")
	argNumWorkerPtr := flag.Int("numworker", 4, "Number of workers")
	argMongoPtr := flag.String("mongo", "mongodb://localhost:27017", "MongoDB URI to connect to")
	argDatabasePtr := flag.String("db", "taxidata", "MongoDB database")
	argCollectionPtr := flag.String("coll", "taxidata", "MongoDB collection")
	flag.Parse()

	if *argFilePtr == "" {
		flag.Usage()
		os.Exit(1)
	}

	f, err := os.Open(*argFilePtr)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	chLines := make(chan string)

	wg := new(sync.WaitGroup)

	session, err := mgo.Dial(*argMongoPtr)
	session.EnsureSafe(&mgo.Safe{W: 0})

	if err != nil {
		panic(err)
	}
	defer session.Close()

	for w := 1; w <= *argNumWorkerPtr; w++ {
		wg.Add(1)
		go worker(chLines, wg, session, *argDatabasePtr, *argCollectionPtr)
	}

	go func() {
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			chLines <- scanner.Text()
		}
		close(chLines)
	}()

	wg.Wait()
}
