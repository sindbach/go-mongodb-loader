package main

import (
    "fmt"
    "encoding/json"
    "gopkg.in/mgo.v2"
    "os"
    //"gopkg.in/mgo.v2/bson"
    "bufio"
    "sync"
)


type Taxi struct {
    Company string `json:dropoff_census_tract`
    Dropoff_census_tract string `json:dropoff_census_tract`
    Dropoff_centroid_latitude string `json:dropoff_centroid_latitude`
    Dropoff_centroid_longitude string `json:dropoff_centroid_longitude`
    Dropoff_community_area string `json:dropoff_community_area`
    Extras string `json:extras`
    Fare string `json:fare`
    Payment_type string `json:payment_type`
    Pickup_census_tract string `json:pickup_census_tract`
    Pickup_centroid_latitude string `json:pickup_centroid_latitude`
    Pickup_centroid_longitude string `json:pickup_centroid_longitude`
    Pickup_community_area string `json:pickup_community_area`
    Taxi_id string `json:taxi_id`
    Tips string `json:tips`
    Tolls string `json:tolls`
    Trip_end_timestamp string `json:trip_end_timestamp`
    Trip_id string `json:trip_id`
    Trip_miles string `json:trip_miles`
    Trip_seconds string `json:trip_seconds`
    Trip_start_timestamp string `json:trip_start_timestamp`
    Trip_total string `json:trip_total`
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


func worker(chLines <-chan string, wg *sync.WaitGroup, chResults chan<-int, mcoll *mgo.Collection) {
    defer wg.Done()

    var taxiBulk []interface{}

    var taxidata Taxi
    counter := 0 
    for line := range chLines {
        json.Unmarshal([]byte(line[1:]), &taxidata)
        counter +=1
        taxiBulk = append(taxiBulk, taxidata)

        counter +=1
        if counter > 999 {
            fmt.Print(".")
            bulk := mcoll.Bulk()
            bulk.Insert(taxiBulk...)
            bulk.Run()
            counter = 0 
            taxiBulk = nil
        }
    }
    if counter > 0 {
        fmt.Println("last round", counter)
        bulk := mcoll.Bulk()
        bulk.Insert(taxiBulk...)
        bulk.Run()
    }
    chResults<-1
}

func main() {

    f, err := os.Open("./taxidata.json")
    if err != nil {
        panic(err)
    }
    defer f.Close()

    chLines := make(chan string)
    chResults := make(chan int)

    wg:= new(sync.WaitGroup)

    session, err := mgo.Dial("mongodb://localhost:27017")
    session.EnsureSafe(&mgo.Safe{W: 0})

    if err != nil {
        panic(err)
    }
    defer session.Close()

    mcoll := session.DB("taxidata").C("taxidata")
    
    for w:=1; w<=8; w++ {
        wg.Add(1)
        go worker(chLines, wg, chResults, mcoll)
    }

    go func() {
        scanner := bufio.NewScanner(f)     
        for scanner.Scan(){
            chLines <- scanner.Text()
        }   
        close(chLines)
    }()

    go func(){
        wg.Wait()
        close(chResults)
    }()

    for range chResults {
    }
}
