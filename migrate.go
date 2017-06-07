package main

import (
	"flag"
	"fmt"
	"sync"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Classifier struct {
	Id     bson.ObjectId `json:"id" bson:"_id"`
	Fields bson.M        `bson:",inline"`
}

func worker(chLines <-chan Classifier, wg *sync.WaitGroup, sessionDst *mgo.Session, database string, collection string) {
	defer wg.Done()
	var dbulk []interface{}
	counter := 0
	sessionCopy := sessionDst.Copy()
	defer sessionCopy.Close()
	collDst := sessionCopy.DB(database).C(collection)

	for line := range chLines {
		// Assign a new object id
		line.Id = bson.NewObjectId()
		dbulk = append(dbulk, line)
		counter++
		if counter > 99 {
			bulk := collDst.Bulk()
			bulk.Insert(dbulk...)
			_, err := bulk.Run()
			if err != nil {
				fmt.Println(err)
			}
			counter = 0
			dbulk = nil
		}
	}
	if counter > 0 {
		bulk := collDst.Bulk()
		bulk.Insert(dbulk...)
		_, err := bulk.Run()
		if err != nil {
			fmt.Println(err)
		}
	}
}

func main() {

	argMongoSourcePtr := flag.String("source", "mongodb://localhost:27017", "Source MongoDB URI")
	argMongoDestPtr := flag.String("dest", "mongodb://localhost:27018", "Destination Mongo URI")
	argNumWorkerPtr := flag.Int("numworker", 4, "Number of workers")
	argDatabasePtr := flag.String("db", "test", "MongoDB database")
	argCollectionPtr := flag.String("coll", "test", "MongoDB collection")
	flag.Parse()

	chLines := make(chan Classifier)

	wg := new(sync.WaitGroup)

	sessionSrc, errSrc := mgo.Dial(*argMongoSourcePtr)
	if errSrc != nil {
		panic(errSrc)
	}
	defer sessionSrc.Close()
	collSrc := sessionSrc.DB(*argDatabasePtr).C(*argCollectionPtr)

	sessionDst, errDst := mgo.Dial(*argMongoDestPtr)
	if errDst != nil {
		panic(errDst)
	}
	defer sessionDst.Close()

	wg.Add(*argNumWorkerPtr)
	for w := 1; w <= *argNumWorkerPtr; w++ {
		go worker(chLines, wg, sessionDst, *argDatabasePtr, *argCollectionPtr)
	}

	go func() {
		cursor := collSrc.Find(nil).Sort("_id")
		var doc Classifier
		docs := cursor.Iter()
		for docs.Next(&doc) {
			chLines <- doc
		}
		close(chLines)
	}()

	wg.Wait()
}
