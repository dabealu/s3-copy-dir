package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/minio/minio-go"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

func logFatal(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func logErr(err error) {
	if err != nil {
		log.Println("ERROR:", err)
	}
}

type s3endpoint struct {
	Endpoint  string `json:"endpoint"`
	SSL       bool   `json:"ssl"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

type options struct {
	Bucket      string `json:"bucket"`
	Directory   string `json:"directory"`
	Concurrency int    `json:"concurrency"`
}

type config struct {
	Source      s3endpoint `json:"source"`
	Destination s3endpoint `json:"destination"`
	options     `json:"options"`
}

// print sample configuration file
func printExampleConf() {
	c := config{
		s3endpoint{
			Endpoint:  "s3.amazonaws.com",
			SSL:       true,
			AccessKey: "AWSACCESSKEY",
			SecretKey: "AWSSECRETKEY",
		},
		s3endpoint{
			Endpoint:  "minio.example.com",
			SSL:       true,
			AccessKey: "MINIOACCESSKEY",
			SecretKey: "MINIOSECRETKEY",
		},
		options{
			Concurrency: 4,
			Bucket:      "bucketname",
			Directory:   "path/to/files",
		},
	}

	b, _ := json.MarshalIndent(c, "", "    ")
	fmt.Println(string(b))
}

// struct to track progress
type objCounter struct {
	sync.Mutex
	Total   int64
	Current int64
}

func (oc *objCounter) increment() {
	oc.Current++
}

func (oc *objCounter) getCurrent() int64 {
	return oc.Current
}

// load configuration file
func loadConfig(path string, conf *config) {
	file, err := os.Open(path)
	logFatal(err)
	b, err := ioutil.ReadAll(file)
	logFatal(err)
	err = json.Unmarshal(b, conf)
	logFatal(err)
}

// count objects in a dir to show progress during copying
func countDirObjects(src *minio.Client, bucket, dir string) int64 {
	log.Printf("starting counting objects in '%s/%s'", bucket, dir)

	var count int64

	// print objects count periodically
	stopCh := make(chan struct{})
	go func(c *int64) {
		for {
			select {
			case <-stopCh:
				close(stopCh)
				return
			default:
				log.Printf("still counting objects: %d ...", *c)
				time.Sleep(time.Second * 5)
			}
		}
	}(&count)

	objCh := src.ListObjects(bucket, dir, true, make(chan struct{}))
	for _ = range objCh {
		count++
	}
	stopCh <- struct{}{}

	log.Printf("total objects in '%s/%s': %d", bucket, dir, count)
	return count
}

// copy object from source to destination, skip if object already exists in destination
func copyObj(src, dst *minio.Client, bucket, objPath string, workersCh chan struct{}, oc *objCounter) {
	defer func(ch chan struct{}) { <-ch }(workersCh)

	srcObj, err := src.GetObject(bucket, objPath, minio.GetObjectOptions{})
	logErr(err)

	// check and skip if object already exists in dest
	dstObjStat, _ := dst.StatObject(bucket, objPath, minio.StatObjectOptions{})

	// copy
	size, err := dst.PutObject(bucket, objPath, srcObj, -1, minio.PutObjectOptions{})

	// check results
	oc.Lock()
	defer oc.Unlock()

	oc.increment()

	total := ""
	if oc.Total != -1 {
		total = "/" + strconv.FormatInt(oc.Total, 10)
	}

	if dstObjStat.Key != "" {
		log.Printf("[%d%s] skipping '%s/%s', already exists in destination", oc.getCurrent(), total, bucket, objPath)
		return
	}

	if err != nil {
		log.Printf("[%d%s] ERROR copying '%s/%s': %s", oc.getCurrent(), total, bucket, objPath, err)
	} else {
		log.Printf("[%d%s] copied '%s/%s', %d bytes", oc.getCurrent(), total, bucket, objPath, size)
	}
}

func main() {
	// parse flags and load config
	confPath := flag.String("config", "config.json", "location of config file")
	confSample := flag.Bool("sample", false, "print sample config and exit")
	showProgress := flag.Bool("progress", false, "show progress estimation, it requires to count objects before copying")
	flag.Parse()

	if *confSample {
		printExampleConf()
		os.Exit(0)
	}

	c := &config{}
	loadConfig(*confPath, c)

	log.Printf("source: '%s', destination: '%s', path: '%s/%s'",
		c.Source.Endpoint,
		c.Destination.Endpoint,
		c.options.Bucket,
		c.options.Directory)

	// initialize clients (*minio.Client)
	src, err := minio.New(c.Source.Endpoint, c.Source.AccessKey, c.Source.SecretKey, c.Source.SSL)
	logFatal(err)
	dst, err := minio.New(c.Destination.Endpoint, c.Destination.AccessKey, c.Destination.SecretKey, c.Destination.SSL)
	logFatal(err)

	// count objects in source dir, if enabled
	oc := &objCounter{}
	if *showProgress {
		oc.Total = countDirObjects(src, c.options.Bucket, c.options.Directory)
	} else {
		oc.Total = -1
	}

	doneCh := make(chan struct{})
	recursive := true
	// channel with stream of objects (<-chan ObjectInfo)
	objCh := src.ListObjects(c.options.Bucket, c.options.Directory, recursive, doneCh)

	// copy objects, limit workers concurrency with workersCh
	workersCh := make(chan struct{}, c.options.Concurrency)
	for obj := range objCh {
		workersCh <- struct{}{}
		go copyObj(src, dst, c.options.Bucket, obj.Key, workersCh, oc)
	}

	// wait untill all workers completed and exit
	for {
		if len(workersCh) > 0 {
			time.Sleep(time.Second * 1)
		} else {
			log.Println("copy completed")
			return
		}
	}
}
