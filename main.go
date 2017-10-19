package main

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/op/go-logging"
	"gopkg.in/alecthomas/kingpin.v2"
	//"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"os"
	//"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	//  "github.com/aws/aws-sdk-go/aws/awserr"
	//"github.com/golang/glog"
)


var (
	app         = kingpin.New("chat", "A command-line chat application. see: https://github.com/whitmo/s3-fast-upload-golang")
	bucket      = app.Flag("bucket", "s3 bucket to upload to").Required().String()
	subfolder   = app.Flag("subfolder", "subfolder in s3 bucket, can be blank").String()
	num_workers = app.Flag("workers", "number of upload workers to use").Default("100").Int()
	region      = app.Flag("region", "aws region").Default("us-west-1").String()
	acl         = app.Flag("acl", "s3 upload acl - use either private or public").Default("private").String()
	sourceDir   = app.Arg("sourcedir", "source directory").Default("./").String()
	//destDir     = app.Arg("destdir", "dest dir for uploaded files (on local box)").String()

	verbose = app.Flag("verbose", "Be verbose").Bool()
	ec2_iam = app.Flag("ec2-role", "Running with instance iam role").Bool()
	log = logging.MustGetLogger("example")
	format = logging.MustStringFormatter(
		`%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`,
	)
)

// definition of a worker instance
type Worker struct {
	Acl          string          // s3 acl for uploaded files - for our use either "public" or "private"
	Bucket       string          // s3 bucket to upload to
	Subfolder    string          // s3 subfolder destination (if needed)
	Svc          *s3.S3          // instance of s3 svc
	File_channel chan string     // the channel to get file names from (upload todo list)
	Wg           *sync.WaitGroup // wait group - to signal when worker is finished
	SourceDir    string          // where source files are to be uploaded
	//DestDir      string          // where to move uploaded files to (on local box)
	Id           int             // worker id number for debugging
}

// worker to get all files inside a directory (recursively)
func get_file_list(searchDir string, file_channel chan string, num_workers int, wg *sync.WaitGroup) {
	defer wg.Done() // signal we are finished at end of function or return

	// sub function of how to recurse/walk the directory structure of searchDir
	_ = filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {

		// check if it's a file/directory (we just want files)
		file, err := os.Open(path)
		if err != nil {
			return nil
		}
		defer file.Close() // close file handle on return
		fi, err := file.Stat()
		if fi.IsDir() {
			return nil
		}

		path = strings.Replace(path, searchDir, "", 1)
		file_channel <- path // add file to the work channel (queue)
		return nil
	})

	// add num_workers empty files on as termination signal to them
	for i := 0; i < num_workers; i++ {
		file_channel <- ""
	}
}

// upload function for workers
// uploads a given file to s3
func (worker *Worker) upload(file string) (string, error) {

	// s3 destination file path
	destfile := worker.Subfolder + file
	worker.println("uploading to " + destfile)

	// open and read file
	f, err := os.Open(worker.SourceDir + file)
	if err != nil {
		return "Couldn't open file", err
	}
	defer f.Close()
	fileInfo, _ := f.Stat()
	var size int64 = fileInfo.Size()
	buffer := make([]byte, size)
	f.Read(buffer)
	fileBytes := bytes.NewReader(buffer)

	params := &s3.PutObjectInput{
		Bucket: aws.String(worker.Bucket),
		Key:    aws.String(destfile),
		Body:   fileBytes,
		ACL:    aws.String(worker.Acl),
	}

	// try the actual s3 upload
	resp, err := worker.Svc.PutObject(params)
	if err != nil {
		return "", err
	} else {
		return awsutil.StringValue(resp), nil
	}
}

// doUploads function for workers
//
// reads from the file channel (queue),
// calls upload function for each,
// then moves uploaded files to worker.DestDir
func (worker *Worker) doUploads() {
	defer worker.Wg.Done() // notify parent when I complete
	worker.println("doUploads() started")

	// loop until I receive "" as a termination signal
	for {
		file := <-worker.File_channel
		if file == "" {
			break
		}
		worker.println("File to upload: " + file)
		response, err := worker.upload(file)
		if err != nil {
			log.Error("error uploading" + file + ": " + response + " " + err.Error())
		}
		// else {
		// 	log.Info(response)
		// 	// make destination directory if needed
		// 	filename := path.Base(file)
		// 	directory := strings.Replace(file, "/"+filename, "", 1)
		// 	os.MkdirAll(worker.DestDir+directory, 0775)
		// 	// move file
		// 	os.Rename(worker.SourceDir+file, worker.DestDir+file)
		// }
	}
	worker.println("doUploads() finished")
}

// function to print out messages prefixed with worker-[id]
func (worker *Worker) println(message string) {
	msg := fmt.Sprintln("Worker-" + strconv.Itoa(worker.Id) + ": " + message)
	log.Info(msg)
}

func main() {
	kingpin.MustParse(app.Parse(os.Args[1:]))
	backend1 := logging.NewLogBackend(os.Stderr, "", 0)
	backend1Leveled := logging.AddModuleLevel(backend1)
	backend1Leveled.SetLevel(logging.ERROR, "")
	logging.SetBackend(backend1Leveled)

	if *verbose == true {
		fmt.Println("Using options:")
		fmt.Println("  bucket:", bucket)
		fmt.Println("  subfolder:", subfolder)
		fmt.Println("  num_workers:", num_workers)
		fmt.Println("  region:", region)
		fmt.Println("  acl:", acl)
		fmt.Println("  sourceDir:", sourceDir)
		//fmt.Println("  destDir:", destDir)
	}

	var wg sync.WaitGroup
	wg.Add(*num_workers + 1) // add 1 to account for the get_file_list thread!

	// file channel and thread to get the files
	file_channel := make(chan string, 0)
	go get_file_list(*sourceDir, file_channel, *num_workers, &wg)

	// set up s3 credentials from environment variables
	// these are shared to every worker

	base_sess := session.Must(session.NewSession(aws.NewConfig()))
	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.EnvProvider{},
			&ec2rolecreds.EC2RoleProvider{
				Client: ec2metadata.New(base_sess),
			},
		})

	sess := session.New(&aws.Config{
		Region: aws.String(*region),
		Credentials: creds,
		LogLevel: aws.LogLevel(1)})

	// if *ec2_iam != true {
	// 	creds := credentials.NewEnvCredentials()
	// } else {
	// 	creds := credentials.NewCredentials(&ec2rolecreds.EC2RoleProvider{})
	// }

	log.Info(fmt.Sprintln("Starting " + strconv.Itoa(*num_workers) + " workers..."))

	// create the desired number of workers
	for i := 1; i <= *num_workers; i++ {
		// make a new worker
		svc := s3.New(sess)
		worker := &Worker{
			Acl:          *acl,
			Bucket:       *bucket,
			Subfolder:    *subfolder,
			Svc:          svc,
			File_channel: file_channel,
			Wg:           &wg,
			SourceDir:    *sourceDir,
			//DestDir:      *destDir, Id: i,
		}
		go worker.doUploads()
	}

	// wait for all workers to finish
	// (1x file worker and all uploader workers)
	wg.Wait()
}
