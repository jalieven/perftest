/*
 * Minio Cloud Storage (C) 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
	"math/rand"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Change this value to test with a different object size.
const defaultObjectSize = 10 * 1024 * 1024

const defaultMetaCount = 1
const defaultMetaSize = 1024

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// Uploads all the inputs objects in parallel, upon any error this function panics.
func parallelUploads(objectNames []string, data []byte, metaCount int, metaSize int) {
	var wg sync.WaitGroup
	for _, objectName := range objectNames {
		wg.Add(1)
		go func(objectName string) {
			defer wg.Done()
			if err := uploadBlob(data, objectName, metaCount, metaSize); err != nil {
				panic(err)
			}
		}(objectName)
	}
	wg.Wait()
}

// uploadBlob does an upload to the S3/Minio server
func uploadBlob(data []byte, objectName string, metaCount int, metaSize int) error {
	credsUp := credentials.NewStaticCredentials(os.Getenv("ACCESSKEY"), os.Getenv("SECRETKEY"), "")
	sessUp := session.New(aws.NewConfig().
		WithCredentials(credsUp).
		WithRegion("us-east-1").
		WithEndpoint(os.Getenv("ENDPOINT")).
		WithS3ForcePathStyle(true))

	uploader := s3manager.NewUploader(sessUp, func(u *s3manager.Uploader) {
		u.PartSize = 64 * 1024 * 1024 // 64MB per part
	})

	meta := make(map[string]string)
	for n := 0; n <= metaCount.Value; n++ {
		var metadataValue = RandStringBytes(1024)
		meta[fmt.Sprintf("%s-%v", "test-metadata-key", metaCount)] = aws.String(metadataValue)
	}

	var err error
	_, err = uploader.Upload(&s3manager.UploadInput{
		Body:   bytes.NewReader(data),
		Bucket: aws.String(os.Getenv("BUCKET")),
		Key:    aws.String(objectName),
		Metadata: meta,
	})

	return err
}

var (
	objectSize = flag.Int("size", defaultObjectSize, "Size of the object to upload.")
	metaCount = flag.Int("meta-count", defaultMetaCount, "Metadata entry count of the object to upload.")
	metaSize = flag.Int("meta-size", defaultMetaSize, "Metadata size of each entry of the object to upload.")
)

func main() {
	flag.Parse()

	concurrency := os.Getenv("CONCURRENCY")
	conc, err := strconv.Atoi(concurrency)
	if err != nil {
		log.Fatalln(err)
	}

	var objectNames []string
	for i := 0; i < conc; i++ {
		objectNames = append(objectNames, fmt.Sprintf("object%d", i+1))
	}

	var data = bytes.Repeat([]byte("a"), *objectSize)

	start := time.Now().UTC()
	parallelUploads(objectNames, data, *metaCount, *metaSize)

	totalSize := conc * *objectSize
	elapsed := time.Since(start)
	fmt.Println("Elapsed time :", elapsed)
	seconds := float64(elapsed) / float64(time.Second)
	fmt.Printf("Speed        : %4.0f objs/sec\n", float64(conc)/seconds)
	fmt.Printf("Bandwidth    : %4.0f MBit/sec\n", float64(totalSize)/seconds/1024/1024)
}
