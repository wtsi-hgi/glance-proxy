package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/rackspace/gophercloud"
	"github.com/rackspace/gophercloud/openstack"
	"github.com/rackspace/gophercloud/openstack/compute/v2/images"
	imageservice "github.com/rackspace/gophercloud/openstack/imageservice/v2/images"
)

const bufferSize = 4 * 1024 * 1024

var computeClient *gophercloud.ServiceClient
var imageClient *gophercloud.ServiceClient

func main() {
	flag.Parse()
	opts, err := openstack.AuthOptionsFromEnv()
	if err != nil {
		log.Fatalf("Failed to get openstack auth options from environment: %s\n", err)
	}

	provider, err := openstack.AuthenticatedClient(opts)
	if err != nil {
		log.Fatalf("Failed to authenticate to openstack: %s\n", err)
	}

	computeClient, err = openstack.NewComputeV2(provider, gophercloud.EndpointOpts{
		Name:   "nova",
		Region: "regionOne",
	})
	if err != nil {
		log.Fatalf("Failed to initialize openstack compute v2 provider: %s\n", err)
	}

	imageClient, err = openstack.NewImageServiceV2(provider, gophercloud.EndpointOpts{
		Region: "regionOne",
	})
	if err != nil {
		log.Fatalf("Failed to initialize openstack image service v2 provider: %s\n", err)
	}

	router := mux.NewRouter()
	router.HandleFunc("/id/{image_id}", idImageHandler).Methods("GET")
	router.HandleFunc("/name/{image_name}", namedImageHandler).Methods("GET")

	srv := &http.Server{
		Addr:           "127.0.0.1:8080",
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	log.Fatal(srv.ListenAndServe())
}

func namedImageHandler(w http.ResponseWriter, r *http.Request) {
	imageName := mux.Vars(r)["image_name"]

	imageId, err := images.IDFromName(computeClient, imageName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Image name not found: %s", err), http.StatusNotFound)
		return
	}

	getImage(w, r, imageId)
}

func idImageHandler(w http.ResponseWriter, r *http.Request) {
	imageId := mux.Vars(r)["image_id"]

	getImage(w, r, imageId)
}

func getImage(w http.ResponseWriter, r *http.Request, imageId string) {
	image, err := imageservice.Get(imageClient, imageId).Extract()
	if err != nil {
		http.Error(w, fmt.Sprintf("Image not found: %s", err), http.StatusNotFound)
		return
	}

	if image.Status != "active" {
		http.Error(w, "Image not active", http.StatusNotFound)
		return
	}

	var imageResult imageservice.GetImageDataResult
	rangeHeader := r.Header.Get("Range")
	var byteRangeSpec string
	if rangeHeader != "" {
		log.Printf("Have Range: %s\n", rangeHeader)
		const b = "bytes="
		if !strings.HasPrefix(rangeHeader, b) {
			http.Error(w, fmt.Sprintf("Invalid non-bytes range: %s", rangeHeader), http.StatusRequestedRangeNotSatisfiable)
			return
		}
		byteRangeSpec = rangeHeader[len(b):]
		imageResult = imageservice.DownloadPartial(imageClient, imageId, byteRangeSpec)
	} else {
		imageResult = imageservice.Download(imageClient, imageId)
	}

	imageReader, err := imageResult.Extract()

	if err != nil {
		http.Error(w, fmt.Sprintf("Image not downloadable: %s", err), http.StatusNotFound)
		return
	}

	hashWriter := md5.New()
	hashingImageReader := io.TeeReader(imageReader, hashWriter)

	buf := make([]byte, bufferSize)
	_, err = io.CopyBuffer(w, hashingImageReader, buf)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error retrieving image %s from Glance: %s", imageId, err), http.StatusBadGateway)
		return
	}

	receivedMd5 := hex.EncodeToString(hashWriter.Sum(nil))
	if image.Checksum != receivedMd5 {
		log.Printf("ERROR: checksum mismatch for image %s, glance reported checksum %s but we received md5 %s\n", imageId, image.Checksum, receivedMd5)
	}
}
