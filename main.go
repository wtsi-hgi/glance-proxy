package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
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

var noVerifyChecksum bool

func main() {
	flag.BoolVar(&noVerifyChecksum, "no-verify-checksum", false, "do not verify checksums on every request")
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
	router.HandleFunc("/id/{image_id}", idImageHandler).Methods("GET", "HEAD")
	router.HandleFunc("/name/{image_name}", namedImageHandler).Methods("GET", "HEAD")

	srv := &http.Server{
		Addr:           "127.0.0.1:8080",
		Handler:        router,
		ReadTimeout:    300 * time.Second,
		WriteTimeout:   300 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	log.Fatal(srv.ListenAndServe())
}

func namedImageHandler(w http.ResponseWriter, r *http.Request) {
	imageName := mux.Vars(r)["image_name"]
	log.Printf("%s request for image named %s\n", r.Method, imageName)

	imageId, err := images.IDFromName(computeClient, imageName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Image name not found: %s", err), http.StatusNotFound)
		log.Printf("WARNING: %s\n", fmt.Sprintf("Image name not found: %s", err))
		return
	}

	getImage(w, r, imageId)
}

func idImageHandler(w http.ResponseWriter, r *http.Request) {
	imageId := mux.Vars(r)["image_id"]
	log.Printf("%s request for image id %s\n", r.Method, imageId)

	getImage(w, r, imageId)
}

func getImage(w http.ResponseWriter, r *http.Request, imageId string) {
	w.Header().Set("Accept-Ranges", "bytes")

	image, err := imageservice.Get(imageClient, imageId).Extract()
	if err != nil {
		http.Error(w, fmt.Sprintf("Image not found: %s", err), http.StatusNotFound)
		log.Printf("WARNING: %s\n", fmt.Sprintf("Image not found: %s", err))
		return
	}

	if image.Status != "active" {
		http.Error(w, "Image not active", http.StatusNotFound)
		log.Printf("WARNING: %s\n", "Image not active")
		return
	}

	imageSize := int64(image.SizeBytes)
	if imageSize <= 0 {
		log.Printf("WARNING: returning empty content for image with Size <= 0: %s\n", imageSize)
		return
	}

	if image.Checksum == "" {
		log.Printf("WARNING: image has no checksum in glance, disabling checksum verification\n")
		noVerifyChecksum = true
	}

	// default to the entire image when Range not requested
	var offset int64 = 0
	var length int64 = imageSize
	var statusCode int = http.StatusOK

	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		// set the Content-Range header now for any 416 responses, we will override it later for 206
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", imageSize))
		const bytesUnitEq = "bytes="
		if !strings.HasPrefix(rangeHeader, bytesUnitEq) {
			http.Error(w, fmt.Sprintf("Invalid Range (only byte ranges are supported): %s", rangeHeader), http.StatusRequestedRangeNotSatisfiable)
			log.Printf("WARNING: %s\n", fmt.Sprintf("Invalid Range (only byte ranges are supported): %s", rangeHeader))
			return
		}
		byteRangeSpec := strings.TrimSpace(rangeHeader[len(bytesUnitEq):])
		// support single range only
		if strings.Contains(byteRangeSpec, ",") {
			http.Error(w, fmt.Sprintf("Invalid Range (multiple byte ranges are not supported): %s", rangeHeader), http.StatusRequestedRangeNotSatisfiable)
			log.Printf("WARNING: %s\n", fmt.Sprintf("Invalid Range (multiple byte ranges are not supported): %s", rangeHeader))
			return
		}
		if byteRangeSpec != "" {
			i := strings.Index(byteRangeSpec, "-")
			if i < 0 {
				http.Error(w, fmt.Sprintf("Invalid Range (missing '-'): %s", rangeHeader), http.StatusRequestedRangeNotSatisfiable)
				log.Printf("WARNING: %s\n", fmt.Sprintf("Invalid Range (missing '-'): %s", rangeHeader))
				return
			}
			startSpec := strings.TrimSpace(byteRangeSpec[:i])
			endSpec := strings.TrimSpace(byteRangeSpec[i+1:])
			if startSpec == "" {
				// ranges such as '-300' mean to start 300 from the end
				end, err := strconv.ParseInt(endSpec, 10, 64)
				if err != nil {
					http.Error(w, fmt.Sprintf("Invalid Range '%s' (end position '%s' could not be parsed as an integer): %s", rangeHeader, endSpec, err), http.StatusRequestedRangeNotSatisfiable)
					log.Printf("WARNING: %s\n", fmt.Sprintf("Invalid Range '%s' (end position '%s' could not be parsed as an integer): %s", rangeHeader, endSpec, err))
					return
				}
				if end < 0 {
					http.Error(w, fmt.Sprintf("Invalid Range '%s' (end position '%s' was negative): %s", rangeHeader, endSpec, err), http.StatusRequestedRangeNotSatisfiable)
					log.Printf("WARNING: %s\n", fmt.Sprintf("Invalid Range '%s' (end position '%s' was negative): %s", rangeHeader, endSpec, err))
					return
				}
				if end > imageSize {
					end = imageSize
				}
				offset = imageSize - end
				length = imageSize - offset
			} else {
				offset, err = strconv.ParseInt(startSpec, 10, 64)
				if err != nil {
					http.Error(w, fmt.Sprintf("Invalid Range (start position '%s' could not be parsed as an integer): %s", rangeHeader, err), http.StatusRequestedRangeNotSatisfiable)
					log.Printf("WARNING: %s\n", fmt.Sprintf("Invalid Range (start position '%s' could not be parsed as an integer): %s", rangeHeader, err))
					return
				}
				if offset < 0 {
					http.Error(w, fmt.Sprintf("Invalid Range '%s' (start position '%s' was negative): %s", rangeHeader, startSpec, err), http.StatusRequestedRangeNotSatisfiable)
					log.Printf("WARNING: %s\n", fmt.Sprintf("Invalid Range '%s' (start position '%s' was negative): %s", rangeHeader, startSpec, err))
					return
				}
				if offset >= imageSize {
					http.Error(w, fmt.Sprintf("Range '%s' has a start position '%s' beyond extent of image (size %d)", rangeHeader, startSpec, imageSize), http.StatusRequestedRangeNotSatisfiable)
					log.Printf("WARNING: %s\n", fmt.Sprintf("Range '%s' has a start position '%s' beyond extent of image (size %d)", rangeHeader, startSpec, imageSize))
					return
				}
				if endSpec == "" {
					// no end specified, range extends to imageSize
					length = imageSize - offset
				} else {
					end, err := strconv.ParseInt(endSpec, 10, 64)
					if err != nil {
						http.Error(w, fmt.Sprintf("Invalid Range '%s' (end position '%s' could not be parsed as an integer): %s", rangeHeader, endSpec, err), http.StatusRequestedRangeNotSatisfiable)
						log.Printf("WARNING: %s\n", fmt.Sprintf("Invalid Range '%s' (end position '%s' could not be parsed as an integer): %s", rangeHeader, endSpec, err))
						return
					}
					if end <= offset {
						http.Error(w, fmt.Sprintf("Invalid Range '%s' (end position '%d' does not come after start '%d')", rangeHeader, end, offset), http.StatusRequestedRangeNotSatisfiable)
						log.Printf("WARNING: %s\n", fmt.Sprintf("Invalid Range '%s' (end position '%d' does not come after start '%d')", rangeHeader, end, offset))
						return
					}
					if end >= imageSize {
						end = imageSize - 1
					}
					length = end - offset + 1
				}
			}
		}
		contentRange := fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, imageSize)
		log.Printf("Range: %s will read at offset %d for %d bytes out of %d (Content-Range: %s)\n", rangeHeader, offset, length, imageSize, contentRange)
		w.Header().Set("Content-Range", contentRange)
		statusCode = http.StatusPartialContent
	} else {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", imageSize))
	}

	imageReader, err := imageservice.Download(imageClient, imageId).Extract()
	if err != nil {
		http.Error(w, fmt.Sprintf("Image not downloadable: %s", err), http.StatusNotFound)
		log.Printf("WARNING: %s\n", fmt.Sprintf("Image not downloadable: %s", err))
		return
	}

	// for HEAD reuests, we are done
	if r.Method == "HEAD" {
		w.WriteHeader(statusCode)
		return
	}

	var hashingImageReader io.Reader
	hashWriter := md5.New()
	if !noVerifyChecksum {
		hashingImageReader = io.TeeReader(imageReader, hashWriter)
	} else {
		hashingImageReader = imageReader
	}

	buf := make([]byte, bufferSize)
	var skipped, written, flushed int64
	if offset > 0 {
		// need to read offset bytes and throw them away
		skipReader := io.LimitReader(hashingImageReader, offset)
		skipped, err = io.CopyBuffer(ioutil.Discard, skipReader, buf)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error retrieving image %s from glance while skipping to offset %d: %s", imageId, offset, err), http.StatusBadGateway)
			log.Printf("WARNING: %s\n", fmt.Sprintf("Error retrieving image %s from glance while skipping to offset %d: %s", imageId, offset, err))
			return
		}
		log.Printf("Skipped %d bytes to wind to offset\n", skipped)
	}

	w.WriteHeader(statusCode)
	lengthReader := io.LimitReader(hashingImageReader, length)
	written, err = io.CopyBuffer(w, lengthReader, buf)
	if err != nil {
		log.Printf("WARNING: %s\n", fmt.Sprintf("Error transmitting image %s from glance to client: %s", imageId, err))
		return
	}
	log.Printf("Wrote %d bytes\n", written)

	if !noVerifyChecksum {
		flushed, err = io.CopyBuffer(ioutil.Discard, hashingImageReader, buf)
		if err != nil {
			log.Printf("WARNING: %s\n", fmt.Sprintf("Error retrieving image %s from glance: %s", imageId, err))
			return
		}
		log.Printf("Flushed %d bytes to wind to end for checksum verification\n", flushed)

		receivedMd5 := hex.EncodeToString(hashWriter.Sum(nil))
		if image.Checksum != receivedMd5 {
			bytesRead := skipped + written + flushed
			log.Printf("ERROR: checksum mismatch for image %s, glance reported checksum %s but we received md5 %s (read %d bytes out of %d)\n", imageId, image.Checksum, receivedMd5, bytesRead, imageSize)
		} else {
			log.Printf("Checksum verified md5{%s} for image %s", receivedMd5, imageId)
		}
	}

	return
}
