/*******************************************************************************
 * Copyright (c) 2018 Genome Research Ltd.
 *
 * Author: Joshua C. Randall <jcrandall@alum.mit.edu>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	minio "github.com/minio/minio-go"
	"github.com/rackspace/gophercloud"
	"github.com/rackspace/gophercloud/openstack"
	"github.com/rackspace/gophercloud/openstack/compute/v2/images"
	imageservice "github.com/rackspace/gophercloud/openstack/imageservice/v2/images"
	log "github.com/sirupsen/logrus"
)

const bufferSize = 4 * 1024 * 1024

var computeClient *gophercloud.ServiceClient
var imageClient *gophercloud.ServiceClient

var minioBucket string
var minioPrefix string
var s3c *minio.Client
var buf []byte

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stderr)
	log.SetLevel(log.WarnLevel)
}

func main() {
	var err error
	var logLevelStr string
	flag.StringVar(&minioBucket, "minio-bucket", "glance-proxy", "S3 bucket to use for temporary image storage")
	flag.StringVar(&minioPrefix, "minio-prefix", "tmp", "Path prefix within bucket to use for temporary image storage")
	flag.StringVar(&logLevelStr, "log-level", "warn", "Log level (one of panic, fatal, error, warn, info, debug)")
	flag.Parse()

	logLevel, err := log.ParseLevel(logLevelStr)
	if err != nil {
		log.WithFields(log.Fields{
			"log_level": logLevelStr,
			"error":     err,
		}).Fatal("Error parsing log level")
	}
	log.SetLevel(logLevel)

	minioEndpoint := os.Getenv("MINIO_ENDPOINT")
	minioAccessKeyId := os.Getenv("MINIO_ACCESS_KEY_ID")
	minioSecretAccessKey := os.Getenv("MINIO_SECRET_ACCESS_KEY")

	if minioEndpoint == "" {
		log.Fatal("Please set MINIO_ENDPOINT environment variable to an S3 endpoint to use for temporary image storage")
		return
	}

	if minioAccessKeyId == "" {
		log.Print("WARNING: MINIO_ACCESS_KEY_ID was not set or empty, S3 authentication may fail")
	}

	if minioSecretAccessKey == "" {
		log.Print("WARNING: MINIO_SECRET_ACCESS_KEY was not set or empty, S3 authentication may fail")
	}

	// Use a secure connection
	minioSsl := true

	minioLog := log.WithFields(log.Fields{
		"endpoint":   minioEndpoint,
		"access_key": minioAccessKeyId,
	})
	// Initialize minio client object.
	s3c, err = minio.New(minioEndpoint, minioAccessKeyId, minioSecretAccessKey, minioSsl)
	if err != nil {
		minioLog.WithFields(log.Fields{
			"error": err,
		}).Fatal("Failed to initialize minio")
		return
	}

	exists, err := s3c.BucketExists(minioBucket)
	if err != nil {
		minioLog.WithFields(log.Fields{
			"error": err,
		}).Fatal("Error checking for existence of bucket")
		return
	}
	if !exists {
		minioLog.Fatal("Bucket does not exist")
		return
	}

	opts, err := openstack.AuthOptionsFromEnv()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Failed to get openstack auth options from environment")
		return
	}

	provider, err := openstack.AuthenticatedClient(opts)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Failed to authenticate to openstack")
		return
	}

	computeClient, err = openstack.NewComputeV2(provider, gophercloud.EndpointOpts{
		Name:   "nova",
		Region: "regionOne",
	})
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Failed to initialize openstack compute v2 provider")
		return
	}

	imageClient, err = openstack.NewImageServiceV2(provider, gophercloud.EndpointOpts{
		Region: "regionOne",
	})
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Failed to initialize openstack image service v2 provider")
		return
	}

	buf = make([]byte, bufferSize)

	router := mux.NewRouter()
	router.HandleFunc("/id/{image_id}", idImageHandler).Methods("GET", "HEAD", "DELETE")
	router.HandleFunc("/name/{image_name}", namedImageHandler).Methods("GET", "HEAD", "DELETE")

	srv := &http.Server{
		Addr:           "127.0.0.1:8080",
		Handler:        router,
		ReadTimeout:    300 * time.Second,
		WriteTimeout:   300 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	err = srv.ListenAndServe()
	log.WithFields(log.Fields{
		"error": err,
	}).Fatal("ListenAndServe error")
	return
}

func httpErrorLog(w http.ResponseWriter, error string, code int) {
	http.Error(w, error, code)
	log.WithFields(log.Fields{
		"code":  code,
		"error": error,
	}).Error("Returned http error")
	return
}

func namedImageHandler(w http.ResponseWriter, r *http.Request) {
	imageName := mux.Vars(r)["image_name"]
	log.WithFields(log.Fields{
		"method":    r.Method,
		"imageName": imageName,
	}).Info("Request for image by name")

	imageId, err := images.IDFromName(computeClient, imageName)
	if err != nil {
		httpErrorLog(w, fmt.Sprintf("Image name not found: %s", err), http.StatusNotFound)
		return
	}

	handleImage(w, r, imageId)
}

func idImageHandler(w http.ResponseWriter, r *http.Request) {
	imageId := mux.Vars(r)["image_id"]
	log.WithFields(log.Fields{
		"method":   r.Method,
		"image_id": imageId,
	}).Info("Request for image by id")

	handleImage(w, r, imageId)
}

func handleImage(w http.ResponseWriter, r *http.Request, imageId string) {
	switch m := r.Method; m {
	case "GET":
		getImage(w, r, imageId)
	case "HEAD":
		getImage(w, r, imageId)
	case "DELETE":
		deleteImage(w, r, imageId)
	}
}

func deleteImage(w http.ResponseWriter, r *http.Request, imageId string) {
	var err error
	log.WithFields(log.Fields{
		"image_id": imageId,
	}).Info("Deleting image from S3")
	err = deleteImageFromS3(imageId)
	if err != nil {
		httpErrorLog(w, fmt.Sprintf("Failed to delete image %s from S3: %s", imageId, err), http.StatusBadGateway)
		return
	}
	// log.WithFields(log.Fields{
	// 	"image_id": imageId,
	// }).Info("Deleting image from Glance")
	// err = deleteImageFromGlance(imageId)
	// if err != nil {
	// 	httpErrorLog(w, fmt.Sprintf("Failed to delete image %s from Glance: %s", imageId, err), http.StatusBadGateway)
	// 	return
	// }
	w.WriteHeader(http.StatusNoContent)
	return
}

func deleteImageFromGlance(imageId string) error {
	res := imageservice.Delete(imageClient, imageId)
	if res.Err == nil {
		log.WithFields(log.Fields{
			"image_id": imageId,
		}).Info("Removed image from Glance")
	}
	return res.Err
}

func deleteImageFromS3(imageId string) (err error) {
	imagePath := path.Join(minioPrefix, imageId)
	err = s3c.RemoveObject(minioBucket, imagePath)
	if err == nil {
		log.WithFields(log.Fields{
			"bucket": minioBucket,
			"object": imagePath,
		}).Info("Removed image from S3")
	}
	return
}

func getImage(w http.ResponseWriter, r *http.Request, imageId string) {
	w.Header().Set("Accept-Ranges", "bytes")

	image, err := imageservice.Get(imageClient, imageId).Extract()
	if err != nil {
		httpErrorLog(w, fmt.Sprintf("Image not found: %s", err), http.StatusNotFound)
		return
	}

	if image.Status != "active" {
		httpErrorLog(w, "Image not active", http.StatusNotFound)
		return
	}

	imageSize := int64(image.SizeBytes)
	if imageSize <= 0 {
		log.WithFields(log.Fields{
			"image_size": imageSize,
		}).Warning("Returning empty content for image with Size <= 0")
		return
	}

	if image.Checksum == "" {
		log.Info("Image has no checksum in glance")
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
			httpErrorLog(w, fmt.Sprintf("Invalid Range (only byte ranges are supported): %s", rangeHeader), http.StatusRequestedRangeNotSatisfiable)
			return
		}
		byteRangeSpec := strings.TrimSpace(rangeHeader[len(bytesUnitEq):])
		// support single range only
		if strings.Contains(byteRangeSpec, ",") {
			httpErrorLog(w, fmt.Sprintf("Invalid Range (multiple byte ranges are not supported): %s", rangeHeader), http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if byteRangeSpec != "" {
			i := strings.Index(byteRangeSpec, "-")
			if i < 0 {
				httpErrorLog(w, fmt.Sprintf("Invalid Range (missing '-'): %s", rangeHeader), http.StatusRequestedRangeNotSatisfiable)
				return
			}
			startSpec := strings.TrimSpace(byteRangeSpec[:i])
			endSpec := strings.TrimSpace(byteRangeSpec[i+1:])
			if startSpec == "" {
				// ranges such as '-300' mean to start 300 from the end
				end, err := strconv.ParseInt(endSpec, 10, 64)
				if err != nil {
					httpErrorLog(w, fmt.Sprintf("Invalid Range '%s' (end position '%s' could not be parsed as an integer): %s", rangeHeader, endSpec, err), http.StatusRequestedRangeNotSatisfiable)
					return
				}
				if end < 0 {
					httpErrorLog(w, fmt.Sprintf("Invalid Range '%s' (end position '%s' was negative): %s", rangeHeader, endSpec, err), http.StatusRequestedRangeNotSatisfiable)
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
					log.WithFields(log.Fields{
						"range":      rangeHeader,
						"start_spec": startSpec,
						"error":      err,
					}).Warning("Invalid Range (start specification could not be parsed as an integer)")
					return
				}
				if offset < 0 {
					httpErrorLog(w, fmt.Sprintf("Invalid Range '%s' (start position '%s' was negative): %s", rangeHeader, startSpec, err), http.StatusRequestedRangeNotSatisfiable)
					return
				}
				if offset >= imageSize {
					httpErrorLog(w, fmt.Sprintf("Range '%s' has a start position '%s' beyond extent of image (size %d)", rangeHeader, startSpec, imageSize), http.StatusRequestedRangeNotSatisfiable)
					return
				}
				if endSpec == "" {
					// no end specified, range extends to imageSize
					length = imageSize - offset
				} else {
					end, err := strconv.ParseInt(endSpec, 10, 64)
					if err != nil {
						httpErrorLog(w, fmt.Sprintf("Invalid Range '%s' (end position '%s' could not be parsed as an integer): %s", rangeHeader, endSpec, err), http.StatusRequestedRangeNotSatisfiable)
						return
					}
					if end <= offset {
						httpErrorLog(w, fmt.Sprintf("Invalid Range '%s' (end position '%d' does not come after start '%d')", rangeHeader, end, offset), http.StatusRequestedRangeNotSatisfiable)
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
		log.WithFields(log.Fields{
			"range":         rangeHeader,
			"offset":        offset,
			"length":        length,
			"image_size":    imageSize,
			"content_range": contentRange,
		}).Info("Range request")
		w.Header().Set("Content-Range", contentRange)
		statusCode = http.StatusPartialContent
	} else {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", imageSize))
	}

	// check for image in S3
	imagePath := path.Join(minioPrefix, imageId)
	imageObjInfo, err := s3c.StatObject(minioBucket, imagePath, minio.StatObjectOptions{})
	if err != nil {
		log.WithFields(log.Fields{
			"bucket": minioBucket,
			"object": imagePath,
			"error":  err,
		}).Info("Failed to stat S3 object, it probably does not exist")
		// prepare to download image from glance
		imageReader, err := imageservice.Download(imageClient, imageId).Extract()
		if err != nil {
			httpErrorLog(w, fmt.Sprintf("Image not downloadable: %s", err), http.StatusNotFound)
			return
		}
		// prepare to verify checksum while transferring to s3
		glanceHashWriter := md5.New()
		hashingImageReader := io.TeeReader(imageReader, glanceHashWriter)

		log.WithFields(log.Fields{
			"bucket": minioBucket,
			"object": imagePath,
			"error":  err,
		}).Info("Attempting to upload image from glance to S3")
		written, err := s3c.PutObject(minioBucket, imagePath, hashingImageReader, imageSize, minio.PutObjectOptions{ContentType: "application/x-raw-disk-image"})
		if err != nil {
			httpErrorLog(w, fmt.Sprintf("Failed to transfer image %s from glance to S3 %s/%s: %s\n", imageId, minioBucket, imagePath, err), http.StatusBadGateway)
			return
		}

		if imageSize != written {
			httpErrorLog(w, fmt.Sprintf("Image %s is %d bytes but only %d bytes were written to S3 %s/%s\n", imageId, imageSize, written, minioBucket, imagePath), http.StatusBadGateway)
			return
		}

		glanceMd5 := hex.EncodeToString(glanceHashWriter.Sum(nil))
		if image.Checksum != "" {
			if image.Checksum != glanceMd5 {
				log.WithFields(log.Fields{
					"image_id":                 imageId,
					"image_checksum":           image.Checksum,
					"received_from_glance_md5": glanceMd5,
				}).Error("Checksum mismatch for image between image checksum and MD5 of data received from glance")
			} else {
				log.WithFields(log.Fields{
					"image_id":                 imageId,
					"image_checksum":           image.Checksum,
					"received_from_glance_md5": glanceMd5,
				}).Info("Checksum verified for image")
			}
		}

		// verify upload and get object info
		imageObjInfo, err = s3c.StatObject(minioBucket, imagePath, minio.StatObjectOptions{})
		if err != nil {
			httpErrorLog(w, fmt.Sprintf("Image just transferred to S3 is not accessible %s/%s: %s\n", minioBucket, imagePath, err), http.StatusBadGateway)
			return
		}

		// as images are likely to be large, the ETag will not be the MD5 of the whole object but rather some multipart hash tree scheme.
		// download the image we just uploaded to verify md5
		imageObject, err := s3c.GetObject(minioBucket, imagePath, minio.GetObjectOptions{})
		if err != nil {
			httpErrorLog(w, fmt.Sprintf("Failed to get object %s/%s from S3: %s", minioBucket, imagePath, err), http.StatusBadGateway)
			return
		}
		s3Md5Writer := md5.New()
		verified, err := io.CopyBuffer(s3Md5Writer, imageObject, buf)
		if err != nil {
			httpErrorLog(w, fmt.Sprintf("Failed to download image %s/%s from S3 to verify md5: %s", minioBucket, imagePath, err), http.StatusBadGateway)
			// delete from S3
			err = deleteImageFromS3(imageId)
			if err != nil {
				log.WithFields(log.Fields{
					"bucket": minioBucket,
					"object": imagePath,
					"error":  err,
				}).Error("Failed to remove S3 image object")
			}
			return
		}
		if verified != imageSize {
			httpErrorLog(w, fmt.Sprintf("Image %s/%s is %d bytes but received %d bytes from S3 for verification: %s", minioBucket, imagePath, imageSize, verified), http.StatusBadGateway)
			// delete from S3
			err = deleteImageFromS3(imageId)
			if err != nil {
				log.WithFields(log.Fields{
					"bucket": minioBucket,
					"object": imagePath,
					"error":  err,
				}).Error("Failed to remove S3 image object")
			}
			return
		}
		s3Md5 := hex.EncodeToString(s3Md5Writer.Sum(nil))
		if s3Md5 != glanceMd5 {
			httpErrorLog(w, fmt.Sprintf("Image %s/%s checksum mismatch, uploaded %s to S3 but received %s back\n", minioBucket, imagePath, glanceMd5, s3Md5), http.StatusBadGateway)
			// delete from S3
			err = deleteImageFromS3(imageId)
			if err != nil {
				log.WithFields(log.Fields{
					"bucket": minioBucket,
					"object": imagePath,
					"error":  err,
				}).Error("Failed to remove S3 image object")
			}
			return
		} else {
			log.Printf("Checksum verified md5{%s} for image %s\n", glanceMd5, imageId)
		}

	}

	if imageSize != imageObjInfo.Size {
		httpErrorLog(w, fmt.Sprintf("Size mismatch, glance reports size %d but S3 has size %d", imageSize, imageObjInfo.Size), http.StatusBadGateway)
		// delete from S3
		err = deleteImageFromS3(imageId)
		if err != nil {
			log.WithFields(log.Fields{
				"bucket": minioBucket,
				"object": imagePath,
				"error":  err,
			}).Error("Failed to remove S3 image object")
		}
	}

	// FIXME this check does not work because large files in S3 don't have simple MD5 ETag
	// verify md5 in S3 is the same as what glance reports
	// if image.Checksum != "" && image.Checksum != imageObjInfo.ETag {
	// 	httpErrorLog(w, fmt.Sprintf("Checksum mismatch, glance reports Checksum %s but S3 has ETag %s", image.Checksum, imageObjInfo.ETag), http.StatusBadGateway)
	// 	return
	// }

	// for HEAD reuests, we are done
	if r.Method == "HEAD" {
		w.WriteHeader(statusCode)
		return
	}

	// Prepare to read from S3
	imageObject, err := s3c.GetObject(minioBucket, imagePath, minio.GetObjectOptions{})
	if err != nil {
		httpErrorLog(w, fmt.Sprintf("Failed to get object %s/%s from S3: %s", minioBucket, imagePath, err), http.StatusBadGateway)
		return
	}

	var imageReader io.Reader
	if offset > 0 {
		imageReader = io.NewSectionReader(imageObject, offset, length)
	} else {
		imageReader = imageObject
	}

	w.WriteHeader(statusCode)
	written, err := io.CopyBuffer(w, imageReader, buf)
	if err != nil {
		log.WithFields(log.Fields{
			"image_id": imageId,
			"error":    err,
		}).Warning("Error transmitting image from S3 to client")
		return
	}
	log.WithFields(log.Fields{
		"bytes_written": written,
		"offset":        offset,
		"length":        length,
	}).Info("Finished transmitting image from S3 to client")

	return
}
