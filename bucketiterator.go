package vpcflow

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// BucketStateIterator holds the current state of the iterator.
type BucketStateIterator struct {
	Bucket                string
	nextContinuationToken *string
	currentListPosition   int
	currentLogFileList    []LogFile
	isDone                bool
	Queue                 s3iface.S3API
	error                 error
}

// Iterate pushes the cursor one record forward such that
// the current value is fetched when calling Current().
// This method should return false after all records have
// been iterated over or an error is encountered attempting
// to fetch records.
func (iter *BucketStateIterator) Iterate() bool {

	for (iter.currentListPosition >= len(iter.currentLogFileList)-1) && !iter.isDone {
		// we're at the end of the currentLogFileList,
		// lets reset our list and list position and get the next page of results from the bucket
		iter.currentLogFileList = nil
		iter.currentListPosition = -1
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(iter.Bucket),
			ContinuationToken: iter.nextContinuationToken,
		}
		result, err := iter.Queue.ListObjectsV2(input)
		if err != nil {
			iter.error = fmt.Errorf("error getting log file metadata. %s", err)
			return false
		}
		for _, c := range result.Contents {
			if *c.Size == 0 {
				continue
			}

			logfile, err := parseLogFile(c, iter.Bucket)
			if err != nil {
				iter.error = fmt.Errorf("error parsing logfile name. %s", err)
				return false
			}
			iter.currentLogFileList = append(iter.currentLogFileList, logfile)
		}
		iter.nextContinuationToken = result.NextContinuationToken
		iter.isDone = !(result.IsTruncated != nil && *result.IsTruncated)
	}
	iter.currentListPosition++
	return !iter.isExausted()
}

// Current gets the current value of the iterator.
func (iter *BucketStateIterator) Current() LogFile {
	if iter.isExausted() {
		return LogFile{}
	}
	return iter.currentLogFileList[iter.currentListPosition]
}

// Close cleans up any resources used by the iterator and
// returns an error, if any, that caused iterations to stop.
func (iter BucketStateIterator) Close() error {
	iter.currentLogFileList = nil
	iter.currentListPosition = 0
	iter.isDone = true
	iter.nextContinuationToken = nil
	return iter.error
}

func (iter BucketStateIterator) isExausted() bool {
	return iter.isDone && iter.currentListPosition >= len(iter.currentLogFileList)
}

// parseLogFile takes in an s3.Object and converts it into and returns a vpcflow.LogFile.
func parseLogFile(content *s3.Object, bucket string) (LogFile, error) {
	var logfile LogFile
	logfile.Bucket = bucket
	// most of what we need can be extracted from the *s3.Object.Key, which has this format:
	// <aws_account_id>_vpcflowlogs_<region>_<flow_log_id>_<timestamp>_<hash>.log.gz
	// example: AWSLogs/123456789012/vpcflowlogs/us-west-2/2018/10/17/123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_0a1b2c3d.log.gz
	var key = *content.Key
	logfile.Key = key
	var keyElements = strings.Split(key, "/")
	var logFileName = keyElements[len(keyElements)-1]
	var logFileNameElements = strings.Split(logFileName, "_")
	logfile.Account = logFileNameElements[0]
	logfile.Region = logFileNameElements[2]
	var form = "20060102T1504Z"
	var timestamp, err = time.Parse(form, logFileNameElements[4])
	if err != nil {
		return LogFile{}, errors.New("timestamp could not be parsed from log file name. " + err.Error())
	}
	logfile.Timestamp = timestamp
	logfile.FlowLogID = logFileNameElements[3]
	logfile.Hash = strings.Split(logFileNameElements[5], ".")[0]

	logfile.Size = *content.Size
	return logfile, nil
}
