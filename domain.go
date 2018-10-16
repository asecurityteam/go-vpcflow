package vpcflow

import "time"

// LogFile is a structured representation of a VPC Flow
// log file. It should contain enough data that a consumer
// could fetch the file contents.
type LogFile struct {
	// Bucket is the S3 bucket in which the logs are stored.
	Bucket string
	// Key is "key" value used by ListObject and GetObject.
	Key string
	// Account is the AWS account ID extraced from the log path.
	Account string
	// Region is the AWS region extracted from the log path.
	Region string
	// Timestamp is the value from the log file name.
	Timestamp time.Time
	// FlowLogID is the key for the VPC log resource in AWS.
	FlowLogID string
	// Hash is the checksum value extracted from the log file name.
	Hash string
	// Size of the file containing the logs.
	Size int64
}

// BucketIterator scans an S3 bucket and converts AWS API responses
// to LogFile records.
type BucketIterator interface {
	// Iterate pushes the cursor one record forward such that
	// the current value is fetched when calling Current().
	// This method should return false after all records have
	// been iterated over or an error is encountered attempting
	// to fetch records.
	Iterate() bool
	// Get the current value of the iterator.
	Current() LogFile
	// Close cleans up any resources used by the iterator and
	// returns an error, if any, that caused iterations to stop.
	Close() error
}
