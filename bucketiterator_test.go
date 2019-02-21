package vpcflow

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestIterateOverOneResultWithMoreLeft(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()
	var queue = NewMockS3API(ctrl)
	var bi = BucketStateIterator{
		Bucket: "testbucket",
		Queue:  queue,
	}
	var key1 = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_0a1b2c3d.log.gz"
	var size1 = int64(100)
	var object1 = s3.Object{
		Key:  &key1,
		Size: &size1,
	}
	var key2 = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_0a1b2c3d.log.gz"
	var size2 = int64(100)
	var object2 = s3.Object{
		Key:  &key2,
		Size: &size2,
	}
	var output = s3.ListObjectsV2Output{
		Contents: []*s3.Object{&object1, &object2},
	}

	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output, nil)

	var result = bi.Iterate()
	assert.Equal(t, true, result)
	result = bi.Iterate()
	assert.Equal(t, true, result)
}

func TestIterateOverOneResultWithNoneLeft(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()
	var queue = NewMockS3API(ctrl)
	var bi = BucketStateIterator{
		Bucket: "testbucket",
		Queue:  queue,
	}
	var key = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_0a1b2c3d.log.gz"
	var size = int64(100)
	var object = s3.Object{
		Key:  &key,
		Size: &size,
	}
	var output = s3.ListObjectsV2Output{
		Contents: []*s3.Object{&object},
	}

	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output, nil)

	var result = bi.Iterate()
	assert.Equal(t, true, result)
	result = bi.Iterate()
	assert.Equal(t, false, result)
}

func TestIterateEmptyList(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()
	var queue = NewMockS3API(ctrl)
	var bi = BucketStateIterator{
		Bucket: "testbucket",
		Queue:  queue,
	}
	var output = s3.ListObjectsV2Output{
		Contents: []*s3.Object{},
	}

	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output, nil)

	var result = bi.Iterate()
	assert.Equal(t, false, result)

	_ = bi.Current() // shouldn't panic
}

func TestIterateWithOnePage(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()
	var queue = NewMockS3API(ctrl)
	var bi = BucketStateIterator{
		Bucket: "testbucket",
		Queue:  queue,
	}
	var key1 = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_0a1b2c3d.log.gz"
	var size1 = int64(100)
	var object1 = s3.Object{
		Key:  &key1,
		Size: &size1,
	}
	var key2 = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_0a1b2c3d.log.gz"
	var size2 = int64(100)
	var object2 = s3.Object{
		Key:  &key2,
		Size: &size2,
	}
	var key3 = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_0a1b2c3d.log.gz"
	var size3 = int64(100)
	var object3 = s3.Object{
		Key:  &key3,
		Size: &size3,
	}
	var output = s3.ListObjectsV2Output{
		Contents: []*s3.Object{&object1, &object2, &object3},
	}

	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output, nil)

	var counter = 0
	for bi.Iterate() {
		counter++
	}
	assert.Equal(t, 3, counter)
}

func TestIterateWithMoreThanOnePage(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()
	var queue = NewMockS3API(ctrl)
	var bi = BucketStateIterator{
		Bucket: "testbucket",
		Queue:  queue,
	}
	var key1 = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_1.log.gz"
	var size1 = int64(100)
	var object1 = s3.Object{
		Key:  &key1,
		Size: &size1,
	}
	var key2 = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_2.log.gz"
	var size2 = int64(100)
	var object2 = s3.Object{
		Key:  &key2,
		Size: &size2,
	}
	var key3 = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_3.log.gz"
	var size3 = int64(100)
	var object3 = s3.Object{
		Key:  &key3,
		Size: &size3,
	}
	var contents = []*s3.Object{&object1, &object2, &object3}
	var isTruncatedTrue = true
	var output1 = s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: &isTruncatedTrue,
	}
	var output2 = s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: &isTruncatedTrue,
	}
	var isTruncatedFalse = false
	var output3 = s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: &isTruncatedFalse,
	}

	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output1, nil)
	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output2, nil)
	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output3, nil)

	var counter = 0
	for bi.Iterate() {
		counter++
	}
	assert.Equal(t, 9, counter)
}

func TestIterateWithOnlyDirs(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()
	var queue = NewMockS3API(ctrl)
	var bi = BucketStateIterator{
		Bucket: "testbucket",
		Queue:  queue,
	}
	var key1 = "AWSLogs/123456789012/"
	var size1 = int64(0)
	var object1 = s3.Object{
		Key:  &key1,
		Size: &size1,
	}
	var key2 = "AWSLogs/123456789012/vpcflowlogs"
	var size2 = int64(0)
	var object2 = s3.Object{
		Key:  &key2,
		Size: &size2,
	}
	var contents = []*s3.Object{&object1, &object2}
	var isTruncatedTrue = true
	var output1 = s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: &isTruncatedTrue,
	}
	var isTruncatedFalse = false
	var output2 = s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: &isTruncatedFalse,
	}

	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output1, nil)
	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output2, nil)

	var result = bi.Iterate()
	assert.Equal(t, false, result)
}

func TestIterateWithFileSurroundedByPagesOfDirs(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()
	var queue = NewMockS3API(ctrl)
	var bi = BucketStateIterator{
		Bucket: "testbucket",
		Queue:  queue,
	}
	var fileKey = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_1.log.gz"
	var size1 = int64(100)
	var fileObject = s3.Object{
		Key:  &fileKey,
		Size: &size1,
	}
	var dirKey = "AWSLogs/123456789012/"
	var size2 = int64(0)
	var dirObject = s3.Object{
		Key:  &dirKey,
		Size: &size2,
	}
	var pageOne = []*s3.Object{&dirObject, &dirObject, &dirObject}
	var pageTwo = []*s3.Object{&dirObject, &fileObject, &dirObject}
	var pageThree = []*s3.Object{&dirObject, &dirObject, &dirObject}

	var isTruncatedTrue = true
	var output1 = s3.ListObjectsV2Output{
		Contents:    pageOne,
		IsTruncated: &isTruncatedTrue,
	}
	var output2 = s3.ListObjectsV2Output{
		Contents:    pageTwo,
		IsTruncated: &isTruncatedTrue,
	}
	var isTruncatedFalse = false
	var output3 = s3.ListObjectsV2Output{
		Contents:    pageThree,
		IsTruncated: &isTruncatedFalse,
	}

	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output1, nil)
	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output2, nil)
	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output3, nil)

	var counter = 0
	for bi.Iterate() {
		counter++
	}
	assert.Equal(t, 1, counter)
}

func TestParseLogFileMetadata(t *testing.T) {
	var key = "AWSLogs/123456789012/vpcflowlogs/us-west-2/2018/10/17/123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_0a1b2c3d.log.gz"
	var size = int64(100)
	var input = s3.Object{
		Key:  &key,
		Size: &size,
	}
	var expectedLogFile = LogFile{
		Bucket:    "testbucket",
		Key:       key,
		Account:   "123456789012",
		Region:    "us-west-2",
		Timestamp: time.Date(2018, 10, 17, 00, 30, 00, 00, time.UTC),
		FlowLogID: "fl-00123456789abcdef",
		Hash:      "0a1b2c3d",
		Size:      int64(100),
	}

	var logFile, err = parseLogFile(&input, "testbucket")

	assert.NoError(t, err, "there shouldn't be an error here")
	assert.Equal(t, expectedLogFile, logFile, "logFiles match")
}

func TestParseLogFileMetadataWithNoDirs(t *testing.T) {
	var key = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_0a1b2c3d.log.gz"
	var size = int64(100)
	var input = s3.Object{
		Key:  &key,
		Size: &size,
	}
	var expectedLogFile = LogFile{
		Bucket:    "testbucket",
		Key:       key,
		Account:   "123456789012",
		Region:    "us-west-2",
		Timestamp: time.Date(2018, 10, 17, 00, 30, 00, 00, time.UTC),
		FlowLogID: "fl-00123456789abcdef",
		Hash:      "0a1b2c3d",
		Size:      int64(100),
	}

	var logFile, err = parseLogFile(&input, "testbucket")

	assert.NoError(t, err, "error parsing key with no path")
	assert.Equal(t, expectedLogFile, logFile, "logFiles match")
}

func TestParseLogFileMetadataWithSillyTimestamp(t *testing.T) {
	var key = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181917T0030Z_0a1b2c3d.log.gz"
	var size = int64(100)
	var input = s3.Object{
		Key:  &key,
		Size: &size,
	}

	var _, err = parseLogFile(&input, "testbucket")

	assert.EqualError(t, err, "timestamp could not be parsed from log file name. parsing time \"20181917T0030Z\": month out of range")
}

func TestCurrent(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()
	var queue = NewMockS3API(ctrl)
	var bi = BucketStateIterator{
		Bucket: "testbucket",
		Queue:  queue,
	}
	var key = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_0a1b2c3d.log.gz"
	var size = int64(100)
	var object = s3.Object{
		Key:  &key,
		Size: &size,
	}
	var output = s3.ListObjectsV2Output{
		Contents: []*s3.Object{&object},
	}
	var expectedLogFile = LogFile{
		Bucket:    "testbucket",
		Key:       key,
		Account:   "123456789012",
		Region:    "us-west-2",
		Timestamp: time.Date(2018, 10, 17, 00, 30, 00, 00, time.UTC),
		FlowLogID: "fl-00123456789abcdef",
		Hash:      "0a1b2c3d",
		Size:      int64(100),
	}

	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output, nil)

	assert.Equal(t, true, bi.Iterate())
	assert.Equal(t, expectedLogFile, bi.Current())
}

func TestCurrentWithMultipleFiles(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()
	var queue = NewMockS3API(ctrl)
	var bi = BucketStateIterator{
		Bucket: "testbucket",
		Queue:  queue,
	}
	var key1 = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_11111.log.gz"
	var size1 = int64(100)
	var object1 = s3.Object{
		Key:  &key1,
		Size: &size1,
	}
	var expectedLogFile1 = LogFile{
		Bucket:    "testbucket",
		Key:       key1,
		Account:   "123456789012",
		Region:    "us-west-2",
		Timestamp: time.Date(2018, 10, 17, 00, 30, 00, 00, time.UTC),
		FlowLogID: "fl-00123456789abcdef",
		Hash:      "11111",
		Size:      int64(100),
	}
	var key2 = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_22222.log.gz"
	var size2 = int64(100)
	var object2 = s3.Object{
		Key:  &key2,
		Size: &size2,
	}
	var output = s3.ListObjectsV2Output{
		Contents: []*s3.Object{&object1, &object2},
	}
	var expectedLogFile2 = LogFile{
		Bucket:    "testbucket",
		Key:       key2,
		Account:   "123456789012",
		Region:    "us-west-2",
		Timestamp: time.Date(2018, 10, 17, 00, 30, 00, 00, time.UTC),
		FlowLogID: "fl-00123456789abcdef",
		Hash:      "22222",
		Size:      int64(100),
	}

	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output, nil)

	var results = make([]LogFile, 0, 2)
	for bi.Iterate() {
		results = append(results, bi.Current())
	}
	assert.Equal(t, expectedLogFile1, results[0])
	assert.Equal(t, expectedLogFile2, results[1])
}

func TestCloseWithoutErrors(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()
	var queue = NewMockS3API(ctrl)
	var bi = BucketStateIterator{
		Bucket: "testbucket",
		Queue:  queue,
	}
	var key = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_0a1b2c3d.log.gz"
	var size = int64(100)
	var object = s3.Object{
		Key:  &key,
		Size: &size,
	}
	var expectedLogFile = LogFile{
		Bucket:    "testbucket",
		Key:       key,
		Account:   "123456789012",
		Region:    "us-west-2",
		Timestamp: time.Date(2018, 10, 17, 00, 30, 00, 00, time.UTC),
		FlowLogID: "fl-00123456789abcdef",
		Hash:      "0a1b2c3d",
		Size:      int64(100),
	}
	var output = s3.ListObjectsV2Output{
		Contents: []*s3.Object{&object},
	}

	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output, nil)

	assert.Equal(t, true, bi.Iterate())
	assert.Equal(t, expectedLogFile, bi.Current())
	assert.Equal(t, nil, bi.Close())
}

func TestCloseWithErrorGettingLogFileMetadata(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()
	var queue = NewMockS3API(ctrl)
	var bi = BucketStateIterator{
		Bucket: "wrongbucket",
		Queue:  queue,
	}

	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(nil, errors.New(s3.ErrCodeNoSuchBucket))

	assert.Equal(t, false, bi.Iterate())
	assert.Equal(t, fmt.Errorf("error getting log file metadata. %s", s3.ErrCodeNoSuchBucket), bi.Close())
}

func TestCloseWithErrorParsingLogFileName(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()
	var queue = NewMockS3API(ctrl)
	var bi = BucketStateIterator{
		Bucket: "testbucket",
		Queue:  queue,
	}
	var key = "123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_2018notatimeT0030Z_0a1b2c3d.log.gz"
	var size = int64(100)
	var object = s3.Object{
		Key:  &key,
		Size: &size,
	}
	var output = s3.ListObjectsV2Output{
		Contents: []*s3.Object{&object},
	}

	queue.EXPECT().ListObjectsV2(gomock.Any()).Return(&output, nil)

	var errorString = "error parsing logfile name. timestamp could not be parsed from log file name. parsing time \"2018notatimeT0030Z\": month out of range"
	assert.Equal(t, false, bi.Iterate())
	assert.Equal(t, errors.New(errorString), bi.Close())
}

func BenchmarkParseLogFile(b *testing.B) {
	obj := &s3.Object{
		Key:  aws.String("AWSLogs/123456789012/vpcflowlogs/us-west-2/2018/10/17/123456789012_vpcflowlogs_us-west-2_fl-00123456789abcdef_20181017T0030Z_0a1b2c3d.log.gz"),
		Size: aws.Int64(100),
	}
	bucket := "vpcflow"
	b.ResetTimer()
	for n := 0; n < b.N; n = n + 1 {
		_, err := parseLogFile(obj, bucket)
		if err != nil {
			b.Fatal(err.Error())
		}
	}
}
