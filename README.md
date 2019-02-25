<a id="markdown-go-vpc---tools-for-working-with-aws-vpc-flow-logs" name="go-vpc---tools-for-working-with-aws-vpc-flow-logs"></a>
# go-vpc - Tools for working with AWS VPC Flow Logs #

*Status: Incubation*

<!-- TOC -->

- [go-vpc - Tools for working with AWS VPC Flow Logs](#go-vpc---tools-for-working-with-aws-vpc-flow-logs)
    - [Overview](#overview)
    - [Usage](#usage)
        - [Iterating over bucket objects](#iterating-over-bucket-objects)
        - [Filtering bucket objects](#filtering-bucket-objects)
        - [Reading Log File contents](#reading-log-file-contents)
        - [Digesting multiple log files](#digesting-multiple-log-files)
        - [Converting to DOT](#converting-to-dot)
    - [Contributing](#contributing)
        - [License](#license)
        - [Contributing Agreement](#contributing-agreement)

<!-- /TOC -->

<a id="markdown-overview" name="overview"></a>
## Overview ##

AWS Flow Logs are a data source by which a team can detect anomalies in connection patterns, use of non-standard ports, or even view the interconnections of systems. To assist in the consumption and analysis of these logs, go-vpc provides the following feature set:

* extract flow logs from an S3 bucket
* filter out logs of interest based on log metadata
* perform compactions on the data resulting in a digest for a particular network interface
* convert the AWS VPC log file format into a DOT graph to easily visualize nodes and edges in a network

<a id="markdown-usage" name="usage"></a>
## Usage ##

In order to get started, your AWS account should be configured to [publish flow logs to S3](https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs-s3.html)

This project provides an iterator interface to interact with the objects in an S3 bucket as well as chains together `io.ReadCloser` streams to get access to bucket data.

<a id="markdown-iterating-over-bucket-objects" name="iterating-over-bucket-objects"></a>
### Iterating over bucket objects ###

To iterate over the objects in an S3 bucket, use the `vpcflow.BucketStateIterator`. This will iterate over objects, and
provide various metadata about the log files.

```
bucketIter := &vpcflow.BucketStateIterator{
	Bucket: bucket,
	Queue:  client,
}
for bucketIter.Iterate() {
	logFile := bucketIter.Current()
	...
}
err := bucketIter.Close()
// check error
```

To focus on a subset of data in your bucket, you can apply a prefix fileter to the iterator. Only objects with this prefix will be returned.
By default, all objects in the bucket will be iterated over.

```
bucketIter := &vpcflow.BucketStateIterator{
	Bucket: bucket,
	Queue:  client,
	Prefix: "AWSLogs/123456789123/vpcflowlogs/us-west-2/2018/10/15",
}
```

<a id="markdown-filtering-bucket-objects" name="filtering-bucket-objects"></a>
### Filtering bucket objects ###

It's probable that not all object in the S3 bucket will be of interest.
A Log File decorator is provided to filter out log files which may not
be relevant.

```
bucketIter := &vpcflow.BucketStateIterator{
	Bucket: bucket,
	Queue:  client,
}
filterIter := &vpcflow.BucketFilter{
	BucketIterator: bucketIter,
	Filter: vpcflow.LogFileTimeFilter{
		Start: start,
		End:   stop,
	},
}
for filterIter.Iterate() {
	logFile := filterIter.Current()
	...
}
err := bucketIter.Close()
```
<a id="markdown-reading-log-file-contents" name="reading-log-file-contents"></a>
### Reading Log File contents ###

To consume the contents of the log files, `vpcflow.BucketIteratorReader`
is provided to convert the iterator into a consumable stream of Log File
contents. The reader should be initialized with an iterator from which to
consume, and a `FetchPolicy`. The built-in `FetchPolicy` produces a `vpcflow.FileManager` which will eagerly fetch file contents before they are needed.

```
bucketIter := &vpcflow.BucketStateIterator{
	Bucket: bucket,
	Queue:  client,
}
filterIter := &vpcflow.BucketFilter{
	BucketIterator: bucketIter,
	Filter: vpcflow.LogFileTimeFilter{
		Start: start,
		End:   stop,
	},
}
readerIter := &vpcflow.BucketIteratorReader{
	BucketIterator: filterIter,
	FetchPolicy:    vpcflow.NewPrefetchPolicy(client, maxBytes, concurrency),
}
```

<a id="markdown-digesting-multiple-log-files" name="digesting-multiple-log-files"></a>
### Digesting multiple log files ###

To compact log files, use the `vpcflow.Digester` component. This will aggregate all of the traffic between two nodes into one log line. Note,
this will also result in a loss of some data, specifically, the ephemeral port.

```
d := &vpcflow.ReaderDigester{Reader: readerIter}
reader, err := d.Digest()
```

To illustrate, the following lines:

```
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20641 80 6 20 1000 1418530010 1418530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20541 80 6 20 1000 1518530010 1518530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20441 80 6 20 1000 1618530010 1618530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20341 80 6 20 1000 1718530010 1718530070 REJECT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20241 80 6 20 1000 1818530010 1818530070 ACCEPT OK
```
would become:

```
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 20 1000 1418530010 1818530070 REJECT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 80 8000 1418530010 1818530070 ACCEPT OK
```

<a id="markdown-converting-to-dot" name="converting-to-dot"></a>
### Converting to DOT ###

The `vpcflow.DOTConverter` converts an AWS VPC Flow log file format into
a DOT graph representation.  This is useful for visualizing the nodes and
edges in a network graph.

```
d := &vpcflow.ReaderDigester{Reader: readerIter}
digested, _ := d.Digest()
converted, err := vpcflow.DOTConvter(digested)
```

<a id="markdown-contributing" name="contributing"></a>
## Contributing ##

<a id="markdown-license" name="license"></a>
### License ###

This project is licensed under Apache 2.0. See LICENSE.txt for details.

<a id="markdown-contributing-agreement" name="contributing-agreement"></a>
### Contributing Agreement ###

Atlassian requires signing a contributor's agreement before we can accept a
patch. If you are an individual you can fill out the
[individual CLA](https://na2.docusign.net/Member/PowerFormSigning.aspx?PowerFormId=3f94fbdc-2fbe-46ac-b14c-5d152700ae5d).
If you are contributing on behalf of your company then please fill out the
[corporate CLA](https://na2.docusign.net/Member/PowerFormSigning.aspx?PowerFormId=e1c17c66-ca4d-4aab-a953-2c231af4a20b).
