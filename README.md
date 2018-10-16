# go-vpc #

**A library for consuming AWS VPC logs**

## Overview ##

AWS VPC logs are a data source by which a team can detect anomalies in connection patterns, use of non-standard ports, or even view the interconnections of systems. To assist in the consumption and analysis of these logs, go-vpc provides the following feature set:

* extract VPC logs from an S3 bucket
* filter out logs of interest based on log metadata
* perform compactions on the data resulting in a digest for a particular network interface

## Usage ##

In order to get started, your AWS account should be configured to [publish VPC flow logs to S3](https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs-s3.html)

## Contributing ##

### License ###

This project is licensed under Apache 2.0. See LICENSE.txt for details.

### Contributing Agreement ###

Atlassian requires signing a contributor's agreement before we can accept a
patch. If you are an individual you can fill out the
[individual CLA](https://na2.docusign.net/Member/PowerFormSigning.aspx?PowerFormId=3f94fbdc-2fbe-46ac-b14c-5d152700ae5d).
If you are contributing on behalf of your company then please fill out the
[corporate CLA](https://na2.docusign.net/Member/PowerFormSigning.aspx?PowerFormId=e1c17c66-ca4d-4aab-a953-2c231af4a20b).