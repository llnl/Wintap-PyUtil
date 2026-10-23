# Sleeper for Wintap Testing

## Install/setup Sleeper - based on Dave's email
* Built Ubuntu EC2 with nix/docker

* Run bootstrap (required only 1x for CDK setup in aws account)
    * Note: we did this in the first attempt, but this should be the right place for with the EC2 setup
* Deployed with: ./scripts/test/deployAll/buildDeployTest.sh $ID $VPC $SUBNETS
* Modified schema.template and added table with:
    `./scripts/deploy/addTable.sh $ID process`
* Uploaded all the process files to the existing system-test bucket, in a new path: wintap/process
* Sent an ingest request with python (ingest-request.py)

## Install from the README (using a docker instance)
* Start local docker container, used to run sleeper commands, with `./sleeper-install.sh develop`
* For example, to deploy EC2 and VPC
* Run bootstrap (required only 1x for CDK setup in aws account)
* Run `envoironment deploy TestEnvironment` to deploy EC2 and VPC in aws
* This EC2 is then used to deploy sleeper itself
* `environment connect` will open a shell on the remote EC2 instance via the local docker container


## Test Ingest with Process data
* Confirmed that parquet files need to be defined with every column as REQUIRED
* Replace existing nulls with something datatype appropriate
* Timestamps aren't supported, so convert to LONG/BIGINT
* Upload files to process in to the existing ingest bucket, using a new path
    * Simplest for now to create a new dir for each batch to process.
* Successful test with all 60 rolling/process files. ~900MB in, 300MB out!

## Operating Notes

### Add a new table
* Create a schema
* Pre-process data to eliminate nulls/convert datatypes
    * (table of Parquet -> Sleeper datatypes)
        * Ref: https://github.com/gchq/sleeper/blob/d470ffeef5929b59e27c8f367012afebed38593c/docs/03-schema.md
    * Set nulls to some value (create-sleeper-no-nulls.sh)
    * Add "required" flag to parquet files (add-required.py)
* Upload into ingest bucket
    * Use a new folder for each batch? Maybe use the wintap strategy of uploadPK sets?
* Ingest Data (ingest-request.py)
    * Monitor progess by watching either the dashboard or the CloudWatch Log Groups

### Modify a table: just don't for now...

## Things to try
* Try the ingest batcher
* Ingest network, both raw and rolling

### Useful AWS Console Locations
* CloudWatch 
    * Dashboard - customize for our tables
    * Log Group - (ID)-IngestTasks

# Questions
* When does a build actually need to be run? Will deploy pull jars as appropriate from… the docker image (?)
    * Only when source is changed?
* From the EC2 instance in the nix-shell, there is no "sleeper" command available? That's correct?
    * Yes, although the CLI has a separate build: https://github.com/gchq/sleeper/blob/develop/docs/11-dev-guide.md#sleeper-cli
* What would be use case for having multiple 'environments'?
* How do I run the admin client? Are there docs on it somewhere?
    * Do you use the CLI or do you build from source? There's a script to start it in the repository at
        `scripts/utility/adminClient.sh`
    * You can run that if you've built from source, or you can run it through the CLI with
        `sleeper deployment utility/adminClient.sh`
