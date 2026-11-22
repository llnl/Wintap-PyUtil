from sleeper.sleeper import SleeperClient

# Assumes AWS keys are defined as either default in ~/.aws/credentials or set in the environment
# Bucket can be a folder or a single file
# Monitor progress in CloudWatch Log Group: [ID]-IngestTasks

# TODO
# Add args to the script
# Expand to handle upload of local files, then submit a request

table_name = "process"
bucket = "sleeper-wintapsleeper-system-test-ingest/wintap-acme/process2/"

# Create Sleeper instance with base name of install
my_sleeper = SleeperClient("wintapsleeper")
# for file in files:
my_sleeper.ingest_parquet_files_from_s3(table_name, [bucket])
