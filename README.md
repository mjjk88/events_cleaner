# The aim of the script
The package is an example of the ETL script, which can be part of the data processing pipeline.
It downloads files (that contain web events in JSON lines format) from the AWS S3 bucket and cleans them (remove broken JSONs and validate basic business rules).

This is a demonstration of how the basic cleansing data could be automated.
The current version of the script does not push the result to the database but produces cleaned-up files in JSON lines format.

## Motivation
Providing clean, valuable, consistent, and reliable data speeds up analysis work and helped the business make the right decisions.
The cleansing and validation data process allows for storing ready to uses data in the data warehouse.  
It is also an additional mechanism that alerts about bad data quality before pushing it to the data warehouse. 
Storing broken records in error files can be used for data monitoring purposes. It could help fix the problems within the source.   


# How does it work
Script downloads all files from directory (`--remote_dir` param)
in S3 bucket (`--bucket` param) into local directory `input` (inside `--local_dir` param).
*See `FilesHandler` class to understand the details about directory structure.*

Script cleans files downloaded in previous step and saves valid events to `output` directory (inside `--local_dir` param).  
All errors (invalid records) are logged in `errors` directory for debug and error monitoring purposes.
Thanks to that, it is much easier to root cause issues in production and provide proper explanation of production events. 

Files in `output` directory are ready to be pushed to database.

## Cleansing mechanism
I. The script rejects the lines which cannot be parsed (incorrect JSON format) and saves them to the error log files.

II. For every field in passable JSON line, the script applies one or many validators:
  - field is not a blank string,
  - field matches specific regex (e.g. UUID format, date format, email format)
Invalid records filer out in this step are also appended to the error log file. 

This is just an example of the potential cleansing of the data. 
More validation rules could be added depending on the customer's needs. 

# Pre-requirements 
- `python >= 3.7` installed in the system
- aws account configured

# How to install
This package is managed by `setuptool`.
To install dependencies use:
 
```shell
cd events_cleaner
python3 -m venv venv
source venv/bin/activate
python setup.py install
```

# How to run
Activate `venv` if not already active:
```shell
cd events_cleaner
source venv/bin/activate
```

Run python command line script, e.g.:
```shell
cd events_cleaner
python3 main.py --bucket 'my-bucket' --remote_dir 'events/' --local_dir .
```

# Improvements to consider 
a) For the current size of files (~300MB), processing the whole file at once works even on smaller machines/EC2 instances. If the amount of the events in a file increase then:
- processing of events in batches can be a solution (reading e.g. 10k lines at once)
- or, we can "just" buy a bigger instance (m6g.8xlarge should buy us a lot of time with 128GB of memory)

b) There is no resuming functionality.
We could improve it and in case of failure do not restart the flow from scratch but somehow resume it - process only the files that have not yet been processed or not fully finished.  

c) We could also process 1 file at once instead of downloading entire directory in s3. For now, it does not look like an issue.