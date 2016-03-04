# s3-fast-upload-golang

## Usage
Requires the following to be set in env before running (in bash use export x="")
- AWS_ACCESS_KEY_ID=AKID1234567890
- AWS_SECRET_ACCESS_KEY=MY-SECRET-KEY

Flags/parameters:
bucket - s3 bucket to upload to
subfolder - subfolder in s3 bucket, can be blank
workers - number of upload workers to use (default 100)
region - aws region (default eu-west-1)
acl - s3 upload acl - use either private or public (default private)
sourcedir - source directory (default files/)
destdir - dest dir for uploaded files (on local box) (default files-uploaded/)


## Why
Allows uploading a large number of files to AWS S3 very quickly

This was created as we were making tens of thousands of files which needed to be uploaded to s3 as quickly as possible

This was attempted in Python and Bash with multiple threads but these versions were far too slow,
they managed around 3 uploads per second per thread,
depending upon the processor cores, clock speed and number of threads you use,
this can push tens, hundreds or even thousands of files per second.

## TODO
the program would probably benefit from file_channel in main() being
made larger, it essentially has no buffer at the moment, so the
get_file_list() function has to wait until a worker pulls from the channel
before it can push the next file on, maybe a max size of num_workers would
be a good start?
