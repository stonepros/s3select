# s3select
  The s3select is another S3 request, that enables the client to push down an SQL statement(according to [CEPH specs](https://docs.ceph.com/en/latest/radosgw/s3select/#)) into CEPH storage.
  <br />The s3select is an implementation of a **push-down paradigm**.  That paradigm is about moving(“pushing”) the operation close to the data.
  <br />It's contrary to what is commonly done, i.e. moving the data to the “place” of operation.
  <br />In a big-data ecosystem, it's a big difference. 
  <br />In order to execute ***“select sum( x + y) from s3object where a + b > c”*** 
  <br />It needs to fetch the entire object to the client side, and only then execute the operation by some analytic application,
  <br />With push-down operation (s3-select) the entire operation is executed on the server side only the result is returned to the client side.

## Analyzing huge amount of "cold" / "warm" data without moving or converting  
The s3-storage is reliable, efficient, cheap, and already contains a huge amount of objects, with many **CSV**, **JSON**, and **Parquet** objects, and these objects contain a huge amount of data to analyze.
<br />An ETL may convert these objects into Parquet and then run queries on these converted objects or may run these queries without converting.
<br />Either way it comes with an expensive price, downloading all of these objects for the analytic application.
<br />The s3select-engine that resides within the s3-storage can do these jobs for many use cases, saving time and resources. 

## The s3select engine stands by itself 

The engine resides on a dedicated GitHub repo, and it is also capable to execute SQL statements on standard input or files residing on a local file system.
Users may clone and build this repo, and execute various SQL statements as CLI.

## a docker image containing a development environment
An immediate way for a quick start is available using the following container.
<br />That container already contains the cloned repo, enabling code review and modification.

### boot the container 
sudo docker run -w /s3select -it galsl/ubunto_arrow_parquet_s3select:dev
### running the G-tests suite, it contains hundreads of queries. 
./test/s3select_test
### running an SQL statement on standard input 
./example/s3select_example, is a small demo app, enables to run queries on local file or standard-input.
for one example, the following runs the engine on standard output
<br />seq 1 1000 | ./example/s3select_example -q 'select count(0) from *stdin*;'
### running an SQL statement on local file 
./example/s3select_example -q 'select count(0) from /full/path/file_name;'
