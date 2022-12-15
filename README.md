# Union reader-writer example

## Introduction
You can run the example using Docker (the easiest option) or through IDE.
In here, we describe Docker option.
If you want to run the example in IDE, go to *README.md* to the folder with example in the language of your choice.

## Prerequisite
If you haven't Docker with docker-compose already installed, go to [Official "Get Docker" webpage](https://docs.docker.com/get-docker/) 
and download Docker Desktop. Also, you need to have user and writer accounts already created. If you haven't done it yet, please go through [this tutorial](https://theunion.cloud/#get-started).

## Get started with Docker
Fill writer's and reader's credentials in *.env* file (these are AUTH_WRITER_USER_USERNAME, AUTH_WRITER_USER_PASSWORD, AUTH_READER_USER_USERNAME, AUTH_READER_USER_PASSWORD properties to set),
and run `docker-compose up --build` command **in one of reader-writer example directory in your language of choice**.

Then use sample data files and add them to the *writer-folder*.
After a few seconds, the writer will take the data, push them to the Union and remove the files from *writer-folder*.
Once this is done, the reader will notice the new data, pull and save them to *reader-folder*.

## How does it work?
docker-compose.yaml file contains both writer and reader services.

Writer listens to *writer-folder*. It accepts JSON files with data in JWLF (it stands for [Json Well Log Format](https://jsonwelllogformat.org/)) 
and CSV files which are converted eventually to JWLF. The files are then pushed to Union.

Reader, in turn, frequently query [Union](https://theunion.cloud) for the data,
pulls the logs in JWLF and saves them in *reader-folder*.

Both reader and writer works in the same data space that is identified by the following parameters:
UNION_CLIENT, UNION_REGION, UNION_ASSET and UNION_FOLDER.

### Base64 encoded binaries sent as part of JWLF
It is possible to send binary files using JWLF too. To do that, you can encode binary files into base64 format and send them in the shape of string data in JWLF log.
When receiving and parsing that data you just need to decode it and use wherever you need.

If you want to test this scenerio, you may copy *base64-encoded-binaries-example* folder from sample data to the *writer-folder*.

The writer app looks for csv file with metadata and encodes all other files inside of this folder.
Once this is done, it creates JWLF log and sends it to the Union. On the other side, the reader app checks for new data including the base64 encoded. When it receives base64 encoded data, it decodes the binary files and saves them into the folder named after the csv file with metadata.

## Test data
For test purpose, you can use the data from [JWLF The Volve data set](https://jsonwelllogformat.org/Volve/Well_logs/).

## Querying data by time range
You can query data using *sinceTimestamp* and *tillTimestamp* query params. This way, you're capable of getting continuous real-time stream of new data.

Also, it might be tempting to set *tillTimestamp* to the current UTC timestamp. However, such action may result in data lost if you won't run this query again after some time.
This is because current timestamp on Union servers may differ from your current timestamp. Moreover, even if the servers were ideally synchronized, already saved data might not be visible yet.

That's what stable timestamp is for.

In our example *tillTimestamp* is evaluated based on stable data timestamp retrieved from Union.
This value is returned with all JWLF data query responses.
The stable data timestamp tells you what till timestamp you can use to be sure that the same query if repeated, will yield the same data unless any of the logs haven't been removed.
In other words, if you run many queries with the same *sinceTimestamp* and *tillTimestamp* equal to *stableTimestamp + 1* maximally, then each time you get the same data results.
Why is there *stableTimestamp + 1*?
That's because ***tillTimestamp* is exclusive**.
On the other hand, ***sinceTimestamp* is inclusive**.
If you use *tillTimestamp* equal to the current timestamp, the returned results may not be the all data that will eventually get saved in Union within the timestamp range you request for.
So, if the query is repeated a few hundred milliseconds later, the response may contain more data.
**Note that listening to data in Union this way, may cause data loss. Usually, this is why it's very important to use *stableTimestamp + 1* as *tillTimestamp* value**.


## What is the date time format that Union accepts?
In compliance with the [Json Well Log Format](https://jsonwelllogformat.org/), Union accepts date time in [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html).
The following are available formats - ISO Local Date, ISO Local Date and Time and ISO Zoned Date Time.
- **yyyy-MM-DD**
- **yyyy-MM-DDTHH:mm:ss**
- **yyyy-MM-DDTHH:mm:ss.S**
- **yyyy-MM-DDTHH:mm:ss+HH:MM:ss**

Here are some examples of date time values you can use:
- 2011-12-23
- 2011-12-23T10:15:30
- 2011-12-23T10:15:30.821
- 2011-12-23T10:15:30+01:00
- 2011-12-23T10:15:30Z (the *'Z'* letter means there is no Zone offset so it may be omitted)
