# Union reader-writer example in Python

## Get started
Run `docker-compose up --build` command in this directory.

Then, just use sample data files and add them to the *writer-folder*.
After a few seconds, the writer will take the data, push them to the Union and remove the files from *writer-folder*.
Once this is done, the reader will notice the new data, pull and save them to *reader-folder*.

## How does it work?
The docker-compose.yaml file contains both writer and reader services that are written in Python.

Writer listens to *writer-folder*. It accepts JSON files with data in JWLF (it stands for [Json Well Log Format](https://jsonwelllogformat.org/)) 
and CSV files which are converted eventually to JWLF. The files are then pushed to Union.

Reader, in turn, frequently query [Union](https://dev-dsp.southcentralus.cloudapp.azure.com) for the data,
pulls the logs in JWLF and saves them in *reader-folder*.

Both reader and writer works in the same data space that is identified by the following parameters:
UNION_CLIENT, UNION_REGION, UNION_ASSET and UNION_FOLDER.

If you created your own users or roles different from the described on [Union - Get Started](https://dev-dsp.southcentralus.cloudapp.azure.com/#get-started),
feel free to change the parameters in docker-compose.yaml or .env file that docker-compose refers to, and rerun the apps.

**If you haven't already visited [Union - Get Started](https://dev-dsp.southcentralus.cloudapp.azure.com/#get-started),
we encourage you to do so to better understand the whole process.**
