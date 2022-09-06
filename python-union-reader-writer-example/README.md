# Union reader-writer example in Python

## Run docker-compose
Run `docker-compose up` command in this directory.
The docker-compose.yaml file contains both writer and reader services that are written in Python.

Writer listens to writer-folder. It accepts json files in JWLF (it stands for [Json Well Log Format](https://jsonwelllogformat.org/)) 
and csv files which are converted eventually to JWLF. The files are then pushed to Union.

Reader, in turn, frequently query [Union](https://dev-dsp.southcentralus.cloudapp.azure.com) for the data,
pulls the logs in JWLF and saves them in reader-folder.

Both reader and writer works in the same data space that is identified by UNION_CLIENT, UNION_REGION, UNION_ASSET and UNION_FOLDER.
