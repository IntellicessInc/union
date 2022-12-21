# Union reader-writer example in Python

## Introduction
You can run the example using Docker (the easiest option) or through IDE.
In here, we describe Python IDE option.

## Run reader and writer with IDE
In order to run reader and writer Python scripts, you need to create virtual environment in this folder
and install packages enlisted in *requirements.txt*. Then, create *reader-folder* and *writer-folder* in this directory.
Before you run the apps, set reader and writer user accounts' credentials in *reader_local_config.ini* and *writer_local_config.ini*
that you can find in *src* folder.
Once this is done, you can run *src/reader.py* and *src/writer.py*.

When reader and writer are run locally (not in Docker), by default they use *src/reader_local_config.ini*
and *src/writer_local_config.ini* configurations.

You can find more details on how to test reader and writer example in *README.md* in the parent folder. 

## Configuration
In Docker version, you can provide configuration through environment variables. 
However, when running the example locally, usually you don't want to play around with environment variables.
That's why when you run the example locally, configuration can be changed modifying
*src/reader_local_config.ini* and *src/writer_local_config.ini*.
