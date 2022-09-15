# Union reader-writer example in Java

## Introduction
You can run the example using Docker (the easiest option) or through IDE.
In here, we describe Java IDE option.

## Run reader and writer with IDE
If your IDE supports maven (like e.g. Intellij Community), you can run com.intellicess.union.java_union_reader_writer_example.Reader 
and com.intellicess.union.java_union_reader_writer_example.Writer directly in IDE.

If it doesn't, you need to run `./mvnw clean install` in root folder of this project.
This command will create jar in *target* folder.

In order to run writer and reader you need to run the following commands from main folder:
- `java -cp ./target/java-union-reader-writer-example.jar com.intellicess.union.java_union_reader_writer_example.Reader`
- `java -cp ./target/java-union-reader-writer-example.jar com.intellicess.union.java_union_reader_writer_example.Writer`

## Configuration
In Docker version, you can provide configuration through environment variables. 
However, when running the example locally, usually you don't want to play around with environment variables.
That's why you can also set configuration properties in *com.intellicess.union.java_union_reader_writer_example.config.DevAppConfiguration*.
