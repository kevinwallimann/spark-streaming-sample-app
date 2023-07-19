# Parquet Streaming Application

This is a sample Scala application that demonstrates streaming data processing using Apache Spark and Spark's Structured Streaming API. The application reads streaming data, performs transformations, and writes the output in Parquet format.

## Features

- Generates streaming data
- Writes the transformed data in Parquet format
- Allows configuration of checkpoint path, output path, and processing time interval

## Prerequisites

- Java 8 from Amazon Corretto is installed
- Apache Spark 2.4.7 is installed
- Scala is installed
- Maven is installed
- SDKMAN! is installed (optional but recommended for managing Java and Spark versions)

## Usage

1. Clone this repository or download the source code.
2. Ensure that Java 8 from Amazon Corretto and Apache Spark 2.4.7 are installed and configured properly.
3. (Optional) Install SDKMAN! for easier management of Java and Spark versions. Use `sdk env install` to install the correct Java and Spark versions.
4. Build the application using `mvn clean package`
5. Run the `MySparkApp` application 
Command to run the `MySparkApp` application:
```
spark-submit --class org.github.kevinwallimann.sssa.MySparkApp <jar-file> --checkpoint <checkpoint-path> --output <output-path> --interval <processing-interval>
```

Example:
```
spark-submit --class org.github.kevinwallimann.sssa.MySparkApp target/spark-streaming-sample-app-0.1.0-SNAPSHOT.jar --checkpoint /tmp/sssa/checkpoints --output /tmp/sssa/out --interval "5 seconds"
```

## Configuration

The `MySparkApp` application supports the following command-line options:

- `--checkpoint <checkpoint-path>`: Specifies the checkpoint path for the streaming query.
- `--output <output-path>`: Specifies the output path for writing Parquet files.
- `--interval <processing-interval>`: Specifies the processing time interval for triggering Parquet writes (e.g., '5 seconds', '1 minute').

## Acknowledgements

A large amount of this codebase was generated with chatGPT.
