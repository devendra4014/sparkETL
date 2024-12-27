# Spark Data Processing Project

This project demonstrates the process of reading data from different sources, applying transformations using Apache Spark (with Scala), and writing the processed data back to multiple destinations (S3 and local file system). The project is designed to be flexible and scalable, utilizing Spark's powerful distributed processing capabilities.

## Project Overview

The project consists of the following main components:

1. **Reader**: A module to read data from two sources:
   - **Amazon S3**: Reads data from an S3 bucket.
   - **Local File System**: Reads data from local disk storage.

2. **Transformations**: Data transformations are performed using Spark, both at the column level and DataFrame level, to process and manipulate the data.

3. **Writer**: The processed data is written back to:
   - **Amazon S3**: Outputs the transformed data to a specified S3 bucket.
   - **Local File System**: Writes the transformed data to a specified local directory.

## Project Structure


