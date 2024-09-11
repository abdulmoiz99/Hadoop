# Hadoop Tasks Repository

This repository contains various Hadoop-related tasks, including MapReduce, Combiner, local aggregation, Avro, and Pig scripts. Each task is implemented to explore different features and techniques within the Hadoop ecosystem.

## Contents

- **MapReduce**: Java implementations of MapReduce jobs to process large-scale datasets.
- **Combiner**: Usage of Combiners to optimize MapReduce jobs by reducing data transfer between Mapper and Reducer.
- **Local Aggregation**: Techniques to aggregate data locally before passing it to the Reducer.
- **Avro**: Working with Apache Avro for efficient data serialization.
- **Pig Scripts**: Data transformation and querying using Apache Pig scripts.

## Setup

1. Clone the repository:
    ```bash
    git clone <repository_url>
    cd hadoop-tasks-repo
    ```

2. Ensure that Hadoop and necessary tools (like Pig, Avro) are installed and configured on your system.

3. Build the Java code (if applicable):
    ```bash
    cd mapreduce
    mvn clean package
    ```

## Tasks

### 1. MapReduce
This folder contains various MapReduce jobs, such as:
- Temperature data processing
- Word count
- Sorting records

### 2. Combiner
Explore examples that use Combiners to reduce the amount of intermediate data transferred during a MapReduce job.

### 3. Local Aggregation
Implementations of local aggregation techniques within MapReduce jobs to improve efficiency.

### 4. Avro
Tasks demonstrating the usage of Apache Avro for data serialization and schema-based processing.

### 5. Pig Scripts
Pig scripts to perform data analysis, including:
- Word count
- Top visited websites by users aged between 18-25
- Various data transformation tasks

## Usage

- **MapReduce**: Run using Hadoop's command line, for example:
    ```bash
    hadoop jar mapreduce/task.jar input output
    ```

- **Pig**: Execute Pig scripts:
    ```bash
    pig -x local pig_scripts/wordcount.pig
    ```

## License

This project is licensed under the MIT License.
