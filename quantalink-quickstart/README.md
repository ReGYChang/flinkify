# QuantaStream Quickstart

## Introduction

QuantaStream is a robust and flexible structure for building streaming applications using Apache Flink. It provides an
easy way to initialize, configure, execute, and terminate Flink streaming jobs. With QuantaStream, integrating source
and
sink connectors into Flink jobs is a breeze, thanks to our auto-wiring approach.

In this quickstart guide, we'll introduce you to the basics of using QuantaStream. You will learn how to set up your
development environment, run an example Flink job, and understand how to configure and use various connectors.

## Pre-requisites

Before we get started, ensure that you have the following installed:

- JDK 11
- Apache Maven 3.8.6+

Clone the Quantastream repository from GitHub.

```bash
git clone https://github.com/ReGYChang/quantastream.git
mvn clean install -DskipTests
```

## Running the Quickstart

In order to run the Flink job, navigate to the directory containing your compiled Java files, and use the java command
to run your program. Replace `your-jar-path` with the path to the JAR file you built.

> ❗️ Note that you should set up your configuration for the job with `application.yml`. For example, you should assign your `chat-id` and other required configurations for the Telegram bot

```bash
mvn clean package -DskipTests

java \
-cp /your-jar-path/target/quantalink-quickstart-connector-telegram-1.0-SNAPSHOT-jar-with-dependencies.jar \
com.regy.quantalink.quickstart.connector.telegram.TelegramSink
```

## Connectors Quickstart

> 💡 In this quickstart guide, you will get hands-on experience with setting up various connectors in QuantaStream. From
> the Kafka Connector to the Telegram Connector, you will learn how to configure, initialize, and use these connectors
> within your Flink streaming jobs. This guide serves as a comprehensive introduction to effectively working with
> different data sources and sinks, paving the way for more complex data processing tasks.

- ☀️ High Priority
- 🌤️ Medium Priority
- ☁️ Low Priority
- ✅ Quickstart Link

| Type          | Data Source   |                                                                           Source                                                                           |                                                                           CDC Source                                                                           |                                                                               Sink                                                                                | CDC Sink |
|---------------|---------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------:|:--------:|
| RDBMS         | MySQL         |                                                                            🌤️                                                                             |  [✅](./quantalink-quickstart-connector/quantalink-quickstart-connector-mysql/src/main/java/com/regy/quantalink/quickstart/connector/mysql/cdc/MySqlCdc.java)   |                                                                                ☁️                                                                                 |    ☁️    |
|               | Oracle        |                                                                             ☁️                                                                             | [✅](./quantalink-quickstart-connector/quantalink-quickstart-connector-oracle/src/main/java/com/regy/quantalink/quickstart/connector/oracle/cdc/OracleCdc.java) |                                                                                ☁️                                                                                 |    ☁️    |
|               | PostgreSQL    |                                                                            🌤️                                                                             |                                                                               ☀️                                                                               |                                                                                ☁️                                                                                 |    ☁️    |
| Graph         | Nebula Graph  |                                                                             ☁️                                                                             |                                                                               ☁️                                                                               |    [✅](./quantalink-quickstart-connector/quantalink-quickstart-connector-nebula/src/main/java/com/regy/quantalink/quickstart/connector/nebula/NebulaSink.java)    |    ☁️    |
| NoSQL         | MongoDB       |                                                                            🌤️                                                                             |                                                                               ☀️                                                                               |                                                                                 ✅                                                                                 |    ☁️    |
|               | Elasticsearch |                                                                             ☁️                                                                             |                                                                               ☁️                                                                               |                                                                                ☁️                                                                                 |    ☁️    |
| OLAP          | Apache Doris  | [✅](./quantalink-quickstart-connector/quantalink-quickstart-connector-doris/src/main/java/com/regy/quantalink/quickstart/connector/doris/DorisSource.java) |                                                                               ☁️                                                                               |                                                                                 ✅                                                                                 |    ✅     |
| IM            | Telegram      |                                                                             ☁️                                                                             |                                                                               ☁️                                                                               | [✅](./quantalink-quickstart-connector/quantalink-quickstart-connector-telegram/src/main/java/com/regy/quantalink/quickstart/connector/telegram/TelegramSink.java) |    ☁️    |
| Message Queue | Kafka         | [✅](./quantalink-quickstart-connector/quantalink-quickstart-connector-kafka/src/main/java/com/regy/quantalink/quickstart/connector/kafka/KafkaSource.java) |                                                                               -                                                                                |                                                                                 ✅                                                                                 |    -     |
|               | RabbitMQ      |                                                                             ✅                                                                              |                                                                               -                                                                                |                                                                                 ✅                                                                                 |    -     |

Each section of this guide will equip you with the knowledge and skills to use QuantaStream with various connectors
effectively. The practical examples provided will give you a better understanding of how these connectors can be used in
real-world data streaming scenarios.

## QuantaStream Quickstart

> 🌊 Focused on Flink's DataStream API, this guide offers insights into defining, configuring, and executing datastream
> operations for real-time processing within the QuantaStream framework.

- [Apache Flink 1.16](./quantalink-quickstart-datastream/quantalink-quickstart-datastream-v1.16)

## Table Quickstart

> 💼 This guide explores the use of Flink's Table API in QuantaStream, teaching how to perform SQL queries, define
> tables, and convert between streams and tables for complex data analytics applications.

- [JDBC](./quantalink-quickstart-table/quantalink-quickstart-table-jdbc)

## Contributing

We encourage you to contribute to QuantaStream! Check out our contribution guidelines for more details.

## Conclusion

We hope this quickstart guide helps you understand how to get started with QuantaStream. Feel free to reach out if you
have any issues or questions. Happy streaming!