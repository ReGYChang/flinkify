# Quantastream Quickstart

## Introduction

Quantastream is a robust and flexible structure for building streaming applications using Apache Flink. It provides an
easy way to initialize, configure, execute, and terminate Flink streaming jobs. With Quantalink, integrating source and
sink connectors into Flink jobs is a breeze, thanks to our auto-wiring approach.

In this quickstart guide, we'll introduce you to the basics of using Quantastream. You will learn how to set up your
development environment, run an example Flink job, and understand how to configure and use various connectors.

## Pre-requisites

Before we get started, ensure that you have the following installed:

- JDK 11
- Apache Maven 3.8.6

Clone the Quantastream repository from GitHub.

```bash
git clone https://github.com/your-repository/quantalink.git
mvn clean install -DskipTests
```

## Running the Quickstart

In order to run the Flink job, navigate to the directory containing your compiled Java files, and use the java command to run your program. Replace `your-jar-file.jar` with the path to the JAR file you built.

```bash
mvn clean package -DskipTests
java -cp your-jar-file.jar com.regy.quantalink.quickstart.connector.telegram.TelegramSink
```

## Connectors Quickstart

> 💡 In this quickstart guide, you will get hands-on experience with setting up various connectors in Quantastream. From the Kafka Connector to the Telegram Connector, you will learn how to configure, initialize, and use these connectors within your Flink streaming jobs. This guide serves as a comprehensive introduction to effectively working with different data sources and sinks, paving the way for more complex data processing tasks.

- [Doris Connector Quickstart](./quantalink-quickstart-connector/quantalink-quickstart-connector-doris)
- [Kafka Connector Quickstart](./quantalink-quickstart-connector/quantalink-quickstart-connector-kafka)
- [Mongo Connector Quickstart](./quantalink-quickstart-connector/quantalink-quickstart-connector-mongo)
- [MySQL Connector Quickstart](./quantalink-quickstart-connector/quantalink-quickstart-connector-mysql)
- [Nebula Graph Connector Quickstart](./quantalink-quickstart-connector/quantalink-quickstart-connector-nebula)
- [Oracle Connector Quickstart](./quantalink-quickstart-connector/quantalink-quickstart-connector-oracle)
- [RabbitMQ Connector Quickstart](./quantalink-quickstart-connector/quantalink-quickstart-connector-rabbitmq)
- [Telegram Connector Quickstart](./quantalink-quickstart-connector/quantalink-quickstart-connector-telegram)

Each section of this guide will equip you with the knowledge and skills to use Quantalink with various connectors effectively. The practical examples provided will give you a better understanding of how these connectors can be used in real-world data streaming scenarios.

## Datastream Quickstart

> 🌊 Focused on Flink's DataStream API, this guide offers insights into defining, configuring, and executing datastream operations for real-time processing within the Quantalink framework.

- [Apache Flink 1.16](./quantalink-quickstart-datastream/quantalink-quickstart-datastream-v1.16)

## Table Quickstart

> 💼 This guide explores the use of Flink's Table API in Quantalink, teaching how to perform SQL queries, define tables, and convert between streams and tables for complex data analytics applications.

- [JDBC](./quantalink-quickstart-table/quantalink-quickstart-table-jdbc)

## Contributing

We encourage you to contribute to Quantalink! Check out our contribution guidelines for more details.

## Conclusion

We hope this quickstart guide helps you understand how to get started with Quantalink. Feel free to reach out if you
have any issues or questions. Happy streaming!