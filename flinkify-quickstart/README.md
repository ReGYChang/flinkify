## Flinkify Quickstart

This guide provides a quick introduction to Flinkify, a framework for building streaming
applications using Apache
Flink.

### Flinkify in a Nutshell

Flinkify simplifies Flink streaming job development by offering an intuitive way to:

* Initialize and configure Flink applications
* Execute streaming jobs
* Manage source and sink connectors

### Prerequisites

* JDK 11
* Apache Maven 3.8.6+

Clone the Flinkify repository from GitHub and install it:

```bash
git clone https://github.com/ReGYChang/flinkify.git
mvn clean install -DskipTests
```

### Running the Quickstart

Navigate to the directory containing your compiled Java files and run the program using the `java`
command.
Replace `your-jar-path` with the path to your JAR file.

> [!IMPORTANT]
> Configure your job using `application.yml`. For instance, you'll need to provide a `chat-id` for
> the Telegram bot.

```bash
mvn clean package -DskipTests

java \
-cp /your-jar-path/target/flinkify-quickstart-connector-telegram-0.1.3-SNAPSHOT-jar-with-dependencies.jar \
com.regy.flinkify.quickstart.connector.telegram.TelegramSink
```

### Using Connectors with Flinkify

This guide will introduce you to setting up various connectors in Flinkify. You'll learn how to
configure, initialize,
and use these connectors within your Flink streaming jobs.

The provided examples demonstrate how to use connectors for real-world data processing tasks.

**Connectors Supported:**

| Type          | Data Source   | Source | CDC Source | Sink | CDC Sink |
|---------------|---------------|:------:|:----------:|:----:|:--------:|
| RDBMS         | MySQL         |   ️    |     ✅      |  ☁️  |    ☁️    |
|               | Oracle        |   ☁️   |     ✅      |  ☁️  |    ☁️    |
|               | PostgreSQL    |   ️    |     ☀️     |  ☁️  |    ☁️    |
| Graph         | Nebula Graph  |   ☁️   |     -      |  ✅   |    ☁️    |
| NoSQL         | MongoDB       |   ️    |     ☀️     |  ✅   |    ☁️    |
|               | Elasticsearch |   ☁️   |     -      |  ☁️  |    ☁️    |
| OLAP          | Apache Doris  |   ✅    |     -      |  ✅   |    ✅     |
| IM            | Telegram      |   ☁️   |     -      |  ✅   |    ☁️    |
| Message Queue | Kafka         |   ✅    |     -      |  ✅   |    -     |
|               | RabbitMQ      |   ✅    |     -      |  ✅   |    -     |

### DataStream API with Flinkify

This section offers a guide on using Flink's DataStream API with Flinkify. You'll learn how to
define, configure, and
execute data stream operations for real-time processing within the Flinkify framework.

See: [Apache Flink 1.18](https://nightlies.apache.org/flink/flink-docs-release-1.18/)

### Table API with Flinkify

This guide explores using Flink's Table API in Flinkify. You'll learn how to perform SQL queries,
define tables, and
convert between streams and tables for complex data analytics applications.

See: [JDBC](flinkify-quickstart-table)

### Contributing

We welcome contributions to Flinkify! Refer to the contribution guidelines for details.

### Conclusion

This quickstart guide serves as an introduction to Flinkify. Feel free to reach out if you have any
questions. Happy
streaming!