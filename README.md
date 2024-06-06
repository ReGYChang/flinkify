# Flinkify: Simplify Flink Streaming Job Development

**Flinkify** provides a streamlined framework for building Apache Flink streaming applications. It encapsulates the
entire Flink job lifecycle, from initialization to execution, allowing you to focus on your business logic.

**Features:**

* **Effortless Initialization:** Customize various aspects of your Flink environment through flexible interfaces,
  including configuration, connectors, and execution settings.

* **Auto-wired Connectors:** Simplify data integration with auto-wiring support for source and sink connectors. Define
  configurations in YAML for seamless integration with Flink connectors.

**Getting Started:**

See the provided examples and API to define your streaming jobs, configure sources and sinks, and launch them in any
environment.

**Quick Example:**

Leverage auto-wiring with YAML:

```yaml
flink:
  sources:
    - kafka:
        - bootstrap-servers: kafka.bootstrap.servers:9092
          group-id: test
          topic: test
          offset-reset-strategy: EARLIEST
          data-type: io.github.regychang.flinkify.quickstart.connector.kafka.entity.DcsEvent
```

Extend `FlinkStreaming` in your Java project and implement `execute()` to define your stream processing logic. Here's an
example with Kafka source:

```java
public class KafkaSource extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        FlinkStreamingInitializer initializer = new FlinkStreamingInitializer.Builder()
                .withSourceConnectorSetup(
                        sourceConnector ->
                                sourceConnector.withDeserializationSchemaAdapter(KafkaDeserializationAdapter.valueOnlyDefault(TypeInformation.get(DcsEvent.class)))
                                        .withWatermarkStrategy(WatermarkStrategy.noWatermarks()),
                        TypeInformation.get(DcsEvent.class)).build();

        (new KafkaSource()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext context) throws FlinkException {
        DataStreamSource<DcsEvent> sourceStream = context.getSourceDataStream(TypeInformation.get(DcsEvent.class));
        sourceStream.print();
    }
}
```

Run `KafkaSource` with the `--conf` argument followed by your configuration file path. This will start your Flink job
with the specified configuration.

**Building & Contributing:**

Refer to the main Apache Flink repository for building instructions. Flinkify is open-source! We welcome contributions
for improvements and new features.

**Support:**

Need help? Open an issue for bug reports or questions.
