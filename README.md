# QuantaStream

The QuantaStream provides a comprehensive structure for building applications using Apache Flink's streaming API. The framework encapsulates all necessary steps for running a Flink streaming job including initialization, configuration, execution, and termination. It facilitates defining specific behaviors for Flink streaming jobs, acting as a blueprint for streaming data processing tasks.

## Features

* **Flexible Initialization:** Provides interfaces for custom initialization steps that can be applied to various aspects of the Flink environment, including the execution environment, configuration, source connectors, and sink connectors.

* **Auto-wiring Approach:** Provides an auto-wiring approach for source and sink connectors. This feature simplifies the process of integrating data sources and sinks into Flink jobs, eliminating the need for manual setup. Users can use Flink connectors through YAML configuration.

## Usage

To use this framework, follow the provided examples and use the API to define your streaming jobs, configure sources and sinks, and launch the job in the desired environment.

## Quickstart

Check out QuantaStream's [quickstart](./quantalink-quickstart/README.md) for more information, A simple quickstart of how you might use Quantastream is as follows:

For the auto-wiring of connectors, you can simply provide a YAML configuration:

```yaml
flink:
  sources:
    - kafka:
        - bootstrap-servers: kafka.bootstrap.servers:9092
          group-id: test
          topic: test
          offset-reset-strategy: EARLIEST
          data-type: com.regy.quantalink.quickstart.connector.kafka.entity.DcsEvent
```

Next, extend the `FlinkStreaming` class in your Java project. The `execute()` method should be implemented in order to create the graph topology. Initialize our Flink streaming job with a source connector setup. The source connector setup includes the `deserialization schema adapter` and watermark strategy.

Here is an example of how to implement this:

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

Then you just need to run the `KafkaSource` class. To run your `KafkaSource` class, you need to pass the `--conf` argument followed by the path to your configuration file.

This will start your Flink job with the specified configuration. Be sure to monitor the logs for any potential issues during the execution.

## Building from Source

Refer to the instructions in the main [Apache Flink](https://github.com/apache/flink) repository for building the QuantaStream from source.

## Contributing

The QuantaStream is an open-source project. We encourage contributions from developers who want to improve the system or add new features.

## Support

Don’t hesitate to ask for help!

Open an issue if you find a bug in the QuantaStream.