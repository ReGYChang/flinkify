package io.github.regychang.flinkify.flink.core.connector.telegram.serialization;

// FIXME
//public class DefaultTelegramHttpConvert
//        implements SchemaLifecycleAwareElementConverter<TelegramPayload, HttpSinkRequestEntry> {
//
//    private final String insertMethod;
//
//    private final SerializationSchema<TelegramPayload> serializationSchema;
//
//    public DefaultTelegramHttpConvert(
//            String insertMethod, SerializationSchema<TelegramPayload> serializationSchema) {
//        this.insertMethod = insertMethod;
//        this.serializationSchema = serializationSchema;
//    }
//
//    @Override
//    public void open(Sink.InitContext ctx) {
//        try {
//            this.serializationSchema.open(ctx.asSerializationSchemaInitializationContext());
//        } catch (Exception e) {
//            throw new FlinkRuntimeException("Failed to initialize serialization schema.", e);
//        }
//    }
//
//    @Override
//    public HttpSinkRequestEntry apply(TelegramPayload payload, SinkWriter.Context ctx) {
//        return new HttpSinkRequestEntry(this.insertMethod, this.serializationSchema.serialize(payload));
//    }
//}
