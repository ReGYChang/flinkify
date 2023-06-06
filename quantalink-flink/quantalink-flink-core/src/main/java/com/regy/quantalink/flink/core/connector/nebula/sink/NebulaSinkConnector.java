package com.regy.quantalink.flink.core.connector.nebula.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.flink.core.config.ConnectorOptions;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.nebula.config.NebulaOptions;
import com.regy.quantalink.flink.core.connector.nebula.enums.NebulaRowType;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaEdgeBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.connector.nebula.sink.NebulaVertexBatchOutputFormat;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import java.util.List;

/**
 * @author regy
 */
public class NebulaSinkConnector<T> extends SinkConnector<T> {

    private final String graphAddress;
    private final String metaAddress;
    private final String username;
    private final String password;
    private final WriteModeEnum writeMode;
    private final int batchSize;
    private final String graphSpace;
    private final NebulaRowType rowType;
    private String vertexName;
    private int vertexIdIndex;
    private List<String> vertexFields;
    private List<Integer> vertexPositions;
    private String edgeName;
    private int edgeSrcIndex;
    private int edgeDstIndex;
    private int edgeRandIndex;
    private List<String> edgeFields;
    private List<Integer> edgePositions;

    public NebulaSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config, config.get(ConnectorOptions.PARALLELISM), config.get(ConnectorOptions.NAME), config.getNotNull(ConnectorOptions.DATA_TYPE, "Nebula sink connector data type must not be null"));
        this.graphAddress = config.getNotNull(NebulaOptions.NEBULA_GRAPH_ADDRESS, String.format("Nebula sink connector '%s' graph address must not be null", super.sinkName));
        this.metaAddress = config.getNotNull(NebulaOptions.NEBULA_META_ADDRESS, String.format("Nebula sink connector '%s' meta address must not be null", super.sinkName));
        this.username = config.getNotNull(NebulaOptions.NEBULA_USERNAME, String.format("Nebula sink connector '%s' username must not be null", super.sinkName));
        this.password = config.getNotNull(NebulaOptions.NEBULA_PASSWORD, String.format("Nebula sink connector '%s' password must not be null", super.sinkName));
        this.graphSpace = config.getNotNull(NebulaOptions.NEBULA_GRAPH_SPACE, String.format("Nebula sink connector '%s' graph space must not be null", super.sinkName));
        this.writeMode = config.getNotNull(NebulaOptions.NEBULA_WRITE_MODE, String.format("Nebula sink connector '%s' write mode must not be null", super.sinkName));
        this.batchSize = config.getNotNull(NebulaOptions.NEBULA_BATCH_SIZE, String.format("Nebula sink connector '%s' batch must not be null", super.sinkName));
        this.rowType = config.getNotNull(NebulaOptions.NEBULA_ROW_TYPE, String.format("Nebula sink connector '%s' row type must not be null", super.sinkName));
        if (rowType.equals(NebulaRowType.Vertex)) {
            this.vertexName = config.getNotNull(NebulaOptions.NEBULA_VERTEX_NAME, String.format("Nebula sink connector '%s' vertex name must not be null", super.sinkName));
            this.vertexIdIndex = config.getNotNull(NebulaOptions.NEBULA_VERTEX_ID_INDEX, String.format("Nebula sink connector '%s' vertex id index must not be null", super.sinkName));
            this.vertexFields = config.getNotNull(NebulaOptions.NEBULA_VERTEX_FIELDS, String.format("Nebula sink connector '%s' vertex fields must not be null", super.sinkName));
            this.vertexPositions = config.getNotNull(NebulaOptions.NEBULA_VERTEX_POSITIONS, String.format("Nebula sink connector '%s' must not be null", super.sinkName));
        } else {
            this.edgeName = config.getNotNull(NebulaOptions.NEBULA_EDGE_NAME, String.format("Nebula sink connector '%s' must not be null", super.sinkName));
            this.edgeSrcIndex = config.getNotNull(NebulaOptions.NEBULA_EDGE_SRC_INDEX, String.format("Nebula sink connector '%s' must not be null", super.sinkName));
            this.edgeDstIndex = config.getNotNull(NebulaOptions.NEBULA_EDGE_DST_INDEX, String.format("Nebula sink connector '%s' must not be null", super.sinkName));
            this.edgeRandIndex = config.getNotNull(NebulaOptions.NEBULA_EDGE_RANK_INDEX, String.format("Nebula sink connector '%s' must not be null", super.sinkName));
            this.edgeFields = config.getNotNull(NebulaOptions.NEBULA_EDGE_FIELDS, String.format("Nebula sink connector '%s' must not be null", super.sinkName));
            this.edgePositions = config.getNotNull(NebulaOptions.NEBULA_EDGE_POSITIONS, String.format("Nebula sink connector '%s' must not be null", super.sinkName));
        }
    }

    @Override
    public void init() {

    }

    @Override
    public DataStreamSink<T> getSinkDataStream(DataStream<T> stream) {
        NebulaClientOptions clientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnProvider = new NebulaGraphConnectionProvider(clientOptions);
        NebulaMetaConnectionProvider metaConnProvider = new NebulaMetaConnectionProvider(clientOptions);
        NebulaSinkFunction<Row> sinkFunc = getSinkFunc(graphConnProvider, metaConnProvider);
        try {
            ((SingleOutputStreamOperator<?>) stream).getSideOutput(getOutputTag())
                    .addSink(sinkFunc).name(sinkName).disableChaining();
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Could not add sink from stream '%s' to nebula connector: ", stream.toString()), e);
        }
        return null;
    }

    private NebulaSinkFunction<Row> getSinkFunc(
            NebulaGraphConnectionProvider graphConnProvider, NebulaMetaConnectionProvider metaConProvider) {
        if (rowType.equals(NebulaRowType.Vertex)) {
            VertexExecutionOptions executionOptions =
                    new VertexExecutionOptions.ExecutionOptionBuilder()
                            .setGraphSpace(graphSpace)
                            .setTag(vertexName)
                            .setIdIndex(vertexIdIndex)
                            .setFields(vertexFields)
                            .setPositions(vertexPositions)
                            .setBatchSize(batchSize)
                            .build();
            NebulaVertexBatchOutputFormat outputFormat =
                    new NebulaVertexBatchOutputFormat(graphConnProvider, metaConProvider, executionOptions);
            return new NebulaSinkFunction<>(outputFormat);
        } else {
            EdgeExecutionOptions executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                    .setGraphSpace(graphSpace)
                    .setEdge(edgeName)
                    .setSrcIndex(edgeSrcIndex)
                    .setDstIndex(edgeDstIndex)
                    .setRankIndex(edgeRandIndex)
                    .setFields(edgeFields)
                    .setPositions(edgePositions)
                    .setBatchSize(batchSize)
                    .build();
            NebulaEdgeBatchOutputFormat outputFormat =
                    new NebulaEdgeBatchOutputFormat(graphConnProvider, metaConProvider, executionOptions);
            return new NebulaSinkFunction<>(outputFormat);
        }
    }

    public OutputTag<Row> getOutputTag() {
        String rowName = rowType.equals(NebulaRowType.Vertex) ? vertexName : edgeName;
        return new OutputTag<>(String.format("%s-%s", rowName, writeMode.name()), Types.GENERIC(Row.class));
    }

    public Integer getRowArity() {
        return rowType.equals(NebulaRowType.Vertex) ?
                vertexFields.size() + 1 : edgeFields.size() + 2;
    }

    private NebulaClientOptions getClientOptions() {
        return new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setGraphAddress(graphAddress)
                .setMetaAddress(metaAddress)
                .setUsername(username)
                .setPassword(password)
                .build();
    }
}
