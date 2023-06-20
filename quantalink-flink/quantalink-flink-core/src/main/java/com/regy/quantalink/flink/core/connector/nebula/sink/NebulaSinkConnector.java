package com.regy.quantalink.flink.core.connector.nebula.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Optional;

/**
 * @author regy
 */
public class NebulaSinkConnector extends SinkConnector<Row> {

    private final String graphAddress;
    private final String metaAddress;
    private final String username;
    private final String password;
    private final WriteModeEnum writeMode;
    private final int batchSize;
    private final int batchIntervalMs;
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
        super(env, config);
        this.writeMode = config.get(NebulaOptions.NEBULA_WRITE_MODE);
        this.batchSize = config.get(NebulaOptions.NEBULA_BATCH_SIZE);
        this.batchIntervalMs = config.get(NebulaOptions.NEBULA_BATCH_INTERVAL_MS);
        this.graphAddress = config.getNotNull(NebulaOptions.NEBULA_GRAPH_ADDRESS, String.format("Nebula sink connector '%s' graph address must not be null", super.connectorName));
        this.metaAddress = config.getNotNull(NebulaOptions.NEBULA_META_ADDRESS, String.format("Nebula sink connector '%s' meta address must not be null", super.connectorName));
        this.username = config.getNotNull(NebulaOptions.NEBULA_USERNAME, String.format("Nebula sink connector '%s' username must not be null", super.connectorName));
        this.password = config.getNotNull(NebulaOptions.NEBULA_PASSWORD, String.format("Nebula sink connector '%s' password must not be null", super.connectorName));
        this.graphSpace = config.getNotNull(NebulaOptions.NEBULA_GRAPH_SPACE, String.format("Nebula sink connector '%s' graph space must not be null", super.connectorName));
        this.rowType = config.getNotNull(NebulaOptions.NEBULA_ROW_TYPE, String.format("Nebula sink connector '%s' row type must not be null", super.connectorName));
        if (rowType.equals(NebulaRowType.Vertex)) {
            this.vertexName = config.getNotNull(NebulaOptions.NEBULA_VERTEX_NAME, String.format("Nebula sink connector '%s' vertex name must not be null", super.connectorName));
            this.vertexIdIndex = config.getNotNull(NebulaOptions.NEBULA_VERTEX_ID_INDEX, String.format("Nebula sink connector '%s' vertex id index must not be null", super.connectorName));
            this.vertexFields = config.getNotNull(NebulaOptions.NEBULA_VERTEX_FIELDS, String.format("Nebula sink connector '%s' vertex fields must not be null", super.connectorName));
            this.vertexPositions = config.getNotNull(NebulaOptions.NEBULA_VERTEX_POSITIONS, String.format("Nebula sink connector '%s' must not be null", super.connectorName));
        } else {
            this.edgeName = config.getNotNull(NebulaOptions.NEBULA_EDGE_NAME, String.format("Nebula sink connector '%s' must not be null", super.connectorName));
            this.edgeSrcIndex = config.getNotNull(NebulaOptions.NEBULA_EDGE_SRC_INDEX, String.format("Nebula sink connector '%s' must not be null", super.connectorName));
            this.edgeDstIndex = config.getNotNull(NebulaOptions.NEBULA_EDGE_DST_INDEX, String.format("Nebula sink connector '%s' must not be null", super.connectorName));
            this.edgeRandIndex = config.getNotNull(NebulaOptions.NEBULA_EDGE_RANK_INDEX, String.format("Nebula sink connector '%s' must not be null", super.connectorName));
            this.edgeFields = config.getNotNull(NebulaOptions.NEBULA_EDGE_FIELDS, String.format("Nebula sink connector '%s' must not be null", super.connectorName));
            this.edgePositions = config.getNotNull(NebulaOptions.NEBULA_EDGE_POSITIONS, String.format("Nebula sink connector '%s' must not be null", super.connectorName));
        }
        super.outputTag = new OutputTag<>(String.format("%s-%s", Optional.ofNullable(vertexName).orElse(edgeName), rowType.name()), Types.GENERIC(Row.class));
    }

    @Override
    public DataStreamSink<Row> getSinkDataStream(DataStream<Row> stream) {
        NebulaClientOptions clientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnProvider = new NebulaGraphConnectionProvider(clientOptions);
        NebulaMetaConnectionProvider metaConnProvider = new NebulaMetaConnectionProvider(clientOptions);
        NebulaSinkFunction<Row> sinkFunc = getSinkFunc(graphConnProvider, metaConnProvider);
        try {
            return stream.addSink(sinkFunc).name(connectorName).disableChaining();
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Could not add sink from stream '%s' to nebula connector: ", stream.toString()), e);
        }
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
                            .setBatchIntervalMs(batchIntervalMs)
                            .setWriteMode(writeMode)
                            .build();
            NebulaVertexBatchOutputFormat outputFormat =
                    new NebulaVertexBatchOutputFormat(graphConnProvider, metaConProvider, executionOptions);
            return new NebulaSinkFunction<>(outputFormat);
        } else if (rowType.equals(NebulaRowType.Edge)) {
            EdgeExecutionOptions executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                    .setGraphSpace(graphSpace)
                    .setEdge(edgeName)
                    .setSrcIndex(edgeSrcIndex)
                    .setDstIndex(edgeDstIndex)
                    .setRankIndex(edgeRandIndex)
                    .setFields(edgeFields)
                    .setPositions(edgePositions)
                    .setBatchSize(batchSize)
                    .setBatchIntervalMs(batchIntervalMs)
                    .setWriteMode(writeMode)
                    .build();
            NebulaEdgeBatchOutputFormat outputFormat =
                    new NebulaEdgeBatchOutputFormat(graphConnProvider, metaConProvider, executionOptions);
            return new NebulaSinkFunction<>(outputFormat);
        }
        throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Unknown Nebula row type: %s", rowType));
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
