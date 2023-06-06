package com.regy.quantalink.common.exception;

/**
 * @author regy
 */

public interface ErrCode {

    String MISSING_CONFIG_FILE = "E_CONF_001";
    String MISSING_CONFIG_FIELD = "E_CONF_002";
    String PARSING_CONFIG_FAILED = "E_CONF_003";
    String STREAMING_ENV_FAILED = "E_FLINK_001";
    String STREAMING_CONFIG_FAILED = "E_FLINK_002";
    String STREAMING_CONNECTOR_FAILED = "E_FLINK_003";
    String STREAMING_EXECUTION_FAILED = "E_FLINK_004";
}
