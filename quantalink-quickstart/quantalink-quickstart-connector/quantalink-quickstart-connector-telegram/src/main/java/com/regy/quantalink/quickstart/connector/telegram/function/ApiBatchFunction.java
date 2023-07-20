package com.regy.quantalink.quickstart.connector.telegram.function;

import com.regy.quantalink.common.utils.TimeUtils;
import com.regy.quantalink.quickstart.connector.telegram.entity.Transaction;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * @author regy
 */
public class ApiBatchFunction extends RichSourceFunction<Transaction> {

    private static final Logger LOG = LoggerFactory.getLogger(ApiBatchFunction.class);
    private static final long DEFAULT_CONNECTION_RETRY_SLEEP = TimeUtils.toMillis(Duration.ofSeconds(20));
    private final LinkedHashSet<Transaction> txList = new LinkedHashSet<>();
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Transaction> sourceCtx) throws Exception {
        while (isRunning) {
            LOG.debug("---------------->");
            String response = fetchData();

            List<Transaction> transactions = JSON.parseArray(response, Transaction.class);
            transactions.sort(Comparator.comparing(in -> in.timestamp));
            for (Transaction transaction : transactions) {
                if (txList.add(transaction)) {
                    sourceCtx.collect(transaction);
                    if (txList.size() > 1000) {
                        Iterator<Transaction> iterator = txList.iterator();
                        iterator.next();
                        iterator.remove();
                    }
                }
            }
            Thread.sleep(DEFAULT_CONNECTION_RETRY_SLEEP);
        }
    }

    private String fetchData() throws IOException {
        String urlStr = "https://barn.traderjoexyz.com/v1/events/swap/avalanche/0x9b2cc8e6a2bbb56d6be4682891a91b0e48633c72?pageNum=1&pageSize=10";
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        // Set properties before opening the input stream
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/json");

        StringBuilder response = new StringBuilder();
        try {
            if (conn.getResponseCode() != 200) {
                LOG.error("Failed : HTTP error code : " + conn.getResponseCode());
                return response.toString();
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String output;
                while ((output = br.readLine()) != null) {
                    response.append(output);
                }
            }
        } finally {
            conn.disconnect();
        }

        return response.toString();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
