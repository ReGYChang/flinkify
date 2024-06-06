package io.github.regychang.flinkify.quickstart.connector.telegram.function;

import io.github.regychang.flinkify.common.utils.TimeUtils;
import io.github.regychang.flinkify.quickstart.connector.telegram.entity.Transaction;

import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

@Slf4j
public class ApiBatchFunction extends RichSourceFunction<Transaction> {

    private static final Logger LOG = LoggerFactory.getLogger(ApiBatchFunction.class);

    private static final long DEFAULT_CONNECTION_RETRY_SLEEP = TimeUtils.toMillis(Duration.ofSeconds(20));

    private final LinkedHashSet<Transaction> txList = new LinkedHashSet<>();

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Transaction> sourceCtx) throws Exception {
        while (isRunning) {
            LOG.debug("---------------->");
            String response =
                    fetchData(
                            "https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&delay=0&ex_ch=tse_00878.tw",
                            "GET");

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

    public static String fetchData(String urlStr, String method) throws IOException {
        return fetchData(urlStr, method, null);
    }

    public static String fetchData(String urlStr, String method, String requestBody) throws IOException {
        disableSslVerification();
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod(method);
        conn.setDoOutput(true);

        // Send request body if it's not null
        if (requestBody != null) {
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = requestBody.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }
        }

        StringBuilder response = new StringBuilder();
        try {
            if (conn.getResponseCode() != 200) {
                LOG.error("Failed : HTTP error code : {}", conn.getResponseCode());
                return response.toString();
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String output;
                while ((output = br.readLine()) != null) {
                    response.append(output);
                }
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
        } finally {
            conn.disconnect();
        }

        return response.toString();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private static void disableSslVerification() {
        try {
            // Create a trust manager that does not validate certificate chains
            TrustManager[] trustAllCertificates = new TrustManager[]{
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() {
                            return null;
                        }

                        public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        }

                        public void checkServerTrusted(X509Certificate[] certs, String authType) {
                        }
                    }
            };

            // Install the all-trusting trust manager
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCertificates, new SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

            // Install the all-trusting host verifier
            HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
