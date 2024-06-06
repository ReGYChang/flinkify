package io.github.regychang.flinkify.quickstart.connector.telegram.entity;

import io.github.regychang.flinkify.flink.core.connector.telegram.utils.TelegramUtils;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class Transaction {

    @JSONField(name = "pairId")
    public String pairId;

    @JSONField(name = "binId")
    public long binId;

    @JSONField(name = "amountIn")
    public double amountIn;

    @JSONField(name = "amountOut")
    public double amountOut;

    @JSONField(name = "swapForY")
    public boolean swapForY;

    @JSONField(name = "priceXY")
    public double priceXY;

    @JSONField(name = "priceYX")
    public double priceYX;

    @JSONField(name = "userId")
    public String userId;

    @JSONField(name = "timestamp")
    public LocalDateTime timestamp;

    @JSONField(name = "blockNumber")
    public long blockNumber;

    @JSONField(name = "transactionHash")
    public String transactionHash;

    @JSONField(name = "logIndex")
    public int logIndex;

    public String toMarkdownString() {
        String transactionType = TelegramUtils.escapeMarkdown(swapForY ? "USDT -> USDC" : "USDT <- USDC");
        String formattedAmountIn = TelegramUtils.escapeMarkdown(String.format("%.4f", amountIn));
        String formattedAmountOut = TelegramUtils.escapeMarkdown(String.format("%.4f", amountOut));
        String formattedPriceXY = TelegramUtils.escapeMarkdown(String.format("%.4f", priceXY));
        String formattedPriceYX = TelegramUtils.escapeMarkdown(String.format("%.4f", priceYX));
        String formattedTimestamp = TelegramUtils.escapeMarkdown(timestamp.toString());

        return "*Transaction Details:*\n" +
                "*Amount In:* " + formattedAmountIn + "\n" +
                "*Amount Out:* " + formattedAmountOut + "\n" +
                "*Price XY:* " + formattedPriceXY + "\n" +
                "*Price YX:* " + formattedPriceYX + "\n" +
                "*Timestamp:* " + formattedTimestamp + "\n" +
                "*Transaction Type:* " + transactionType;
    }
}
