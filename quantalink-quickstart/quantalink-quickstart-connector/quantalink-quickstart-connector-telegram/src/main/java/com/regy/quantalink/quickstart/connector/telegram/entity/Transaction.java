package com.regy.quantalink.quickstart.connector.telegram.entity;

import com.regy.quantalink.flink.core.connector.telegram.utils.TelegramUtils;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.time.LocalDateTime;

/**
 * @author regy
 */
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Transaction that = (Transaction) o;

        return new EqualsBuilder().append(binId, that.binId).append(amountIn, that.amountIn).append(amountOut, that.amountOut).append(swapForY, that.swapForY).append(priceXY, that.priceXY).append(priceYX, that.priceYX).append(blockNumber, that.blockNumber).append(logIndex, that.logIndex).append(pairId, that.pairId).append(userId, that.userId).append(timestamp, that.timestamp).append(transactionHash, that.transactionHash).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(pairId).append(binId).append(amountIn).append(amountOut).append(swapForY).append(priceXY).append(priceYX).append(userId).append(timestamp).append(blockNumber).append(transactionHash).append(logIndex).toHashCode();
    }

    @Override
    public String toString() {
        String transactionType = swapForY ? "USDT -> USDC" : "USDT <- USDC";
        String formattedAmountIn = String.format("%.4f", amountIn);
        String formattedAmountOut = String.format("%.4f", amountOut);
        String formattedPriceXY = String.format("%.4f", priceXY);
        String formattedPriceYX = String.format("%.4f", priceYX);

        return "Transaction Details:\n" +
                "------------------------\n" +
                "Amount In: " + formattedAmountIn + "\n" +
                "Amount Out: " + formattedAmountOut + "\n" +
                "Price XY: " + formattedPriceXY + "\n" +
                "Price YX: " + formattedPriceYX + "\n" +
                "Timestamp: " + timestamp + "\n" +
                "Transaction Type: " + transactionType;
    }

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
