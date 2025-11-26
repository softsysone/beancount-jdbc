package com.beancount.jdbc.loader.semantic.display;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class DisplayContext implements Serializable {
    private final Map<String, Integer> currencyPrecisions = new HashMap<>();
    private boolean renderCommas;

    public DisplayContext() {}

    public DisplayContext(Map<String, Integer> currencyPrecisions, boolean renderCommas) {
        if (currencyPrecisions != null) {
            this.currencyPrecisions.putAll(currencyPrecisions);
        }
        this.renderCommas = renderCommas;
    }

    public DisplayContext(DisplayContext other) {
        if (other != null) {
            this.currencyPrecisions.putAll(other.currencyPrecisions);
            this.renderCommas = other.renderCommas;
        }
    }

    public void setFixedPrecision(String currency, int fractionalDigits) {
        if (currency == null) {
            return;
        }
        currencyPrecisions.put(currency, fractionalDigits);
    }

    public void setRenderCommas(boolean renderCommas) {
        this.renderCommas = renderCommas;
    }

    public Map<String, Integer> getCurrencyPrecisions() {
        return Collections.unmodifiableMap(currencyPrecisions);
    }

    public boolean isRenderCommas() {
        return renderCommas;
    }

    public int getPrecision(String currency, int defaultPrecision) {
        if (currency == null) {
            return defaultPrecision;
        }
        return currencyPrecisions.getOrDefault(currency, defaultPrecision);
    }

    public String key() {
        return String.format(Locale.ROOT, "commas=%s,precisions=%s", renderCommas, currencyPrecisions);
    }

    public DisplayContext copy() {
        return new DisplayContext(currencyPrecisions, renderCommas);
    }
}
