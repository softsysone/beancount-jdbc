package com.beancount.jdbc.loader;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParsePosition;
import java.util.Locale;

/**
 * Locale-neutral decimal parser that accepts either dot or comma as the decimal separator and
 * rejects grouping/mixed separators. Centralizing this keeps amount parsing consistent across
 * ledger amounts and option values while avoiding accidental locale-dependent behavior.
 */
public final class DecimalParser {

    private static final ThreadLocal<DecimalFormat> DOT_FORMAT =
            ThreadLocal.withInitial(() -> buildFormat('.'));
    private static final ThreadLocal<DecimalFormat> COMMA_FORMAT =
            ThreadLocal.withInitial(() -> buildFormat(','));

    private DecimalParser() {}

    /**
     * Parses a decimal string that may use either '.' or ',' as the decimal separator. Returns
     * {@code null} for null/empty input. Throws {@link NumberFormatException} for mixed separators,
     * grouping characters, or incomplete parses.
     */
    public static BigDecimal parse(String text) {
        if (text == null) {
            return null;
        }
        String trimmed = text.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        boolean hasDot = trimmed.indexOf('.') >= 0;
        boolean hasComma = trimmed.indexOf(',') >= 0;
        if (hasDot && hasComma) {
            throw new NumberFormatException("Mixed decimal separators not allowed: " + text);
        }
        DecimalFormat format = hasComma ? COMMA_FORMAT.get() : DOT_FORMAT.get();
        ParsePosition position = new ParsePosition(0);
        Number parsed = format.parse(trimmed, position);
        if (parsed == null || position.getIndex() != trimmed.length()) {
            throw new NumberFormatException("Invalid decimal: " + text);
        }
        if (!(parsed instanceof BigDecimal)) {
            return new BigDecimal(parsed.toString());
        }
        return (BigDecimal) parsed;
    }

    private static DecimalFormat buildFormat(char decimalSeparator) {
        DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(Locale.ROOT);
        symbols.setDecimalSeparator(decimalSeparator);
        // Disable grouping to avoid silently accepting locale-specific thousand separators.
        DecimalFormat format = new DecimalFormat();
        format.setDecimalFormatSymbols(symbols);
        format.setParseBigDecimal(true);
        format.setGroupingUsed(false);
        format.setMaximumFractionDigits(Integer.MAX_VALUE);
        format.setMaximumIntegerDigits(Integer.MAX_VALUE);
        return format;
    }
}
