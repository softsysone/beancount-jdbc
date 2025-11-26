package com.beancount.jdbc.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

final class DecimalParserTest {

    @Test
    void parsesDotDecimal() {
        assertEquals(new BigDecimal("1234.56"), DecimalParser.parse("1234.56"));
    }

    @Test
    void parsesCommaDecimal() {
        assertEquals(new BigDecimal("1234.56"), DecimalParser.parse("1234,56"));
    }

    @Test
    void parsesNegative() {
        assertEquals(0, DecimalParser.parse("-0,50").compareTo(new BigDecimal("-0.5")));
    }

    @Test
    void returnsNullForBlank() {
        assertNull(DecimalParser.parse("   "));
        assertNull(DecimalParser.parse(null));
    }

    @Test
    void rejectsMixedSeparators() {
        assertThrows(NumberFormatException.class, () -> DecimalParser.parse("1,234.56"));
    }
}
