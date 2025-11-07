package com.beancount.jdbc.loader.semantic.inventory;

import java.util.Locale;

public enum BookingMethod {
    FIFO,
    LIFO,
    AVERAGE,
    STRICT;

    public static BookingMethod fromString(String value) {
        if (value == null) {
            return null;
        }
        try {
            return BookingMethod.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }
}
