package com.beancount.jdbc.loader.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

final class PythonHashTest {
    // Secrets obtained from CPython with PYTHONHASHSEED=29.
    private static final long K0 = 0x8242e80971a3bf85L;
    private static final long K1 = 0x452e7c62c21c1e46L;

    @Test
    void hashesAlignWithPythonSeed29() {
        PythonHash hash = PythonHash.withSecret(K0, K1);
        assertEquals(5109853538790556365L, hash.hash("travel"));
        assertEquals(3705117015269033557L, hash.hash("trip-san-francisco-2020"));
        assertEquals(3346730504516585016L, hash.hash("trip-chicago-2021"));
        assertEquals(-7289325628708766470L, hash.hash("foo"));
        assertEquals(-1696825221828146139L, hash.hash("bar"));
    }

    @Test
    void setIterationAlignsWithPythonSeed29() {
        PythonSetOrdering ordering = new PythonSetOrdering(PythonHash.withSecret(K0, K1));
        assertIterableEquals(
                List.of("travel", "trip-san-francisco-2020"),
                ordering.iterate(List.of("trip-san-francisco-2020", "travel")));
        assertIterableEquals(
                List.of("trip-chicago-2021", "travel"),
                ordering.iterate(List.of("travel", "trip-chicago-2021")));
        assertIterableEquals(
                List.of("foo", "bar"),
                ordering.iterate(List.of("foo", "bar")));
        assertIterableEquals(
                List.of("rx_txn", "you-can-inc-tags"),
                ordering.iterate(List.of("you-can-inc-tags", "rx_txn")));
    }
}
