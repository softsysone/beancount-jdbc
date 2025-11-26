package com.beancount.jdbc.loader.semantic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Replays CPython's set insertion so iteration order (and thus tag/link join order) matches
 * bean-sql.
 */
final class PythonSetOrdering {
    private static final int MIN_TABLE_SIZE = 8; // PySet_MINSIZE

    private final PythonHash hasher;

    PythonSetOrdering(PythonHash hasher) {
        this.hasher = Objects.requireNonNull(hasher, "hasher");
    }

    List<String> iterate(Collection<String> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        int size = MIN_TABLE_SIZE;
        int mask = size - 1;
        String[] table = new String[size];
        long[] hashes = new long[size];
        int used = 0;

        for (String value : values) {
            if (value == null) {
                continue;
            }
            long hash = hasher.hash(value);
            int index = (int) (hash & mask);
            int perturb = (int) hash;
            while (true) {
                if (table[index] == null) {
                    table[index] = value;
                    hashes[index] = hash;
                    used++;
                    break;
                } else if (hashes[index] == hash && table[index].equals(value)) {
                    // Duplicate; ignore.
                    break;
                }
                index = (index * 5 + 1 + perturb) & mask;
                perturb >>>= 5; // PERTURB_SHIFT
            }
            if (needsResize(used, size)) {
                Object[] resized = resize(table, hashes, used, size << 1);
                table = (String[]) resized[0];
                hashes = (long[]) resized[1];
                size = table.length;
                mask = size - 1;
            }
        }
        List<String> ordered = new ArrayList<>(used);
        for (String value : table) {
            if (value != null) {
                ordered.add(value);
            }
        }
        return ordered;
    }

    private static boolean needsResize(int used, int size) {
        // Roughly mirror CPython's load factor (2/3 full).
        return used * 3 >= size * 2;
    }

    private Object[] resize(String[] table, long[] hashes, int used, int newSize) {
        int size = Math.max(MIN_TABLE_SIZE, newSize);
        int mask = size - 1;
        String[] newTable = new String[size];
        long[] newHashes = new long[size];
        for (int i = 0, placed = 0; i < table.length && placed < used; i++) {
            if (table[i] == null) {
                continue;
            }
            placed++;
            String value = table[i];
            long hash = hashes[i];
            int index = (int) (hash & mask);
            int perturb = (int) hash;
            while (newTable[index] != null) {
                index = (index * 5 + 1 + perturb) & mask;
                perturb >>>= 5;
            }
            newTable[index] = value;
            newHashes[index] = hash;
        }
        return new Object[] {newTable, newHashes};
    }
}
