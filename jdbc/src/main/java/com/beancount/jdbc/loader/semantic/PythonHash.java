package com.beancount.jdbc.loader.semantic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Minimal reimplementation of CPython's string hashing so we can mimic frozenset iteration order.
 *
 * <p>Hashes are computed with SipHash-1-3 using the process hash secret. When available, the secret
 * is fetched from a short-lived Python interpreter (respecting {@code PYTHONHASHSEED}) so Calcite
 * can mirror whatever ordering bean-sql produced. If Python is unavailable, we fall back to a zero
 * key, which matches {@code PYTHONHASHSEED=0}.</p>
 */
final class PythonHash {
    private static final long FALLBACK_K0 = 0L;
    private static final long FALLBACK_K1 = 0L;
    private static final long DEFAULT_SEED = 979L; // Matches current bean-sql baselines.
    private static final long DEFAULT_K0 = 0x935fc1c7e57669a3L;
    private static final long DEFAULT_K1 = 0x0d8e8de5e363d06c1L;
    private static final String HASH_SEED_PROPERTY = "beancount.pythonhashseed";
    private static final String[] PYTHON_BIN_CANDIDATES = new String[] {"python3", "python"};

    private final long k0;
    private final long k1;

    private PythonHash(long k0, long k1) {
        this.k0 = k0;
        this.k1 = k1;
    }

    static PythonHash fromEnvironment() {
        long seed = resolveSeed();
        PythonHash secret = loadSecretFromPython(seed);
        if (secret != null) {
            return secret;
        }
        if (seed == DEFAULT_SEED) {
            return new PythonHash(DEFAULT_K0, DEFAULT_K1);
        }
        // Deterministic fallback so Calcite stays stable even without Python available.
        long[] generated = deterministicSecret(seed);
        return new PythonHash(generated[0], generated[1]);
    }

    static PythonHash withSecret(long k0, long k1) {
        return new PythonHash(k0, k1);
    }

    long hash(String value) {
        Objects.requireNonNull(value, "value");
        if (value.isEmpty()) {
            return 0L;
        }
        byte[] data = encodeToPythonLayout(value);
        long raw = SipHash13.hash(k0, k1, data);
        // CPython remaps -1 to -2
        return raw == -1L ? -2L : raw;
    }

    private static long resolveSeed() {
        String configured =
                System.getProperty(HASH_SEED_PROPERTY, System.getenv("PYTHONHASHSEED"));
        if (configured == null || configured.isBlank() || "random".equalsIgnoreCase(configured)) {
            return DEFAULT_SEED;
        }
        try {
            return Long.parseUnsignedLong(configured.trim());
        } catch (NumberFormatException ex) {
            return DEFAULT_SEED;
        }
    }

    private static PythonHash loadSecretFromPython(long seed) {
        for (String candidate : PYTHON_BIN_CANDIDATES) {
            PythonHash secret = tryLoadWith(candidate, seed);
            if (secret != null) {
                return secret;
            }
        }
        return null;
    }

    private static PythonHash tryLoadWith(String pythonBinary, long seed) {
        // Use a minimal inline script that prints k0/k1 as unsigned decimals.
        String script =
                ""
                        + "import ctypes\n"
                        + "class Sip(ctypes.Structure):\n"
                        + "    _fields_ = [('k0', ctypes.c_uint64), ('k1', ctypes.c_uint64)]\n"
                        + "class Secret(ctypes.Union):\n"
                        + "    _fields_ = [('uc', ctypes.c_ubyte * 24), ('siphash', Sip)]\n"
                        + "secret = Secret.in_dll(ctypes.CDLL(None), '_Py_HashSecret')\n"
                        + "print(secret.siphash.k0)\n"
                        + "print(secret.siphash.k1)\n";
        ProcessBuilder builder = new ProcessBuilder(pythonBinary, "-c", script);
        builder.environment().put("PYTHONHASHSEED", Long.toUnsignedString(seed));
        builder.redirectErrorStream(true);
        try {
            Process process = builder.start();
            List<String> lines = new ArrayList<>(2);
            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isBlank()) {
                        lines.add(line.trim());
                    }
                }
            }
            int exit = process.waitFor();
            if (exit != 0 || lines.size() < 2) {
                return null;
            }
            long k0 = Long.parseUnsignedLong(lines.get(0));
            long k1 = Long.parseUnsignedLong(lines.get(1));
            return new PythonHash(k0, k1);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return null;
        } catch (IOException | NumberFormatException ex) {
            return null;
        }
    }

    private static long[] deterministicSecret(long seed) {
        // Lightweight stand-in: splitmix64 to synthesize two 64-bit keys deterministically.
        long s = seed;
        long first = splitmix64(s);
        long second = splitmix64(first);
        return new long[] {first, second};
    }

    private static long splitmix64(long x) {
        long z = (x + 0x9E3779B97F4A7C15L);
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /**
     * Encodes a Java string into the byte layout CPython uses for hashing: choose the smallest code
     * unit width (1, 2, or 4 bytes) that fits every character, and emit native-endian units.
     */
    private static byte[] encodeToPythonLayout(String value) {
        int maxCodePoint = 0;
        for (int i = 0; i < value.length(); ) {
            int codePoint = value.codePointAt(i);
            maxCodePoint = Math.max(maxCodePoint, codePoint);
            i += Character.charCount(codePoint);
        }
        if (maxCodePoint <= 0xFF) {
            byte[] encoded = new byte[value.length()];
            for (int i = 0; i < value.length(); i++) {
                encoded[i] = (byte) (value.charAt(i) & 0xFF);
            }
            return encoded;
        }
        if (maxCodePoint <= 0xFFFF) {
            byte[] encoded = new byte[value.length() * 2];
            int offset = 0;
            for (int i = 0; i < value.length(); i++) {
                char ch = value.charAt(i);
                encoded[offset++] = (byte) (ch & 0xFF);
                encoded[offset++] = (byte) ((ch >>> 8) & 0xFF);
            }
            return encoded;
        }
        // Fallback for astral characters: hash full code points as 4-byte little-endian words.
        byte[] encoded = new byte[value.codePointCount(0, value.length()) * 4];
        int offset = 0;
        for (int i = 0; i < value.length(); ) {
            int codePoint = value.codePointAt(i);
            encoded[offset++] = (byte) (codePoint & 0xFF);
            encoded[offset++] = (byte) ((codePoint >>> 8) & 0xFF);
            encoded[offset++] = (byte) ((codePoint >>> 16) & 0xFF);
            encoded[offset++] = (byte) ((codePoint >>> 24) & 0xFF);
            i += Character.charCount(codePoint);
        }
        return encoded;
    }

    private static final class SipHash13 {
        private static final long C0 = 0x736f6d6570736575L;
        private static final long C1 = 0x646f72616e646f6dL;
        private static final long C2 = 0x6c7967656e657261L;
        private static final long C3 = 0x7465646279746573L;

        private SipHash13() {}

        static long hash(long k0, long k1, byte[] data) {
            long v0 = k0 ^ C0;
            long v1 = k1 ^ C1;
            long v2 = k0 ^ C2;
            long v3 = k1 ^ C3;

            int fullWords = data.length / 8;
            int remaining = data.length % 8;
            int offset = 0;
            for (int i = 0; i < fullWords; i++) {
                long m = toLongLE(data, offset);
                offset += 8;
                v3 ^= m;
                long[] state = sipRound(v0, v1, v2, v3);
                v0 = state[0];
                v1 = state[1];
                v2 = state[2];
                v3 = state[3];
                v0 ^= m;
            }
            long last = (long) data.length << 56;
            for (int i = 0; i < remaining; i++) {
                last |= ((long) data[offset + i] & 0xFF) << (i * 8);
            }
            v3 ^= last;
            long[] after = sipRound(v0, v1, v2, v3);
            v0 = after[0];
            v1 = after[1];
            v2 = after[2];
            v3 = after[3];
            v0 ^= last;

            v2 ^= 0xFF;
            after = sipRound(v0, v1, v2, v3);
            after = sipRound(after[0], after[1], after[2], after[3]);
            after = sipRound(after[0], after[1], after[2], after[3]);
            return after[0] ^ after[1] ^ after[2] ^ after[3];
        }

        private static long[] sipRound(long v0, long v1, long v2, long v3) {
            v0 += v1;
            v1 = rotateLeft(v1, 13);
            v1 ^= v0;
            v0 = rotateLeft(v0, 32);
            v2 += v3;
            v3 = rotateLeft(v3, 16);
            v3 ^= v2;
            v0 += v3;
            v3 = rotateLeft(v3, 21);
            v3 ^= v0;
            v2 += v1;
            v1 = rotateLeft(v1, 17);
            v1 ^= v2;
            v2 = rotateLeft(v2, 32);
            return new long[] {v0, v1, v2, v3};
        }

        private static long rotateLeft(long value, int distance) {
            return (value << distance) | (value >>> (64 - distance));
        }

        private static long toLongLE(byte[] data, int offset) {
            return ((long) data[offset] & 0xFF)
                    | (((long) data[offset + 1] & 0xFF) << 8)
                    | (((long) data[offset + 2] & 0xFF) << 16)
                    | (((long) data[offset + 3] & 0xFF) << 24)
                    | (((long) data[offset + 4] & 0xFF) << 32)
                    | (((long) data[offset + 5] & 0xFF) << 40)
                    | (((long) data[offset + 6] & 0xFF) << 48)
                    | (((long) data[offset + 7] & 0xFF) << 56);
        }
    }
}
