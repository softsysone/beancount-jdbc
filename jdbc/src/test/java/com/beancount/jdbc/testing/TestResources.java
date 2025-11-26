package com.beancount.jdbc.testing;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public final class TestResources {

    private TestResources() {}

    private static final Path CLASSPATH_CACHE_DIR = initClasspathCacheDir();

    public static Path repoRoot() {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null) {
            if (Files.exists(dir.resolve("settings.gradle.kts"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("Unable to locate repository root from " + Paths.get("").toAbsolutePath());
    }

    public static Path absolutePath(String relative) {
        Path path = repoRoot().resolve(relative).normalize();
        if (!Files.exists(path)) {
            try {
                copyClasspathResource(relative, path);
            } catch (IOException ignored) {
            }
        }
        return path;
    }

    public static String calciteLedgerOperand(String relative) {
        return absolutePath(relative).toAbsolutePath().toString().replace("\\", "\\\\");
    }

    public static Path resolveResource(String path) throws IOException {
        if (path.startsWith("classpath:")) {
            String resourceName = path.substring("classpath:".length());
            return extractClasspathResource(resourceName);
        }
        return absolutePath(path);
    }

    private static Path initClasspathCacheDir() {
        try {
            Path dir = Files.createTempDirectory("beancount_test_resources");
            dir.toFile().deleteOnExit();
            return dir;
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to create classpath cache directory", ex);
        }
    }

    private static void copyClasspathResource(String resourceName, Path target) throws IOException {
        try (InputStream in = TestResources.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (in == null) {
                return;
            }
            Files.createDirectories(target.getParent());
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static Path extractClasspathResource(String resourceName) throws IOException {
        String normalized = resourceName.startsWith("/") ? resourceName.substring(1) : resourceName;
        try (InputStream in = TestResources.class.getClassLoader().getResourceAsStream(normalized)) {
            if (in == null) {
                throw new IOException("Missing classpath resource: " + normalized);
            }
            Path target = CLASSPATH_CACHE_DIR.resolve(normalized).normalize();
            Files.createDirectories(target.getParent());
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            target.toFile().deleteOnExit();
            return target;
        }
    }
}
