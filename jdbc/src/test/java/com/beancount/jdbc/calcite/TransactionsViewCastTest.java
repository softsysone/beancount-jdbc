package com.beancount.jdbc.calcite;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

final class TransactionsViewCastTest {

    @Test
    void castDateToVarcharFromTransactionsView() throws Exception {
        Path repoRoot = locateRepoRoot();
        Path ledger =
                repoRoot.resolve("third_party/beancount/examples/example.beancount").normalize();
        Class.forName("com.beancount.jdbc.BeancountDriver");

        assertDoesNotThrow(
                () -> {
                    try (Connection connection =
                                    DriverManager.getConnection("jdbc:beancount:" + ledger.toUri());
                            Statement statement = connection.createStatement();
                            ResultSet resultSet =
                                    statement.executeQuery(
                                            "SELECT id, CAST(\"date\" AS VARCHAR) "
                                                    + "FROM \"transactions\" "
                                                    + "ORDER BY id FETCH FIRST 5 ROWS ONLY")) {
                        while (resultSet.next()) {
                            resultSet.getInt(1);
                            resultSet.getString(2);
                        }
                    }
                });
    }

    private static Path locateRepoRoot() {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null) {
            if (dir.resolve("settings.gradle.kts").toFile().exists()) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("Unable to locate repository root");
    }
}
