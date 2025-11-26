import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public final class RunQuery {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RunQuery <ledger> <sql>");
            System.exit(1);
        }
        Class.forName("com.beancount.jdbc.BeancountDriver");
        Path ledger = Path.of(args[0]).toAbsolutePath();
        try (Connection connection =
                        DriverManager.getConnection("jdbc:beancount:" + ledger.toUri());
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(args[1])) {
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= columns; i++) {
                    if (i > 1) {
                        builder.append(" | ");
                    }
                    builder.append(rs.getMetaData().getColumnLabel(i)).append("=").append(rs.getObject(i));
                }
                System.out.println(builder);
            }
        }
    }
}
