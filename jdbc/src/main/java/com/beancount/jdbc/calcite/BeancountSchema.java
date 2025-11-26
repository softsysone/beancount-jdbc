package com.beancount.jdbc.calcite;

import com.beancount.jdbc.ledger.LedgerData;
import com.beancount.jdbc.ledger.LedgerProvider;
import com.beancount.jdbc.loader.LoaderException;
import com.beancount.jdbc.schema.BalanceTable;
import com.beancount.jdbc.schema.CloseTable;
import com.beancount.jdbc.schema.DocumentTable;
import com.beancount.jdbc.schema.EntryTable;
import com.beancount.jdbc.schema.EventTable;
import com.beancount.jdbc.schema.NoteTable;
import com.beancount.jdbc.schema.OpenTable;
import com.beancount.jdbc.schema.PadTable;
import com.beancount.jdbc.schema.PostingsTable;
import com.beancount.jdbc.schema.PriceTable;
import com.beancount.jdbc.schema.QueryTable;
import com.beancount.jdbc.schema.TransactionsDetailTable;
import com.beancount.jdbc.schema.TransactionsView;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ViewTable;

/**
 * Minimal Calcite schema that currently exposes only the {@code entry} table. This keeps the
 * migration incremental: new tables/views will be layered in once entry is verified.
 */
public final class BeancountSchema extends AbstractSchema {

    private static final List<ColumnMapping> ENTRY_VIEW_COLUMNS =
            List.of(
                    ColumnMapping.entry("id"),
                    ColumnMapping.entry("date"),
                    ColumnMapping.entry("type"),
                    ColumnMapping.entry("source_filename"),
                    ColumnMapping.entry("source_lineno"));
    private static final Map<String, String> VIEW_SQL = buildViewSql();

    private final SchemaPlus parentSchema;
    private final String schemaName;
    private final Path ledgerPath;
    private volatile Map<String, Table> tables;
    private volatile LedgerData ledgerData;
    private volatile SchemaPlus schemaPlus;
    private final Object viewLock = new Object();
    private volatile boolean viewsRegistered;

    BeancountSchema(org.apache.calcite.schema.SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        this.parentSchema = Objects.requireNonNull(parentSchema, "parentSchema");
        this.schemaName = Objects.requireNonNull(name, "name");
        this.ledgerPath = resolveLedgerPath(operand);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> local = tables;
        if (local == null) {
            local = buildTables();
            tables = local;
        }
        return local;
    }

    private Map<String, Table> buildTables() {
        LedgerData data = loadLedgerData();
        Map<String, Table> map = new LinkedHashMap<>();
        map.put(EntryTable.NAME, new EntryCalciteTable(data.getEntries()));
        map.put(TransactionsDetailTable.NAME, new TransactionsDetailCalciteTable(data.getEntries()));
        map.put(OpenTable.DETAIL_NAME, new OpenDetailCalciteTable(OpenTable.materializeDetailRows(data.getOpens())));
        map.put(CloseTable.DETAIL_NAME, new CloseDetailCalciteTable(CloseTable.materializeDetailRows(data.getCloses())));
        map.put(PadTable.DETAIL_NAME, new PadDetailCalciteTable(PadTable.materializeDetailRows(data.getPads())));
        map.put(BalanceTable.DETAIL_NAME, new BalanceDetailCalciteTable(BalanceTable.materializeDetailRows(data.getBalances())));
        map.put(NoteTable.DETAIL_NAME, new NoteDetailCalciteTable(NoteTable.materializeDetailRows(data.getNotes())));
        map.put(DocumentTable.DETAIL_NAME, new DocumentDetailCalciteTable(DocumentTable.materializeDetailRows(data.getDocuments())));
        map.put(EventTable.DETAIL_NAME, new EventDetailCalciteTable(EventTable.materializeDetailRows(data.getEvents())));
        map.put(QueryTable.DETAIL_NAME, new QueryDetailCalciteTable(QueryTable.materializeDetailRows(data.getQueries())));
        map.put(PriceTable.DETAIL_NAME, new PriceDetailCalciteTable(PriceTable.materializeDetailRows(data.getPrices())));
        map.put(
                PostingsTable.NAME,
                new PostingsCalciteTable(PostingsTable.materializeRows(data.getPostings(), data.getEntries())));
        Map<String, Table> immutable = Map.copyOf(map);
        this.tables = immutable;
        registerViews();
        return immutable;
    }

    private LedgerData loadLedgerData() {
        LedgerData current = ledgerData;
        if (current == null) {
            synchronized (this) {
                current = ledgerData;
                if (current == null) {
                    try {
                        current = LedgerProvider.load(ledgerPath).getLedgerData();
                    } catch (LoaderException ex) {
                        throw new IllegalStateException("Failed to load ledger: " + ledgerPath, ex);
                    }
                    ledgerData = current;
                }
            }
        }
        return current;
    }

    private static Path resolveLedgerPath(Map<String, Object> operand) {
        Object ledger = operand.get("ledger");
        if (ledger == null) {
            throw new IllegalArgumentException("Beancount schema operand must include 'ledger'");
        }
        return Paths.get(ledger.toString()).toAbsolutePath().normalize();
    }

    private void registerViews() {
        if (viewsRegistered) {
            return;
        }
        synchronized (viewLock) {
            if (viewsRegistered) {
                return;
            }
            viewsRegistered = true;
            SchemaPlus schema = resolveSchemaPlus();
            List<String> schemaPath = buildSchemaPath(schema);
            for (Map.Entry<String, String> entry : VIEW_SQL.entrySet()) {
                List<String> viewPath = buildViewPath(schemaPath, entry.getKey());
                try {
                    var macro =
                            ViewTable.viewMacro(
                                    schema, entry.getValue(), schemaPath, viewPath, Boolean.FALSE);
                    macro.apply(Collections.emptyList());
                    schema.add(entry.getKey(), macro);
                } catch (RuntimeException ex) {
                    viewsRegistered = false;
                    throw new IllegalStateException(
                            "Failed to register Calcite view '" + entry.getKey() + "' with SQL:\n"
                                    + entry.getValue(),
                            ex);
                }
            }
        }
    }

    private SchemaPlus resolveSchemaPlus() {
        SchemaPlus local = schemaPlus;
        if (local == null) {
            SchemaPlus resolved = parentSchema.getSubSchema(schemaName);
            if (resolved == null) {
                throw new IllegalStateException("Schema not registered yet: " + schemaName);
            }
            schemaPlus = resolved;
            local = resolved;
        }
        return local;
    }

    private static List<String> buildSchemaPath(SchemaPlus schema) {
        List<String> path = new ArrayList<>();
        SchemaPlus current = schema;
        while (current != null) {
            String name = current.getName();
            if (name != null && !name.isEmpty()) {
                path.add(0, name);
            }
            current = current.getParentSchema();
        }
        return List.copyOf(path);
    }

    private static List<String> buildViewPath(List<String> schemaPath, String viewName) {
        List<String> path = new ArrayList<>(schemaPath.size() + 1);
        path.addAll(schemaPath);
        path.add(viewName);
        return List.copyOf(path);
    }

    private static Map<String, String> buildViewSql() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put(
                OpenTable.VIEW_NAME,
                buildViewSql(
                        OpenTable.DETAIL_NAME,
                        List.of(ColumnMapping.detail("account"), ColumnMapping.detail("currencies"))));
        map.put(
                CloseTable.VIEW_NAME,
                buildViewSql(
                        CloseTable.DETAIL_NAME,
                        List.of(ColumnMapping.detail("account"))));
        map.put(
                PadTable.VIEW_NAME,
                buildViewSql(
                        PadTable.DETAIL_NAME,
                        List.of(ColumnMapping.detail("account"), ColumnMapping.detail("source_account"))));
        map.put(
                BalanceTable.VIEW_NAME,
                buildViewSql(
                        BalanceTable.DETAIL_NAME,
                        List.of(
                                ColumnMapping.detail("account"),
                                ColumnMapping.detail("amount_number"),
                                ColumnMapping.detail("amount_currency"),
                                ColumnMapping.detail("diff_number"),
                                ColumnMapping.detail("diff_currency"))));
        map.put(
                NoteTable.VIEW_NAME,
                buildViewSql(
                        NoteTable.DETAIL_NAME,
                        List.of(ColumnMapping.detail("account"), ColumnMapping.detail("comment"))));
        map.put(
                DocumentTable.VIEW_NAME,
                buildViewSql(
                        DocumentTable.DETAIL_NAME,
                        List.of(ColumnMapping.detail("account"), ColumnMapping.detail("filenam"))));
        map.put(
                EventTable.VIEW_NAME,
                buildViewSql(
                        EventTable.DETAIL_NAME,
                        List.of(
                                ColumnMapping.detail("type", "event_type"),
                                ColumnMapping.detail("description"))));
        map.put(
                QueryTable.VIEW_NAME,
                buildViewSql(
                        QueryTable.DETAIL_NAME,
                        List.of(ColumnMapping.detail("name"), ColumnMapping.detail("query_string"))));
        map.put(
                PriceTable.VIEW_NAME,
                buildViewSql(
                        PriceTable.DETAIL_NAME,
                        List.of(
                                ColumnMapping.detail("currency"),
                                ColumnMapping.detail("amount_number"),
                                ColumnMapping.detail("amount_currency"))));
        map.put(
                TransactionsView.NAME,
                buildViewSql(
                        TransactionsDetailTable.NAME,
                        List.of(
                                ColumnMapping.detail("flag"),
                                ColumnMapping.detail("payee"),
                                ColumnMapping.detail("narration"),
                                ColumnMapping.detail("tags"),
                                ColumnMapping.detail("links"))));
        return Map.copyOf(map);
    }

    private static String buildViewSql(String detailName, List<ColumnMapping> detailColumns) {
        StringBuilder select = new StringBuilder("SELECT ");
        appendColumns(select, "e", ENTRY_VIEW_COLUMNS);
        if (!detailColumns.isEmpty()) {
            select.append(", ");
            appendColumns(select, "d", detailColumns);
        }
        select.append(" FROM \"entry\" AS e JOIN ")
                .append(quoteIdentifier(detailName))
                .append(" AS d ON e.\"id\" = d.\"id\"");
        return select.toString();
    }

    private static void appendColumns(StringBuilder builder, String alias, List<ColumnMapping> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            ColumnMapping column = columns.get(i);
            builder.append(alias)
                    .append('.')
                    .append(quoteIdentifier(column.source()))
                    .append(" AS ")
                    .append(quoteIdentifier(column.alias()));
        }
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private record ColumnMapping(String source, String alias, boolean entry) {
        static ColumnMapping entry(String column) {
            return new ColumnMapping(column, column, true);
        }

        static ColumnMapping detail(String column) {
            return new ColumnMapping(column, column, false);
        }

        static ColumnMapping detail(String column, String alias) {
            return new ColumnMapping(column, alias, false);
        }

        boolean isEntry() {
            return entry;
        }
    }
}
