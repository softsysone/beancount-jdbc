package com.beancount.jdbc.loader.semantic;
import com.beancount.jdbc.loader.BeancountAstBuilder;
import com.beancount.jdbc.loader.BeancountParseException;
import com.beancount.jdbc.loader.DecimalParser;
import com.beancount.jdbc.loader.LoaderException;
import com.beancount.jdbc.loader.LoaderMessage;
import com.beancount.jdbc.loader.DebugFlags;
import com.beancount.jdbc.loader.ast.BalanceDirectiveNode;
import com.beancount.jdbc.loader.ast.CloseDirectiveNode;
import com.beancount.jdbc.loader.ast.DirectiveNode;
import com.beancount.jdbc.loader.ast.DocumentDirectiveNode;
import com.beancount.jdbc.loader.ast.EventDirectiveNode;
import com.beancount.jdbc.loader.ast.GlobalDirectiveNode;
import com.beancount.jdbc.loader.ast.IncludeNode;
import com.beancount.jdbc.loader.ast.LedgerNode;
import com.beancount.jdbc.loader.ast.NoteDirectiveNode;
import com.beancount.jdbc.loader.ast.OpenDirectiveNode;
import com.beancount.jdbc.loader.ast.PadDirectiveNode;
import com.beancount.jdbc.loader.ast.PostingNode;
import com.beancount.jdbc.loader.ast.PriceDirectiveNode;
import com.beancount.jdbc.loader.ast.QueryDirectiveNode;
import com.beancount.jdbc.loader.ast.StatementNode;
import com.beancount.jdbc.loader.ast.TransactionMetadataNode;
import com.beancount.jdbc.loader.ast.TransactionNode;
import com.beancount.jdbc.loader.ast.SourceLocation;
import com.beancount.jdbc.ledger.BalanceRecord;
import com.beancount.jdbc.ledger.CloseRecord;
import com.beancount.jdbc.ledger.DocumentRecord;
import com.beancount.jdbc.ledger.EventRecord;
import com.beancount.jdbc.ledger.LedgerData;
import com.beancount.jdbc.ledger.LedgerEntry;
import com.beancount.jdbc.ledger.NoteRecord;
import com.beancount.jdbc.ledger.OpenRecord;
import com.beancount.jdbc.ledger.PadRecord;
import com.beancount.jdbc.ledger.PostingRecord;
import com.beancount.jdbc.ledger.PriceRecord;
import com.beancount.jdbc.ledger.QueryRecord;
import com.beancount.jdbc.ledger.TransactionPayload;
import com.beancount.jdbc.loader.semantic.display.DisplayContext;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Deque;
import java.util.Set;
public final class SemanticAnalyzer {
    private final BeancountAstBuilder astBuilder = new BeancountAstBuilder();
    private static final PythonSetOrdering PYTHON_SET_ORDERING =
            new PythonSetOrdering(PythonHash.fromEnvironment());
    private static final DateTimeFormatter FLEXIBLE_DATE_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendValue(ChronoField.YEAR, 4)
                    .appendLiteral('-')
                    .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
                    .appendLiteral('-')
                    .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
                    .toFormatter();
    private static final Set<String> SUPPORTED_DIRECTIVES =
            Set.of("open", "close", "pad", "balance", "note", "document", "event", "query", "price");
    private static final Map<String, Integer> ENTRY_TYPE_ORDER =
            Map.of("open", -2, "balance", -1, "document", 1, "close", 2);
    private static final Comparator<DirectiveDescriptor> DIRECTIVE_COMPARATOR =
            Comparator.comparing(DirectiveDescriptor::date)
                    .thenComparingInt(descriptor -> ENTRY_TYPE_ORDER.getOrDefault(descriptor.type(), 0))
                    .thenComparingInt(DirectiveDescriptor::sourceLineno)
                    .thenComparingInt(DirectiveDescriptor::tempId);
    private static final Comparator<LedgerEntry> LEDGER_ENTRY_COMPARATOR =
            Comparator.comparing(LedgerEntry::getDate)
                    .thenComparingInt(entry -> ENTRY_TYPE_ORDER.getOrDefault(entry.getType(), 0))
                    .thenComparingInt(LedgerEntry::getSourceLineno)
                    .thenComparingInt(LedgerEntry::getId);
    public SemanticAnalysis analyze(Path ledgerPath) throws LoaderException {
        Objects.requireNonNull(ledgerPath, "ledgerPath");
        AnalyzerState state = new AnalyzerState();
        processFile(ledgerPath.toAbsolutePath().normalize(), state);
        finalizeState(state);
        assignEntryIds(state);
        bookInventoryLots(state);
        reorderPostingsByCurrencyBuckets(state);
        assignPostingIds(state);
        rebuildPostings(state);
        LedgerData ledgerData = buildLedgerData(state);
        SemanticLedger ledger =
                new SemanticLedger(
                        state.transactions,
                        state.openAccounts,
                        dedupePreserveOrder(state.operatingCurrencies),
                        state.displayContext.copy());
        return new SemanticAnalysis(ledgerData, ledger, state.messages);
    }
    private void processFile(Path file, AnalyzerState state) throws LoaderException {
        if (!Files.exists(file)) {
            state.messages.add(
                    new LoaderMessage(
                            LoaderMessage.Level.ERROR,
                            "Ledger file not found: " + file,
                            file.toString(),
                            0));
            return;
        }
        if (!state.activeFiles.add(file)) {
            state.messages.add(
                    new LoaderMessage(
                            LoaderMessage.Level.ERROR,
                            "Recursive include detected: " + file,
                            file.toString(),
                            0));
            return;
        }
        LedgerNode ledger;
        try {
            String contents = Files.readString(file, StandardCharsets.UTF_8);
            ledger = astBuilder.parse(file.toString(), contents);
            if (DebugFlags.isTokenDebugEnabled()) {
                for (String tokenLine : DebugFlags.drainCapturedTokens()) {
                    state.tokenLog.add(tokenLine);
                    state.messages.add(
                            new LoaderMessage(
                                    LoaderMessage.Level.INFO,
                                    "[tokens] " + tokenLine,
                                    file.toString(),
                                    0));
                }
            }
            if (DebugFlags.isParserTraceEnabled()) {
                for (String diagnostic : DebugFlags.drainCapturedDiagnostics()) {
                    state.diagnosticLog.add(diagnostic);
                    state.messages.add(
                            new LoaderMessage(
                                    LoaderMessage.Level.INFO,
                                    "[diagnostic] " + diagnostic,
                                    file.toString(),
                                    0));
                }
            }
            state.tokenLog.clear();
            state.diagnosticLog.clear();
        } catch (IOException ex) {
            throw new LoaderException("Failed to read ledger: " + file, ex);
        } catch (BeancountParseException ex) {
            if (DebugFlags.isTokenDebugEnabled()) {
                for (String tokenLine : DebugFlags.drainCapturedTokens()) {
                    state.tokenLog.add(tokenLine);
                    state.messages.add(
                            new LoaderMessage(
                                    LoaderMessage.Level.INFO,
                                    "[tokens] " + tokenLine,
                                    file.toString(),
                                    0));
                }
            }
            if (DebugFlags.isParserTraceEnabled()) {
                for (String diagnostic : DebugFlags.drainCapturedDiagnostics()) {
                    state.diagnosticLog.add(diagnostic);
                    state.messages.add(
                            new LoaderMessage(
                                    LoaderMessage.Level.INFO,
                                    "[diagnostic] " + diagnostic,
                                    file.toString(),
                                    0));
                }
            }
            StringBuilder message = new StringBuilder(ex.getMessage());
            if (!state.tokenLog.isEmpty()) {
                message.append("\nRecent tokens:\n");
                int start = Math.max(0, state.tokenLog.size() - 10);
                for (int i = start; i < state.tokenLog.size(); i++) {
                    message.append("  ").append(state.tokenLog.get(i)).append('\n');
                }
            }
            if (!state.diagnosticLog.isEmpty()) {
                message.append("\nDiagnostics:\n");
                int start = Math.max(0, state.diagnosticLog.size() - 10);
                for (int i = start; i < state.diagnosticLog.size(); i++) {
                    message.append("  ").append(state.diagnosticLog.get(i)).append('\n');
                }
            }
            throw new LoaderException(
                    "[Version " + com.beancount.jdbc.Version.FULL + "] Failed to parse ledger: "
                            + file
                            + " ("
                            + message.toString().trim()
                            + ")",
                    ex);
        }
        for (StatementNode statement : ledger.getStatements()) {
            if (statement instanceof IncludeNode include) {
                List<Path> resolvedPaths = resolveIncludePaths(file, include, state);
                if (resolvedPaths.isEmpty()) {
                    continue;
                }
                for (Path resolved : resolvedPaths) {
                    if (include.isIncludeOnce() && !state.includeOnceVisited.add(resolved)) {
                        continue;
                    }
                    processFile(resolved, state);
                }
                continue;
            } else if (statement instanceof TransactionNode transaction) {
                processTransaction(file, transaction, state);
            } else if (statement instanceof DirectiveNode directive) {
                processDirective(file, directive, state);
            } else if (statement instanceof GlobalDirectiveNode global) {
                processGlobalDirective(file, global, state);
            }
        }
        state.activeFiles.remove(file);
    }
    private static List<Path> resolveIncludePaths(Path currentFile, IncludeNode include, AnalyzerState state) {
        String rawPath = include.getPath();
        boolean glob = containsGlob(rawPath);
        Path parent = currentFile.getParent();
        Path baseDir = parent != null ? parent : Path.of("").toAbsolutePath();
        if (!glob) {
            Path path = Path.of(rawPath);
            if (!path.isAbsolute()) {
                path = baseDir.resolve(path);
            }
            return List.of(path.normalize());
        }
        List<Path> matches = resolveGlobPaths(baseDir, rawPath);
        if (matches.isEmpty()) {
            state.messages.add(
                    new LoaderMessage(
                            LoaderMessage.Level.WARNING,
                            "Include pattern matched no files: " + rawPath,
                            currentFile.toString(),
                            include.getLocation().getLine()));
        }
        return matches;
    }

    private static boolean containsGlob(String path) {
        for (int i = 0; i < path.length(); i++) {
            char ch = path.charAt(i);
            if (ch == '*' || ch == '?' || ch == '{' || ch == '[') {
                return true;
            }
        }
        return false;
    }

    private static List<Path> resolveGlobPaths(Path baseDir, String rawPath) {
        String normalized = rawPath.replace('\\', '/');
        int globIndex = firstGlobIndex(normalized);
        if (globIndex < 0) {
            Path path = convertToPath(baseDir, normalized);
            return path == null ? List.of() : List.of(path);
        }
        String prefix = normalized.substring(0, globIndex);
        String pattern = normalized.substring(globIndex);
        Path searchRoot = convertToPath(baseDir, prefix);
        if (searchRoot == null) {
            return List.of();
        }
        if (!Files.exists(searchRoot)) {
            return List.of();
        }
        String systemPattern = convertSeparators(pattern);
        PathMatcher matcher = searchRoot.getFileSystem().getPathMatcher("glob:" + systemPattern);
        List<Path> matches = new ArrayList<>();
        try (var stream = Files.walk(searchRoot)) {
            stream.filter(Files::isRegularFile)
                    .forEach(
                            candidate -> {
                                Path relative = searchRoot.relativize(candidate);
                                if (matcher.matches(relative)) {
                                    matches.add(candidate.normalize());
                                }
                            });
        } catch (IOException ignored) {
        }
        return matches;
    }

    private static int firstGlobIndex(String value) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '*' || ch == '?' || ch == '{' || ch == '[') {
                return i;
            }
        }
        return -1;
    }

    private static Path convertToPath(Path baseDir, String raw) {
        String system = convertSeparators(raw);
        try {
            if (system.isEmpty()) {
                return baseDir;
            }
            Path path = Path.of(system);
            if (!path.isAbsolute()) {
                path = baseDir.resolve(path);
            }
            return path.normalize();
        } catch (Exception ex) {
            return null;
        }
    }

    private static String convertSeparators(String path) {
        String separator = FileSystems.getDefault().getSeparator();
        String normalized = path.replace("\\", "/");
        if ("/".equals(separator)) {
            return normalized;
        }
        return normalized.replace("/", separator);
    }

    private void processDirective(Path file, DirectiveNode directive, AnalyzerState state) {
        LocalDate date;
        try {
            date = LocalDate.parse(directive.getDate(), FLEXIBLE_DATE_FORMATTER);
        } catch (DateTimeParseException ex) {
            state.messages.add(
                    new LoaderMessage(
                            LoaderMessage.Level.ERROR,
                            "Invalid date: " + directive.getDate(),
                            file.toString(),
                            directive.getLocation().getLine()));
            return;
        }
        String rawType = directive.getDirectiveType() == null ? "" : directive.getDirectiveType();
        String normalizedType = rawType.toLowerCase(Locale.ROOT);
        int entryId = state.nextEntryId++;
        boolean supported = SUPPORTED_DIRECTIVES.contains(normalizedType);
        state.directiveDescriptors.add(
                new DirectiveDescriptor(
                        entryId,
                        date,
                        normalizedType,
                        file.toString(),
                        directive.getLocation().getLine()));
        if (!supported) {
            state.observedDirectives.add(directive);
            return;
        }
        ParsedDirective parsed =
                new ParsedDirective(
                        entryId,
                        date,
                        normalizedType,
                        file.toString(),
                        directive.getLocation().getLine(),
                        supported);
        state.directives.add(parsed);
        state.directivesById.put(entryId, parsed);
        processNonTransaction(file, directive, date, entryId, normalizedType, parsed, state);
    }
    private void processGlobalDirective(
            Path file, GlobalDirectiveNode directive, AnalyzerState state) {
        String type = directive.getDirectiveType().toLowerCase(Locale.ROOT);
        switch (type) {
            case "option" -> handleOption(file, directive, state);
            case "plugin" -> handlePlugin(file, directive, state);
            case "popt" -> handlePopt(file, directive, state);
            case "push" -> handlePush(file, directive, state);
            case "pop" -> handlePop(file, directive, state);
            case "pushtag" -> handlePushTag(file, directive, state);
            case "poptag" -> handlePopTag(file, directive, state);
            case "pushmeta" -> handlePushMeta(file, directive, state);
            case "popmeta" -> handlePopMeta(file, directive, state);
            default ->
                    state.messages.add(
                            new LoaderMessage(
                                    LoaderMessage.Level.INFO,
                                    "Unhandled global directive: " + directive.getDirectiveType(),
                                    file.toString(),
                                    directive.getLocation().getLine()));
        }
    }
    private void processTransaction(Path file, TransactionNode transaction, AnalyzerState state) {
        LocalDate date;
        try {
            date = LocalDate.parse(transaction.getDate(), FLEXIBLE_DATE_FORMATTER);
        } catch (DateTimeParseException ex) {
            state.messages.add(
                    new LoaderMessage(
                            LoaderMessage.Level.ERROR,
                            "Invalid date: " + transaction.getDate(),
                            file.toString(),
                            transaction.getLocation().getLine()));
            return;
        }
        int entryId = state.nextEntryId++;
        ParsedDirective parsedDirective =
                new ParsedDirective(
                        entryId,
                        date,
                        "txn",
                        file.toString(),
                        transaction.getLocation().getLine(),
                        true);
        state.directives.add(parsedDirective);
        state.directivesById.put(entryId, parsedDirective);
        if (transaction.isUsingPipeSeparator() && !state.allowPipeSeparator) {
            state.messages.add(
                    new LoaderMessage(
                            LoaderMessage.Level.ERROR,
                            "Pipe symbol is deprecated.",
                            file.toString(),
                            transaction.getLocation().getLine()));
        }
        LinkedHashSet<String> tagSet = new LinkedHashSet<>();
        for (Iterator<String> it = state.tagStack.descendingIterator(); it.hasNext(); ) {
            addNormalizedTag(tagSet, it.next());
        }
        for (String tag : toNonEmptyList(transaction.getTags())) {
            addNormalizedTag(tagSet, tag);
        }
        LinkedHashSet<String> linkSet = new LinkedHashSet<>(toNonEmptyList(transaction.getLinks()));
        List<String> transactionComments = new ArrayList<>();
        for (String comment : transaction.getComments()) {
            if (comment == null) {
                continue;
            }
            String trimmed = comment.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            transactionComments.add(trimmed);
            extractAnchorsFromComment(trimmed, tagSet, linkSet);
        }
        List<String> tags = PYTHON_SET_ORDERING.iterate(tagSet);
        List<String> links = PYTHON_SET_ORDERING.iterate(linkSet);
        TransactionPayload payload =
                new TransactionPayload(
                        transaction.getFlag(),
                        transaction.getPayee(),
                        transaction.getNarration(),
                        tags.isEmpty() ? "" : String.join(",", tags),
                        links.isEmpty() ? "" : String.join(",", links));
        LedgerEntry entry =
                new LedgerEntry(
                        entryId,
                        date,
                        "txn",
                        file.toString(),
                        transaction.getLocation().getLine(),
                        payload);
        state.entries.add(entry);
        parsedDirective.setLedgerEntry(entry);
        state.directiveDescriptors.add(
                new DirectiveDescriptor(
                        entryId,
                        date,
                        "txn",
                        file.toString(),
                        transaction.getLocation().getLine()));
        List<SemanticMetadataEntry> metadataEntries = new ArrayList<>();
        for (TransactionMetadataNode metadata : transaction.getMetadata()) {
            metadataEntries.add(new SemanticMetadataEntry(metadata.getKey(), metadata.getValue()));
        }
        for (Iterator<MetadataEntry> it = state.metadataStack.descendingIterator(); it.hasNext(); ) {
            MetadataEntry entryMeta = it.next();
            metadataEntries.add(new SemanticMetadataEntry(entryMeta.key, entryMeta.value));
        }
        List<PostingRecord> postingRecords = new ArrayList<>();
        for (PostingNode posting : transaction.getPostings()) {
            List<SemanticMetadataEntry> postingMetadata = new ArrayList<>();
            for (TransactionMetadataNode metadata : posting.getMetadata()) {
                postingMetadata.add(new SemanticMetadataEntry(metadata.getKey(), metadata.getValue()));
            }
            List<String> postingComments = new ArrayList<>();
            for (String comment : posting.getComments()) {
                if (comment != null && !comment.trim().isEmpty()) {
                    postingComments.add(comment.trim());
                }
            }
            LocalDate costDate = posting.getCostDate();
            if (costDate == null
                    && posting.getCostNumber() != null
                    && posting.getAmountNumber() != null
                    && posting.getAmountNumber().signum() > 0) {
                costDate = date;
            }
            PostingRecord record =
                    new PostingRecord(
                            state.nextPostingId++,
                            entryId,
                            posting.getFlag(),
                            posting.getAccount(),
                            posting.getAmountNumber(),
                            posting.getAmountCurrency(),
                            posting.getCostNumber(),
                            posting.getCostCurrency(),
                            costDate,
                            posting.getCostLabel(),
                            posting.getPriceNumber(),
                            posting.getPriceCurrency());
            postingRecords.add(record);
            validateAccount(posting.getAccount(), posting.getLocation(), file, state);
            if (!postingMetadata.isEmpty() || !postingComments.isEmpty()) {
                state.postingExtras.put(
                        record.getPostingId(), new PostingExtras(postingMetadata, postingComments));
            }
        }
        List<PostingRecord> expandedRecords = expandAutoPostings(postingRecords);
        List<PostingRecord> normalizedRecords = inferMissingPostingNumbers(expandedRecords);
        for (PostingRecord record : normalizedRecords) {
            state.postings.add(record);
            state.rawPostings.add(record);
        }
        SemanticTransaction semanticTransaction =
                new SemanticTransaction(
                        date,
                        transaction.getDirectiveType(),
                        payload.getFlag(),
                        payload.getPayee(),
                        payload.getNarration(),
                        tags,
                        links,
                        metadataEntries,
                        toSemanticPostings(normalizedRecords, state.postingExtras),
                        transactionComments,
                        transaction.getLocation());
        state.transactions.add(semanticTransaction);
    }
    private void processNonTransaction(
            Path file,
            DirectiveNode directive,
            LocalDate date,
            int entryId,
            String entryType,
            ParsedDirective parsedDirective,
            AnalyzerState state) {
        LedgerEntry entry =
                new LedgerEntry(
                        entryId,
                        date,
                        entryType,
                        file.toString(),
                        directive.getLocation().getLine(),
                        null);
        state.entries.add(entry);
        parsedDirective.setLedgerEntry(entry);
        if (directive instanceof OpenDirectiveNode open) {
            OpenRecord record = new OpenRecord(entryId, open.getAccount(), open.getCurrencies());
            state.opens.add(record);
            if (record.getAccount() != null && !record.getAccount().isEmpty()) {
                state.openAccounts.add(record.getAccount());
            }
        } else if (directive instanceof CloseDirectiveNode close) {
            CloseRecord record = new CloseRecord(entryId, close.getAccount());
            state.closes.add(record);
            validateAccount(record.getAccount(), directive.getLocation(), file, state);
        } else if (directive instanceof PadDirectiveNode pad) {
            PadRecord record = new PadRecord(entryId, pad.getAccount(), pad.getSourceAccount());
            state.pads.add(record);
            validateAccount(record.getAccount(), directive.getLocation(), file, state);
            validateAccount(record.getSourceAccount(), directive.getLocation(), file, state);
            PadContext context = new PadContext(record, entry, directive.getLocation());
            state.padContextsByEntryId.put(entryId, context);
        } else if (directive instanceof BalanceDirectiveNode balance) {
            BalanceRecord record =
                    new BalanceRecord(
                            entryId,
                            balance.getAccount(),
                            balance.getAmountNumber(),
                            balance.getAmountCurrency(),
                            balance.getDiffNumber(),
                            balance.getDiffCurrency(),
                            balance.getToleranceNumber(),
                            balance.getToleranceCurrency());
            state.balances.add(record);
            validateAccount(record.getAccount(), directive.getLocation(), file, state);
        } else if (directive instanceof NoteDirectiveNode note) {
            NoteRecord record = new NoteRecord(entryId, note.getAccount(), note.getComment());
            state.notes.add(record);
            validateAccount(record.getAccount(), directive.getLocation(), file, state);
        } else if (directive instanceof DocumentDirectiveNode document) {
            DocumentRecord record = new DocumentRecord(entryId, document.getAccount(), document.getFilename());
            state.documents.add(record);
            validateAccount(record.getAccount(), directive.getLocation(), file, state);
        } else if (directive instanceof EventDirectiveNode event) {
            EventRecord record = new EventRecord(entryId, event.getEventType(), event.getDescription());
            state.events.add(record);
        } else if (directive instanceof QueryDirectiveNode query) {
            QueryRecord record = new QueryRecord(entryId, query.getName(), query.getQueryString());
            state.queries.add(record);
            if (record.getName() == null || record.getName().isEmpty()) {
                state.messages.add(
                        new LoaderMessage(
                                LoaderMessage.Level.WARNING,
                                "Query directive missing name",
                                file.toString(),
                                directive.getLocation().getLine()));
            }
        } else if (directive instanceof PriceDirectiveNode price) {
            PriceRecord record =
                    new PriceRecord(entryId, price.getCurrency(), price.getAmountNumber(), price.getAmountCurrency());
            state.prices.add(record);
            if (record.getCurrency() == null || record.getCurrency().isEmpty()) {
                state.messages.add(
                        new LoaderMessage(
                                LoaderMessage.Level.WARNING,
                                "Price directive missing currency",
                                file.toString(),
                                directive.getLocation().getLine()));
            }
        } else {
            state.observedDirectives.add(directive);
        }
    }
    private static List<String> toNonEmptyList(List<String> values) {
        List<String> cleaned = new ArrayList<>();
        for (String value : values) {
            String trimmed = value.trim();
            if (!trimmed.isEmpty()) {
                cleaned.add(trimmed);
            }
        }
        return cleaned;
    }
    private static void extractAnchorsFromComment(
            String comment, Set<String> tags, Set<String> links) {
        if (comment == null) {
            return;
        }
        String[] tokens = comment.split("\\s+");
        for (String token : tokens) {
            if (token.startsWith("#") && token.length() > 1) {
                tags.add(token.substring(1));
            } else if (token.startsWith("^") && token.length() > 1) {
                links.add(token.substring(1));
            }
        }
    }
    private static void addNormalizedTag(Set<String> tags, String rawTag) {
        String normalized = normalizeTagName(rawTag);
        if (!normalized.isEmpty()) {
            tags.add(normalized);
        }
    }

    private static String normalizeTagName(String tag) {
        if (tag == null) {
            return "";
        }
        String trimmed = tag.trim();
        if (trimmed.startsWith("#")) {
            trimmed = trimmed.substring(1);
        }
        return trimmed;
    }
    private void validateAccount(
            String account, SourceLocation location, Path file, AnalyzerState state) {
        if (account == null || account.isEmpty()) {
            return;
        }
        if (account.startsWith("Equity:")) {
            return;
        }
        if (!state.openAccounts.contains(account)) {
            state.messages.add(
                    new LoaderMessage(
                            LoaderMessage.Level.WARNING,
                            "Account used before open: " + account,
                            file.toString(),
                            location != null ? location.getLine() : 0));
        }
    }
    private void handleOption(Path file, GlobalDirectiveNode directive, AnalyzerState state) {
        List<String> arguments = directive.getArguments();
        if (arguments.isEmpty()) {
            recordWarning(
                    state,
                    file,
                    directive,
                    "option directive missing name/value; ignoring");
            return;
        }
        String name = arguments.get(0).toLowerCase(Locale.ROOT);
        String value = arguments.size() > 1 ? arguments.get(1) : "";
        switch (name) {
            case "tolerance_multiplier":
            case "inferred_tolerance_multiplier": {
                BigDecimal parsed = parseOptionDecimal(value);
                if (parsed != null) {
                    state.toleranceMultiplier = parsed;
                } else {
                    recordWarning(
                            state,
                            file,
                            directive,
                            "invalid tolerance_multiplier value: " + value);
                }
                break;
            }
            case "inferred_tolerance_default": {
                String[] parts = value.split(":", 2);
                if (parts.length != 2) {
                    recordWarning(
                            state,
                            file,
                            directive,
                            "invalid inferred_tolerance_default format; expected CURRENCY:value");
                    break;
                }
                BigDecimal parsed = parseOptionDecimal(parts[1]);
                if (parsed == null) {
                    recordWarning(
                            state,
                            file,
                            directive,
                            "invalid inferred_tolerance_default value: " + value);
                    break;
                }
                String currency = parts[0].trim();
                if ("*".equals(currency)) {
                    state.defaultToleranceOverride = parsed;
                } else {
                    state.toleranceOverrides.put(currency, parsed);
                }
                break;
            }
            case "infer_tolerance_from_cost": {
                Boolean parsed = parseOptionBoolean(value);
                if (parsed != null) {
                    state.inferToleranceFromCost = parsed;
                } else {
                    recordWarning(
                            state,
                            file,
                            directive,
                            "invalid infer_tolerance_from_cost value: " + value);
                }
                break;
            }
            case "operating_currency": {
                if (value == null || value.isBlank()) {
                    recordWarning(state, file, directive, "operating_currency requires a currency code");
                } else {
                    state.operatingCurrencies.add(value.trim());
                }
                break;
            }
            case "tolerance": {
                BigDecimal parsed = parseOptionDecimal(value);
                if (parsed != null) {
                    state.defaultToleranceOverride = parsed.abs();
                } else {
                    recordWarning(state, file, directive, "invalid tolerance value: " + value);
                }
                break;
            }
            case "tolerance_map": {
                String[] parts = value.split(":", 2);
                if (parts.length != 2) {
                    recordWarning(
                            state,
                            file,
                            directive,
                            "invalid tolerance_map format; expected CURRENCY:value");
                    break;
                }
                BigDecimal parsed = parseOptionDecimal(parts[1]);
                if (parsed == null) {
                    recordWarning(state, file, directive, "invalid tolerance_map value: " + value);
                    break;
                }
                String currency = parts[0].trim();
                if (currency.isEmpty()) {
                    recordWarning(state, file, directive, "tolerance_map currency missing");
                    break;
                }
                state.toleranceOverrides.put(currency, parsed.abs());
                break;
            }
            case "render_commas": {
                Boolean parsed = parseOptionBoolean(value);
                if (parsed == null) {
                    recordWarning(state, file, directive, "invalid render_commas value: " + value);
                } else {
                    state.displayContext.setRenderCommas(parsed);
                }
                break;
            }
            case "allow_pipe_separator": {
                Boolean parsed = parseOptionBoolean(value);
                if (parsed == null) {
                    recordWarning(state, file, directive, "invalid allow_pipe_separator value: " + value);
                } else {
                    state.allowPipeSeparator = parsed;
                }
                break;
            }
            case "display_precision": {
                String[] parts = value.split(":", 2);
                if (parts.length != 2) {
                    recordWarning(
                            state, file, directive, "invalid display_precision format; expected CURRENCY:value");
                    break;
                }
                BigDecimal parsed = parseOptionDecimal(parts[1]);
                if (parsed == null) {
                    recordWarning(state, file, directive, "invalid display_precision value: " + value);
                    break;
                }
                String currency = parts[0].trim();
                if (currency.isEmpty()) {
                    recordWarning(state, file, directive, "display_precision currency missing");
                    break;
                }
                BigDecimal normalized = parsed.stripTrailingZeros();
                int scale = Math.max(0, normalized.scale());
                state.displayContext.setFixedPrecision(currency, scale);
                break;
            }
            default:
                logGlobalDirective(
                        file, directive, state, "option directive not yet supported");
        }
    }
    private void handlePlugin(Path file, GlobalDirectiveNode directive, AnalyzerState state) {
        String pluginName =
                directive.getArguments().isEmpty() ? "<unknown>" : directive.getArguments().get(0);
        recordWarning(
                state,
                file,
                directive,
                "Plugin '"
                        + pluginName
                        + "' ignored. Calcite JDBC currently does not execute plugins, so results may differ from bean-sql.");
    }
    private void handlePopt(Path file, GlobalDirectiveNode directive, AnalyzerState state) {
        logGlobalDirective(file, directive, state, "popt directive not yet supported");
    }
    private void handlePush(Path file, GlobalDirectiveNode directive, AnalyzerState state) {
        logGlobalDirective(file, directive, state, "push directive not yet supported");
    }
    private void handlePop(Path file, GlobalDirectiveNode directive, AnalyzerState state) {
        logGlobalDirective(file, directive, state, "pop directive not yet supported");
    }
    private void handlePushTag(Path file, GlobalDirectiveNode directive, AnalyzerState state) {
        String tag = normalizeTagName(firstArgument(directive));
        if (tag == null || tag.isEmpty()) {
            recordWarning(state, file, directive, "pushtag requires a tag name");
            return;
        }
        state.tagStack.push(tag);
    }
    private void handlePopTag(Path file, GlobalDirectiveNode directive, AnalyzerState state) {
        if (state.tagStack.isEmpty()) {
            recordWarning(state, file, directive, "poptag with empty stack");
            return;
        }
        String requested = normalizeTagName(firstArgument(directive));
        if (requested == null || requested.isEmpty()) {
            state.tagStack.pop();
            return;
        }
        if (!removeFromStack(state.tagStack, requested)) {
            recordWarning(state, file, directive, "poptag could not find tag: " + requested);
        }
    }
    private void handlePushMeta(Path file, GlobalDirectiveNode directive, AnalyzerState state) {
        MetadataEntry entry = parseMetadataArgument(firstArgument(directive));
        if (entry == null) {
            recordWarning(state, file, directive, "pushmeta requires \"key value\"");
            return;
        }
        state.metadataStack.push(entry);
    }
    private void handlePopMeta(Path file, GlobalDirectiveNode directive, AnalyzerState state) {
        if (state.metadataStack.isEmpty()) {
            recordWarning(state, file, directive, "popmeta with empty stack");
            return;
        }
        String key = firstArgument(directive);
        if (key == null || key.isEmpty()) {
            state.metadataStack.pop();
            return;
        }
        if (!removeMetadataFromStack(state.metadataStack, key)) {
            recordWarning(state, file, directive, "popmeta could not find metadata key: " + key);
        }
    }
    private void logGlobalDirective(
            Path file, GlobalDirectiveNode directive, AnalyzerState state, String details) {
        List<String> arguments = directive.getArguments();
        String content =
                arguments.isEmpty()
                        ? ""
                        : " -> " + String.join(" ", arguments);
        String message =
                directive.getDirectiveType()
                        + ": "
                        + details
                        + content;
        state.messages.add(
                new LoaderMessage(
                        LoaderMessage.Level.INFO,
                        message,
                        file.toString(),
                        directive.getLocation().getLine()));
    }
    private void recordWarning(
            AnalyzerState state, Path file, GlobalDirectiveNode directive, String message) {
        state.messages.add(
                new LoaderMessage(
                        LoaderMessage.Level.WARNING,
                        message,
                        file.toString(),
                        directive.getLocation().getLine()));
    }
    private static BigDecimal parseOptionDecimal(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            return DecimalParser.parse(value);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
    private static List<String> dedupePreserveOrder(List<String> values) {
        if (values.isEmpty()) {
            return List.of();
        }
        LinkedHashSet<String> set = new LinkedHashSet<>();
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                set.add(value);
            }
        }
        return List.copyOf(set);
    }
    private static String stripQuotes(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        if ((value.startsWith("\"") && value.endsWith("\""))
                || (value.startsWith("'") && value.endsWith("'"))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
    private static boolean removeFromStack(Deque<String> stack, String target) {
        Deque<String> buffer = new ArrayDeque<>();
        boolean removed = false;
        while (!stack.isEmpty()) {
            String current = stack.pop();
            if (!removed && current.equals(target)) {
                removed = true;
                break;
            }
            buffer.push(current);
        }
        while (!buffer.isEmpty()) {
            stack.push(buffer.pop());
        }
        return removed;
    }
    private static boolean removeMetadataFromStack(Deque<MetadataEntry> stack, String key) {
        Deque<MetadataEntry> buffer = new ArrayDeque<>();
        boolean removed = false;
        while (!stack.isEmpty()) {
            MetadataEntry entry = stack.pop();
            if (!removed && entry.key.equals(key)) {
                removed = true;
                break;
            }
            buffer.push(entry);
        }
        while (!buffer.isEmpty()) {
            stack.push(buffer.pop());
        }
        return removed;
    }
    private static Boolean parseOptionBoolean(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return null;
        }
        if (normalized.equals("true") || normalized.equals("yes") || normalized.equals("1")) {
            return Boolean.TRUE;
        }
        if (normalized.equals("false") || normalized.equals("no") || normalized.equals("0")) {
            return Boolean.FALSE;
        }
        return null;
    }
    private static String firstArgument(GlobalDirectiveNode directive) {
        if (directive.getArguments().isEmpty()) {
            return null;
        }
        return directive.getArguments().get(0).trim();
    }
    private MetadataEntry parseMetadataArgument(String raw) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        String key;
        String value;
        int colon = raw.indexOf(':');
        if (colon >= 0) {
            key = raw.substring(0, colon).trim();
            value = raw.substring(colon + 1).trim();
        } else {
            int space = raw.indexOf(' ');
            if (space < 0) {
                return null;
            }
            key = raw.substring(0, space).trim();
            value = raw.substring(space + 1).trim();
        }
        if (key.isEmpty()) {
            return null;
        }
        value = stripQuotes(value);
        return new MetadataEntry(key, value);
    }
    private static List<PostingRecord> expandAutoPostings(List<PostingRecord> postings) {
        if (postings.isEmpty()) {
            return postings;
        }
        List<IndexedPosting> autoPostings = new ArrayList<>();
        List<IndexedPosting> uncategorized = new ArrayList<>();
        LinkedHashMap<String, List<IndexedPosting>> groups = new LinkedHashMap<>();
        for (int i = 0; i < postings.size(); i++) {
            PostingRecord record = postings.get(i);
            if (isAutoPosting(record)) {
                autoPostings.add(new IndexedPosting(i, record));
                continue;
            }
            String currency = primaryCurrency(record);
            if (currency != null) {
                groups.computeIfAbsent(currency, key -> new ArrayList<>()).add(new IndexedPosting(i, record));
            } else {
                uncategorized.add(new IndexedPosting(i, record));
            }
        }
        if (autoPostings.isEmpty() || groups.size() <= 1) {
            return postings;
        }
        for (IndexedPosting auto : autoPostings) {
            for (Map.Entry<String, List<IndexedPosting>> entry : groups.entrySet()) {
                PostingRecord clone =
                        new PostingRecord(
                                auto.posting.getPostingId(),
                                auto.posting.getEntryId(),
                                auto.posting.getFlag(),
                                auto.posting.getAccount(),
                                null,
                                entry.getKey(),
                                auto.posting.getCostNumber(),
                                auto.posting.getCostCurrency(),
                                auto.posting.getCostDate(),
                                auto.posting.getCostLabel(),
                                auto.posting.getPriceNumber(),
                                auto.posting.getPriceCurrency());
                entry.getValue().add(new IndexedPosting(auto.index, clone));
            }
        }
        List<PostingRecord> expanded = new ArrayList<>();
        for (List<IndexedPosting> entries : groups.values()) {
            entries.sort(Comparator.comparingInt(indexed -> indexed.index));
            for (IndexedPosting indexed : entries) {
                expanded.add(indexed.posting);
            }
        }
        if (!uncategorized.isEmpty()) {
            uncategorized.sort(Comparator.comparingInt(indexed -> indexed.index));
            for (IndexedPosting indexed : uncategorized) {
                expanded.add(indexed.posting);
            }
        }
        return expanded;
    }

    private static boolean isAutoPosting(PostingRecord record) {
        return record.getNumber() == null
                && record.getCurrency() == null
                && record.getCostCurrency() == null
                && record.getCostNumber() == null
                && record.getPriceNumber() == null
                && record.getPriceCurrency() == null;
    }

    private static String primaryCurrency(PostingRecord record) {
        if (record.getCostCurrency() != null) {
            return record.getCostCurrency();
        }
        if (record.getPriceCurrency() != null) {
            return record.getPriceCurrency();
        }
        return record.getCurrency();
    }

    private static final class IndexedPosting {
        final int index;
        final PostingRecord posting;

        IndexedPosting(int index, PostingRecord posting) {
            this.index = index;
            this.posting = posting;
        }
    }

    private static List<PostingRecord> inferMissingPostingNumbers(List<PostingRecord> postings) {
        if (postings.isEmpty()) {
            return List.of();
        }
        Map<String, BigDecimal> sums = new HashMap<>();
        Map<String, List<Integer>> missing = new HashMap<>();
        LinkedHashSet<String> observedCurrencies = new LinkedHashSet<>();
        LinkedHashSet<String> observedPrimaryCurrencies = new LinkedHashSet<>();
        for (PostingRecord record : postings) {
            String currency = record.getCurrency();
            if (currency != null) {
                observedCurrencies.add(currency);
                if (record.getCostCurrency() == null || currency.equals(record.getCostCurrency())) {
                    observedPrimaryCurrencies.add(currency);
                }
            }
        }
        String defaultCurrency = null;
        if (observedPrimaryCurrencies.size() == 1) {
            defaultCurrency = observedPrimaryCurrencies.iterator().next();
        } else if (observedPrimaryCurrencies.isEmpty() && observedCurrencies.size() == 1) {
            defaultCurrency = observedCurrencies.iterator().next();
        }
        for (int i = 0; i < postings.size(); i++) {
            PostingRecord record = postings.get(i);
            String currency = record.getCurrency();
            if (currency == null) {
                currency = record.getCostCurrency() != null ? record.getCostCurrency() : defaultCurrency;
            }
            BigDecimal number = record.getNumber();
            if (currency != null) {
                if (number != null) {
                    sums.merge(currency, number, BigDecimal::add);
                } else {
                    missing.computeIfAbsent(currency, key -> new ArrayList<>()).add(i);
                }
            }
            if (record.getCostCurrency() != null
                    && record.getNumber() != null
                    && record.getCostNumber() != null
                    && !record.getCostCurrency().equals(record.getCurrency())) {
                BigDecimal contribution = record.getNumber().multiply(record.getCostNumber());
                if (contribution.signum() != 0) {
                    sums.merge(record.getCostCurrency(), contribution, BigDecimal::add);
                }
            }
        }
        Map<Integer, BigDecimal> inferredNumbers = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry : missing.entrySet()) {
            List<Integer> indices = entry.getValue();
            if (indices.size() == 1) {
                BigDecimal sum = sums.getOrDefault(entry.getKey(), BigDecimal.ZERO);
                inferredNumbers.put(indices.get(0), sum.negate());
            }
        }
        List<PostingRecord> normalized = new ArrayList<>(postings.size());
        for (int i = 0; i < postings.size(); i++) {
            PostingRecord record = postings.get(i);
            BigDecimal inferred = inferredNumbers.get(i);
            String currency = record.getCurrency();
            if (currency == null) {
                currency = record.getCostCurrency() != null ? record.getCostCurrency() : defaultCurrency;
            }
            if ((record.getNumber() == null && inferred != null) || (record.getCurrency() == null && currency != null)) {
                normalized.add(
                        new PostingRecord(
                                record.getPostingId(),
                                record.getEntryId(),
                                record.getFlag(),
                                record.getAccount(),
                                record.getNumber() != null ? record.getNumber() : inferred,
                                currency,
                                record.getCostNumber(),
                                record.getCostCurrency(),
                                record.getCostDate(),
                                record.getCostLabel(),
                                record.getPriceNumber(),
                                record.getPriceCurrency()));
            } else {
                normalized.add(record);
            }
        }
        return normalized;
    }

    private static List<SemanticPosting> toSemanticPostings(
            List<PostingRecord> postings, Map<Integer, PostingExtras> extrasByPostingId) {
        Map<String, BigDecimal> sums = new HashMap<>();
        Map<String, List<Integer>> missing = new HashMap<>();
        List<SemanticPosting> semanticPostings = new ArrayList<>(postings.size());
        LinkedHashSet<String> observedCurrencies = new LinkedHashSet<>();
        LinkedHashSet<String> observedPrimaryCurrencies = new LinkedHashSet<>();
        for (PostingRecord record : postings) {
            String currency = record.getCurrency();
            if (currency != null) {
                observedCurrencies.add(currency);
                if (record.getCostCurrency() == null || currency.equals(record.getCostCurrency())) {
                    observedPrimaryCurrencies.add(currency);
                }
            }
        }
        String defaultCurrency = null;
        if (observedPrimaryCurrencies.size() == 1) {
            defaultCurrency = observedPrimaryCurrencies.iterator().next();
        } else if (observedPrimaryCurrencies.isEmpty() && observedCurrencies.size() == 1) {
            defaultCurrency = observedCurrencies.iterator().next();
        }
        for (int i = 0; i < postings.size(); i++) {
            PostingRecord record = postings.get(i);
            String currency = record.getCurrency();
            if (currency == null) {
                currency = record.getCostCurrency() != null ? record.getCostCurrency() : defaultCurrency;
            }
            BigDecimal number = record.getNumber();
            if (currency != null) {
                if (number != null) {
                    sums.merge(currency, number, BigDecimal::add);
                } else {
                    missing.computeIfAbsent(currency, key -> new ArrayList<>()).add(i);
                }
            }
            if (record.getCostCurrency() != null
                    && record.getNumber() != null
                    && record.getCostNumber() != null
                    && !record.getCostCurrency().equals(record.getCurrency())) {
                BigDecimal contribution = record.getNumber().multiply(record.getCostNumber());
                if (contribution.signum() != 0) {
                    sums.merge(record.getCostCurrency(), contribution, BigDecimal::add);
                }
            }
        }
        Map<Integer, BigDecimal> inferredNumbers = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry : missing.entrySet()) {
            List<Integer> indices = entry.getValue();
            if (indices.size() == 1) {
                BigDecimal sum = sums.getOrDefault(entry.getKey(), BigDecimal.ZERO);
                inferredNumbers.put(indices.get(0), sum.negate());
            }
        }
        for (int i = 0; i < postings.size(); i++) {
            PostingRecord record = postings.get(i);
            BigDecimal number = record.getNumber();
            if (number == null) {
                number = inferredNumbers.get(i);
            }
            String currency = record.getCurrency();
            if (currency == null) {
                currency = record.getCostCurrency() != null ? record.getCostCurrency() : defaultCurrency;
            }
            PostingExtras extras =
                    extrasByPostingId != null ? extrasByPostingId.remove(record.getPostingId()) : null;
            semanticPostings.add(
                    new SemanticPosting(
                            record.getAccount(),
                            number,
                            currency,
                            record.getCostNumber(),
                            record.getCostCurrency(),
                            record.getPriceNumber(),
                            record.getPriceCurrency(),
                            extras != null ? extras.metadata() : List.of(),
                            extras != null ? extras.comments() : List.of()));
        }
        return semanticPostings;
    }
    private static LedgerData buildLedgerData(AnalyzerState state) {
        return new LedgerData(
                List.copyOf(state.entries),
                List.copyOf(state.postings),
                List.copyOf(state.rawPostings),
                List.copyOf(state.opens),
                List.copyOf(state.closes),
                List.copyOf(state.pads),
                List.copyOf(state.balances),
                List.copyOf(state.notes),
                List.copyOf(state.documents),
                List.copyOf(state.events),
                List.copyOf(state.queries),
                List.copyOf(state.prices));
    }
    private static void finalizeState(AnalyzerState state) {
        Map<Integer, BalanceRecord> balanceByEntry = new LinkedHashMap<>();
        for (BalanceRecord record : state.balances) {
            balanceByEntry.put(record.getEntryId(), record);
        }
        Map<Integer, List<PostingRecord>> postingsByEntry = new HashMap<>();
        for (PostingRecord posting : state.postings) {
            postingsByEntry.computeIfAbsent(posting.getEntryId(), key -> new ArrayList<>()).add(posting);
        }
        Map<Integer, PadRecord> padByEntry = new HashMap<>();
        for (PadRecord pad : state.pads) {
            padByEntry.put(pad.getEntryId(), pad);
        }
        state.costToleranceByCurrency.clear();
        state.costToleranceByCurrency.putAll(
                computeCostTolerances(state, state.inferToleranceFromCost));
        Map<String, Map<String, BigDecimal>> runningBalances = new HashMap<>();
        Map<String, PadContext> activePadContexts = new HashMap<>();
        List<LedgerEntry> paddingEntries = new ArrayList<>();
        List<BalanceRecord> normalizedBalances = new ArrayList<>(state.balances.size());
        List<LedgerEntry> orderedEntries = new ArrayList<>(state.entries);
        orderedEntries.sort(LEDGER_ENTRY_COMPARATOR);
        for (LedgerEntry entry : orderedEntries) {
            String type = entry.getType();
            if ("txn".equals(type)) {
                List<PostingRecord> postings = postingsByEntry.get(entry.getId());
                if (postings != null) {
                    updateRunningBalances(runningBalances, postings);
                }
            } else if ("pad".equals(type)) {
                PadRecord pad = padByEntry.get(entry.getId());
                if (pad != null) {
                    PadContext context =
                            state.padContextsByEntryId.getOrDefault(
                                    entry.getId(), new PadContext(pad, entry, null));
                    context.reset();
                    activePadContexts.put(pad.getAccount(), context);
                }
            } else if ("balance".equals(type)) {
                BalanceRecord record = balanceByEntry.get(entry.getId());
                if (record != null) {
                    BalanceRecord adjusted =
                            adjustBalanceRecord(
                                    state,
                                    record,
                                    runningBalances,
                                    activePadContexts,
                                    paddingEntries);
                    normalizedBalances.add(adjusted);
                }
            }
        }
        state.balances.clear();
        state.balances.addAll(normalizedBalances);
        if (!paddingEntries.isEmpty()) {
            state.entries.addAll(paddingEntries);
        }
    }

    private static void assignEntryIds(AnalyzerState state) {
        if (state.directiveDescriptors.isEmpty()) {
            return;
        }
        List<DirectiveDescriptor> sorted = new ArrayList<>(state.directiveDescriptors);
        sorted.sort(DIRECTIVE_COMPARATOR);
        Map<Integer, Integer> idMap = new HashMap<>();
        for (int newId = 0; newId < sorted.size(); newId++) {
            idMap.put(sorted.get(newId).tempId(), newId);
        }
        remapLedgerEntries(state, idMap);
        remapOpenRecords(state, idMap);
        remapCloseRecords(state, idMap);
        remapPadRecords(state, idMap);
        remapBalanceRecords(state, idMap);
        remapNoteRecords(state, idMap);
        remapDocumentRecords(state, idMap);
        remapEventRecords(state, idMap);
        remapQueryRecords(state, idMap);
        remapPriceRecords(state, idMap);
        remapPostings(state.postings, idMap);
        remapPostings(state.rawPostings, idMap);
        state.nextEntryId = sorted.size();
    }

    private static void assignPostingIds(AnalyzerState state) {
        if (state.rawPostings.isEmpty()) {
            state.nextPostingId = 0;
            return;
        }
        Map<Integer, Deque<PostingRecord>> postingsByEntry = groupPostingsByEntry(state.rawPostings);
        List<PostingRecord> ordered = new ArrayList<>(state.rawPostings.size());
        int nextPostingId = 0;
        for (LedgerEntry entry : state.entries) {
            Deque<PostingRecord> queue = postingsByEntry.remove(entry.getId());
            if (queue == null) {
                continue;
            }
            while (!queue.isEmpty()) {
                PostingRecord posting = queue.removeFirst();
                ordered.add(copyPosting(posting, nextPostingId++, entry.getId()));
            }
        }
        for (Map.Entry<Integer, Deque<PostingRecord>> leftover : postingsByEntry.entrySet()) {
            Deque<PostingRecord> queue = leftover.getValue();
            while (!queue.isEmpty()) {
                PostingRecord posting = queue.removeFirst();
                ordered.add(copyPosting(posting, nextPostingId++, leftover.getKey()));
            }
        }
        state.rawPostings.clear();
        state.rawPostings.addAll(ordered);
        state.nextPostingId = nextPostingId;
    }

    private static void reorderPostingsByCurrencyBuckets(AnalyzerState state) {
        if (state.rawPostings.isEmpty()) {
            return;
        }
        List<PostingRecord> reorderedRaw = reorderPostingsForEntries(state.rawPostings, state.entries);
        List<PostingRecord> reorderedPostings = reorderPostingsForEntries(state.postings, state.entries);
        state.rawPostings.clear();
        state.rawPostings.addAll(reorderedRaw);
        state.postings.clear();
        state.postings.addAll(reorderedPostings);
    }

    private static List<PostingRecord> reorderPostingsForEntries(
            List<PostingRecord> postings, List<LedgerEntry> entries) {
        Map<Integer, List<PostingRecord>> byEntry = new LinkedHashMap<>();
        for (PostingRecord posting : postings) {
            byEntry.computeIfAbsent(posting.getEntryId(), key -> new ArrayList<>()).add(posting);
        }
        List<PostingRecord> ordered = new ArrayList<>(postings.size());
        for (LedgerEntry entry : entries) {
            List<PostingRecord> entryPostings = byEntry.remove(entry.getId());
            if (entryPostings == null) {
                continue;
            }
            ordered.addAll(orderEntryPostings(entryPostings));
        }
        for (List<PostingRecord> leftovers : byEntry.values()) {
            ordered.addAll(orderEntryPostings(leftovers));
        }
        return ordered;
    }

    private static List<PostingRecord> orderEntryPostings(List<PostingRecord> postings) {
        if (postings.size() <= 1) {
            return new ArrayList<>(postings);
        }
        Map<String, List<PostingRecord>> groups = new LinkedHashMap<>();
        List<PostingRecord> unknownCurrency = new ArrayList<>();
        for (PostingRecord posting : postings) {
            String bucketCurrency = bucketCurrency(posting);
            if (bucketCurrency == null) {
                unknownCurrency.add(posting);
            } else {
                groups.computeIfAbsent(bucketCurrency, key -> new ArrayList<>()).add(posting);
            }
        }
        if (groups.isEmpty()) {
            return new ArrayList<>(postings);
        }
        List<PostingRecord> ordered = new ArrayList<>(postings.size());
        for (List<PostingRecord> group : groups.values()) {
            ordered.addAll(group);
        }
        ordered.addAll(unknownCurrency);
        return ordered;
    }

    private static String bucketCurrency(PostingRecord posting) {
        String costCurrency = normalizeCurrency(posting.getCostCurrency());
        if (costCurrency != null) {
            return costCurrency;
        }
        String priceCurrency = normalizeCurrency(posting.getPriceCurrency());
        if (priceCurrency != null) {
            return priceCurrency;
        }
        return normalizeCurrency(posting.getCurrency());
    }

    private static String normalizeCurrency(String currency) {
        if (currency == null) {
            return null;
        }
        String trimmed = currency.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static void bookInventoryLots(AnalyzerState state) {
        if (state.postings.isEmpty()) {
            return;
        }
        List<PostingRecord> bookedPostings = bookPostings(state.entries, state.postings);
        state.postings.clear();
        state.postings.addAll(bookedPostings);
        List<PostingRecord> bookedRaw = bookPostings(state.entries, state.rawPostings);
        state.rawPostings.clear();
        state.rawPostings.addAll(bookedRaw);
    }

    private static List<PostingRecord> bookPostings(
            List<LedgerEntry> entries, List<PostingRecord> postings) {
        Map<Integer, List<PostingRecord>> byEntry = new LinkedHashMap<>();
        for (PostingRecord posting : postings) {
            byEntry.computeIfAbsent(posting.getEntryId(), key -> new ArrayList<>()).add(posting);
        }
        Map<String, List<InventoryLot>> inventory = new LinkedHashMap<>();
        List<PostingRecord> booked = new ArrayList<>(postings.size());
        for (LedgerEntry entry : entries) {
            List<PostingRecord> entryPostings = byEntry.remove(entry.getId());
            if (entryPostings == null) {
                continue;
            }
            booked.addAll(bookEntryPostings(entry, entryPostings, inventory));
        }
        for (List<PostingRecord> leftovers : byEntry.values()) {
            booked.addAll(leftovers);
        }
        return booked;
    }

    private static List<PostingRecord> bookEntryPostings(
            LedgerEntry entry,
            List<PostingRecord> postings,
            Map<String, List<InventoryLot>> inventory) {
        List<PostingRecord> booked = new ArrayList<>(postings.size());
        for (PostingRecord posting : postings) {
            BigDecimal number = posting.getNumber();
            String currency = posting.getCurrency();
            if (number == null || currency == null || !isInventoryPosting(posting)) {
                booked.add(posting);
                continue;
            }
            String key = inventoryKey(posting.getAccount(), currency);
            if (key == null) {
                booked.add(posting);
                continue;
            }
            if (number.signum() >= 0) {
                LocalDate costDate = posting.getCostDate();
                if (costDate == null) {
                    costDate = entry.getDate();
                    posting.setCostDate(costDate);
                }
                addInventoryLot(
                        inventory,
                        key,
                        number.abs(),
                        posting.getCostNumber(),
                        posting.getCostCurrency(),
                        posting.getCostDate(),
                        posting.getCostLabel());
                booked.add(posting);
            } else {
                List<PostingRecord> reductions =
                        reduceInventory(posting, entry, inventory, key);
                if (reductions == null) {
                    booked.add(posting);
                } else {
                    booked.addAll(reductions);
                }
            }
        }
        return booked;
    }

    private static boolean isInventoryPosting(PostingRecord posting) {
        return posting.getAccount() != null
                && posting.getCurrency() != null
                && (posting.getCostNumber() != null
                        || posting.getCostCurrency() != null
                        || posting.getCostDate() != null);
    }

    private static String inventoryKey(String account, String currency) {
        if (account == null || currency == null) {
            return null;
        }
        return account + "|" + currency;
    }

    private static void addInventoryLot(
            Map<String, List<InventoryLot>> inventory,
            String key,
            BigDecimal quantity,
            BigDecimal costNumber,
            String costCurrency,
            LocalDate costDate,
            String costLabel) {
        if (key == null || quantity == null || quantity.signum() <= 0) {
            return;
        }
        inventory.computeIfAbsent(key, k -> new ArrayList<>())
                .add(new InventoryLot(quantity, costNumber, costCurrency, costDate, costLabel));
    }

    private static List<PostingRecord> reduceInventory(
            PostingRecord posting,
            LedgerEntry entry,
            Map<String, List<InventoryLot>> inventory,
            String key) {
        List<InventoryLot> lots = inventory.get(key);
        if (lots == null || lots.isEmpty()) {
            return null;
        }
        List<InventoryLot> matches = new ArrayList<>();
        for (InventoryLot lot : lots) {
            if (matchesLot(posting, lot)) {
                matches.add(lot);
            }
        }
        if (matches.isEmpty()) {
            return null;
        }
        BigDecimal remaining = posting.getNumber().abs();
        BigDecimal available = BigDecimal.ZERO;
        for (InventoryLot lot : matches) {
            available = available.add(lot.quantity);
            if (available.compareTo(remaining) >= 0) {
                break;
            }
        }
        if (available.compareTo(remaining) < 0) {
            return null;
        }
        List<PostingRecord> reductions = new ArrayList<>();
        for (Iterator<InventoryLot> iterator = lots.iterator();
                iterator.hasNext() && remaining.signum() > 0; ) {
            InventoryLot lot = iterator.next();
            if (!matchesLot(posting, lot)) {
                continue;
            }
            BigDecimal chunk = lot.quantity.min(remaining);
            LocalDate costDate = lot.costDate != null ? lot.costDate : entry.getDate();
            BigDecimal costNumber =
                    posting.getCostNumber() != null ? posting.getCostNumber() : lot.costNumber;
            String costCurrency =
                    posting.getCostCurrency() != null ? posting.getCostCurrency() : lot.costCurrency;
            String costLabel =
                    posting.getCostLabel() != null ? posting.getCostLabel() : lot.costLabel;
            reductions.add(
                    new PostingRecord(
                            posting.getPostingId(),
                            posting.getEntryId(),
                            posting.getFlag(),
                            posting.getAccount(),
                            chunk.negate(),
                            posting.getCurrency(),
                            costNumber,
                            costCurrency,
                            costDate,
                            costLabel,
                            posting.getPriceNumber(),
                            posting.getPriceCurrency()));
            remaining = remaining.subtract(chunk);
            if (chunk.compareTo(lot.quantity) == 0) {
                iterator.remove();
            } else {
                lot.quantity = lot.quantity.subtract(chunk);
            }
        }
        return reductions;
    }

    private static boolean matchesLot(PostingRecord posting, InventoryLot lot) {
        if (posting.getCostNumber() != null
                && lot.costNumber != null
                && posting.getCostNumber().compareTo(lot.costNumber) != 0) {
            return false;
        }
        if (posting.getCostCurrency() != null
                && lot.costCurrency != null
                && !posting.getCostCurrency().equals(lot.costCurrency)) {
            return false;
        }
        if (posting.getCostDate() != null
                && lot.costDate != null
                && !posting.getCostDate().equals(lot.costDate)) {
            return false;
        }
        if (posting.getCostLabel() != null
                && lot.costLabel != null
                && !posting.getCostLabel().equals(lot.costLabel)) {
            return false;
        }
        return true;
    }

    private static final class InventoryLot {
        BigDecimal quantity;
        final BigDecimal costNumber;
        final String costCurrency;
        final LocalDate costDate;
        final String costLabel;

        InventoryLot(
                BigDecimal quantity,
                BigDecimal costNumber,
                String costCurrency,
                LocalDate costDate,
                String costLabel) {
            this.quantity = quantity;
            this.costNumber = costNumber;
            this.costCurrency = costCurrency;
            this.costDate = costDate;
            this.costLabel = costLabel;
        }
    }

    private static void remapLedgerEntries(AnalyzerState state, Map<Integer, Integer> idMap) {
        List<LedgerEntry> updated = new ArrayList<>(state.entries.size());
        for (LedgerEntry entry : state.entries) {
            Integer newId = idMap.get(entry.getId());
            if (newId == null) {
                continue;
            }
            updated.add(
                    new LedgerEntry(
                            newId,
                            entry.getDate(),
                            entry.getType(),
                            entry.getSourceFilename(),
                            entry.getSourceLineno(),
                            entry.getTransactionPayload()));
        }
        updated.sort(Comparator.comparingInt(LedgerEntry::getId));
        state.entries.clear();
        state.entries.addAll(updated);
    }

    private static void remapOpenRecords(AnalyzerState state, Map<Integer, Integer> idMap) {
        for (int i = 0; i < state.opens.size(); i++) {
            OpenRecord record = state.opens.get(i);
            Integer newId = idMap.get(record.getEntryId());
            if (newId == null) {
                continue;
            }
            state.opens.set(i, new OpenRecord(newId, record.getAccount(), record.getCurrencies()));
        }
    }

    private static void remapCloseRecords(AnalyzerState state, Map<Integer, Integer> idMap) {
        for (int i = 0; i < state.closes.size(); i++) {
            CloseRecord record = state.closes.get(i);
            Integer newId = idMap.get(record.getEntryId());
            if (newId == null) {
                continue;
            }
            state.closes.set(i, new CloseRecord(newId, record.getAccount()));
        }
    }

    private static void remapPadRecords(AnalyzerState state, Map<Integer, Integer> idMap) {
        for (int i = 0; i < state.pads.size(); i++) {
            PadRecord record = state.pads.get(i);
            Integer newId = idMap.get(record.getEntryId());
            if (newId == null) {
                continue;
            }
            state.pads.set(i, new PadRecord(newId, record.getAccount(), record.getSourceAccount()));
        }
    }

    private static void remapBalanceRecords(AnalyzerState state, Map<Integer, Integer> idMap) {
        for (int i = 0; i < state.balances.size(); i++) {
            BalanceRecord record = state.balances.get(i);
            Integer newId = idMap.get(record.getEntryId());
            if (newId == null) {
                continue;
            }
            state.balances.set(
                    i,
                    new BalanceRecord(
                            newId,
                            record.getAccount(),
                            record.getAmountNumber(),
                            record.getAmountCurrency(),
                            record.getDiffNumber(),
                            record.getDiffCurrency(),
                            record.getToleranceNumber(),
                            record.getToleranceCurrency()));
        }
    }

    private static void remapNoteRecords(AnalyzerState state, Map<Integer, Integer> idMap) {
        for (int i = 0; i < state.notes.size(); i++) {
            NoteRecord record = state.notes.get(i);
            Integer newId = idMap.get(record.getEntryId());
            if (newId == null) {
                continue;
            }
            state.notes.set(i, new NoteRecord(newId, record.getAccount(), record.getComment()));
        }
    }

    private static void remapDocumentRecords(AnalyzerState state, Map<Integer, Integer> idMap) {
        for (int i = 0; i < state.documents.size(); i++) {
            DocumentRecord record = state.documents.get(i);
            Integer newId = idMap.get(record.getEntryId());
            if (newId == null) {
                continue;
            }
            state.documents.set(i, new DocumentRecord(newId, record.getAccount(), record.getFilename()));
        }
    }

    private static void remapEventRecords(AnalyzerState state, Map<Integer, Integer> idMap) {
        for (int i = 0; i < state.events.size(); i++) {
            EventRecord record = state.events.get(i);
            Integer newId = idMap.get(record.getEntryId());
            if (newId == null) {
                continue;
            }
            state.events.set(i, new EventRecord(newId, record.getType(), record.getDescription()));
        }
    }

    private static void remapQueryRecords(AnalyzerState state, Map<Integer, Integer> idMap) {
        for (int i = 0; i < state.queries.size(); i++) {
            QueryRecord record = state.queries.get(i);
            Integer newId = idMap.get(record.getEntryId());
            if (newId == null) {
                continue;
            }
            state.queries.set(i, new QueryRecord(newId, record.getName(), record.getQueryString()));
        }
    }

    private static void remapPriceRecords(AnalyzerState state, Map<Integer, Integer> idMap) {
        for (int i = 0; i < state.prices.size(); i++) {
            PriceRecord record = state.prices.get(i);
            Integer newId = idMap.get(record.getEntryId());
            if (newId == null) {
                continue;
            }
            state.prices.set(
                    i,
                    new PriceRecord(
                            newId,
                            record.getCurrency(),
                            record.getAmountNumber(),
                            record.getAmountCurrency()));
        }
    }

    private static void remapPostings(List<PostingRecord> records, Map<Integer, Integer> idMap) {
        for (int i = 0; i < records.size(); i++) {
            records.set(i, remapPosting(records.get(i), idMap));
        }
    }

    private static PostingRecord remapPosting(PostingRecord record, Map<Integer, Integer> idMap) {
        Integer newEntryId = idMap.get(record.getEntryId());
        if (newEntryId == null) {
            return record;
        }
        return copyPosting(record, record.getPostingId(), newEntryId);
    }

    private static void rebuildPostings(AnalyzerState state) {
        Map<Integer, Deque<PostingRecord>> postingsByEntry = groupPostingsByEntry(state.rawPostings);
        List<PostingRecord> ordered = new ArrayList<>(state.rawPostings.size());
        for (LedgerEntry entry : state.entries) {
            if (!"txn".equals(entry.getType())) {
                continue;
            }
            Deque<PostingRecord> queue = postingsByEntry.remove(entry.getId());
            if (queue == null) {
                continue;
            }
            while (!queue.isEmpty()) {
                PostingRecord posting = queue.removeFirst();
                ordered.add(copyPosting(posting, posting.getPostingId(), entry.getId()));
            }
        }
        for (Map.Entry<Integer, Deque<PostingRecord>> leftover : postingsByEntry.entrySet()) {
            Deque<PostingRecord> queue = leftover.getValue();
            while (!queue.isEmpty()) {
                PostingRecord posting = queue.removeFirst();
                ordered.add(copyPosting(posting, posting.getPostingId(), leftover.getKey()));
            }
        }
        state.postings.clear();
        state.postings.addAll(ordered);
        state.nextPostingId = state.rawPostings.size();
    }

    private static Map<Integer, Deque<PostingRecord>> groupPostingsByEntry(List<PostingRecord> postings) {
        Map<Integer, Deque<PostingRecord>> byEntry = new LinkedHashMap<>();
        for (PostingRecord posting : postings) {
            byEntry.computeIfAbsent(posting.getEntryId(), key -> new ArrayDeque<>()).addLast(posting);
        }
        return byEntry;
    }


    private static PostingRecord copyPosting(PostingRecord source, int postingId, int entryId) {
        return new PostingRecord(
                postingId,
                entryId,
                source.getFlag(),
                source.getAccount(),
                source.getNumber(),
                source.getCurrency(),
                source.getCostNumber(),
                source.getCostCurrency(),
                source.getCostDate(),
                source.getCostLabel(),
                source.getPriceNumber(),
                source.getPriceCurrency());
    }
    private static void updateRunningBalances(
            Map<String, Map<String, BigDecimal>> runningBalances, List<PostingRecord> postings) {
        for (PostingRecord posting : postings) {
            BigDecimal number = posting.getNumber();
            String currency = posting.getCurrency();
            if (number == null || currency == null) {
                continue;
            }
            for (String account : accountHierarchy(posting.getAccount())) {
                runningBalances
                        .computeIfAbsent(account, key -> new HashMap<>())
                        .merge(currency, number, BigDecimal::add);
            }
        }
    }
    private static BalanceRecord adjustBalanceRecord(
            AnalyzerState state,
            BalanceRecord record,
            Map<String, Map<String, BigDecimal>> runningBalances,
            Map<String, PadContext> padContexts,
            List<LedgerEntry> paddingEntries) {
        BigDecimal targetNumber = record.getAmountNumber();
        String currency = record.getAmountCurrency();
        if (targetNumber == null || currency == null) {
            return record;
        }
        BigDecimal actual = getBalance(runningBalances, record.getAccount(), currency);
        BigDecimal diff = actual.subtract(targetNumber);
        BigDecimal tolerance = computeBalanceTolerance(state, record);
        BigDecimal diffNumber = null;
        String diffCurrency = null;
        PadContext context = padContexts.get(record.getAccount());
        boolean padded = false;
        if (diff.compareTo(BigDecimal.ZERO) != 0) {
            BigDecimal absDiff = diff.abs();
            boolean withinTolerance = tolerance != null && absDiff.compareTo(tolerance) <= 0;
            if (withinTolerance) {
                diff = BigDecimal.ZERO;
            } else if (context != null && context.canPad(currency)) {
                BigDecimal adjustment = diff.negate();
                applyAdjustment(runningBalances, record.getAccount(), currency, adjustment);
                applyAdjustment(
                        runningBalances, context.getSourceAccount(), currency, adjustment.negate());
                context.recordPadding(currency);
                padded = true;
                addPaddingTransaction(
                        state, context, record, adjustment, currency, paddingEntries);
            } else {
                diffNumber = diff;
                diffCurrency = currency;
            }
        }
        if (context != null && !padded) {
            context.markSeen(currency);
        }
        if (diffNumber != null) {
            diffNumber = diffNumber.stripTrailingZeros();
        }
        return new BalanceRecord(
                record.getEntryId(),
                record.getAccount(),
                record.getAmountNumber(),
                record.getAmountCurrency(),
                diffNumber,
                diffCurrency,
                record.getToleranceNumber(),
                record.getToleranceCurrency());
    }
    private static void addPaddingTransaction(
            AnalyzerState state,
            PadContext context,
            BalanceRecord balanceRecord,
            BigDecimal adjustment,
            String currency,
            List<LedgerEntry> paddingEntries) {
        if (context == null || adjustment == null || currency == null) {
            return;
        }
        LedgerEntry padEntry = context.getPadEntry();
        if (padEntry == null) {
            return;
        }
        String debitAccount = context.getAccount();
        String creditAccount = context.getSourceAccount();
        if (debitAccount == null || creditAccount == null) {
            return;
        }
        int entryId = state.nextEntryId++;
        String filename = padEntry.getSourceFilename();
        int line = padEntry.getSourceLineno();
        TransactionPayload payload =
                new TransactionPayload(
                        "P",
                        null,
                        buildPaddingNarration(balanceRecord, adjustment, currency),
                        "",
                        "");
        LedgerEntry paddingEntry =
                new LedgerEntry(
                        entryId, padEntry.getDate(), "txn", filename, line, payload);
        paddingEntries.add(paddingEntry);
        state.directiveDescriptors.add(
                new DirectiveDescriptor(entryId, paddingEntry.getDate(), "txn", filename, line));
        SourceLocation location = context.getLocation();
        if (location == null) {
            location = new SourceLocation(filename, line, 0);
        }
        PostingRecord padPosting =
                new PostingRecord(
                        state.nextPostingId++,
                        entryId,
                        null,
                        debitAccount,
                        adjustment,
                        currency,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        PostingRecord contraPosting =
                new PostingRecord(
                        state.nextPostingId++,
                        entryId,
                        null,
                        creditAccount,
                        adjustment.negate(),
                        currency,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        state.postings.add(padPosting);
        state.postings.add(contraPosting);
        state.rawPostings.add(padPosting);
        state.rawPostings.add(contraPosting);
        List<SemanticPosting> semanticPostings =
                List.of(
                        new SemanticPosting(
                                debitAccount,
                                adjustment,
                                currency,
                                null,
                                null,
                                null,
                                null,
                                List.of(),
                                List.of()),
                        new SemanticPosting(
                                creditAccount,
                                adjustment.negate(),
                                currency,
                                null,
                                null,
                                null,
                                null,
                                List.of(),
                                List.of()));
        state.transactions.add(
                new SemanticTransaction(
                        paddingEntry.getDate(),
                        "txn",
                        payload.getFlag(),
                        payload.getPayee(),
                        payload.getNarration(),
                        List.of(),
                        List.of(),
                        List.of(),
                        semanticPostings,
                        List.of(),
                        location));
    }

    private static String buildPaddingNarration(
            BalanceRecord balanceRecord, BigDecimal adjustment, String currency) {
        String target =
                formatAmount(balanceRecord.getAmountNumber(), balanceRecord.getAmountCurrency());
        String diffText = formatAmount(adjustment, currency);
        return "(Padding inserted for Balance of %s for difference %s)".formatted(target, diffText);
    }

    private static String formatAmount(BigDecimal amount, String currency) {
        if (amount == null) {
            return currency == null ? "0" : "0 " + currency;
        }
        BigDecimal toFormat = amount;
        String number;
        if (amount.signum() == 0) {
            BigDecimal zeroWithScale =
                    BigDecimal.ZERO.setScale(Math.max(0, amount.scale()), RoundingMode.UNNECESSARY);
            number = zeroWithScale.toPlainString();
        } else {
            number = toFormat.toPlainString();
        }
        return currency == null ? number : number + " " + currency;
    }
    private static BigDecimal computeBalanceTolerance(AnalyzerState state, BalanceRecord record) {
        BigDecimal explicit = record.getToleranceNumber();
        if (explicit != null) {
            return explicit.abs();
        }
        String currency = record.getAmountCurrency();
        BigDecimal override = null;
        if (currency != null) {
            override = state.toleranceOverrides.get(currency);
        }
        if (override == null && state.defaultToleranceOverride != null) {
            override = state.defaultToleranceOverride;
        }
        BigDecimal candidate = override;
        BigDecimal amountNumber = record.getAmountNumber();
        if (amountNumber == null) {
            candidate = candidate != null ? candidate.abs() : BigDecimal.ZERO;
            return applyCostTolerance(state, record.getAmountCurrency(), candidate);
        }
        BigDecimal multiplier = state.toleranceMultiplier != null ? state.toleranceMultiplier : new BigDecimal("0.5");
        BigDecimal baseUnit;
        BigDecimal normalized = amountNumber.stripTrailingZeros();
        int scale = normalized.scale();
        if (scale > 0) {
            baseUnit = BigDecimal.ONE.scaleByPowerOfTen(-scale);
        } else {
            candidate = candidate != null ? candidate.abs() : BigDecimal.ZERO;
            return applyCostTolerance(state, record.getAmountCurrency(), candidate);
        }
        BigDecimal inferred = baseUnit.multiply(multiplier).multiply(BigDecimal.valueOf(2)).abs();
        if (candidate != null) {
            inferred = inferred.max(candidate.abs());
        }
        return applyCostTolerance(state, record.getAmountCurrency(), inferred.abs());
    }
    private static BigDecimal getBalance(
            Map<String, Map<String, BigDecimal>> runningBalances, String account, String currency) {
        if (account == null || currency == null) {
            return BigDecimal.ZERO;
        }
        Map<String, BigDecimal> balancesByCurrency = runningBalances.get(account);
        if (balancesByCurrency == null) {
            return BigDecimal.ZERO;
        }
        return balancesByCurrency.getOrDefault(currency, BigDecimal.ZERO);
    }
    private static void applyAdjustment(
            Map<String, Map<String, BigDecimal>> runningBalances,
            String account,
            String currency,
            BigDecimal adjustment) {
        if (account == null || currency == null || adjustment == null) {
            return;
        }
        if (adjustment.compareTo(BigDecimal.ZERO) == 0) {
            return;
        }
        for (String acct : accountHierarchy(account)) {
            runningBalances
                    .computeIfAbsent(acct, key -> new HashMap<>())
                    .merge(currency, adjustment, BigDecimal::add);
        }
    }
    private static final class AnalyzerState {
        final Set<Path> activeFiles = new HashSet<>();
        final Set<Path> includeOnceVisited = new HashSet<>();
        final Set<String> openAccounts = new HashSet<>();
        final List<SemanticTransaction> transactions = new ArrayList<>();
        final List<LoaderMessage> messages = new ArrayList<>();
        final List<DirectiveNode> observedDirectives = new ArrayList<>();
        final List<String> tokenLog = new ArrayList<>();
        final List<String> diagnosticLog = new ArrayList<>();
        final List<ParsedDirective> directives = new ArrayList<>();
        final Map<Integer, ParsedDirective> directivesById = new HashMap<>();
        final List<LedgerEntry> entries = new ArrayList<>();
        final List<PostingRecord> postings = new ArrayList<>();
        final List<PostingRecord> rawPostings = new ArrayList<>();
        final List<DirectiveDescriptor> directiveDescriptors = new ArrayList<>();
        final List<OpenRecord> opens = new ArrayList<>();
        final List<CloseRecord> closes = new ArrayList<>();
        final List<PadRecord> pads = new ArrayList<>();
        final List<BalanceRecord> balances = new ArrayList<>();
        final List<NoteRecord> notes = new ArrayList<>();
        final List<DocumentRecord> documents = new ArrayList<>();
        final List<EventRecord> events = new ArrayList<>();
        final List<QueryRecord> queries = new ArrayList<>();
        final List<PriceRecord> prices = new ArrayList<>();
        final List<String> operatingCurrencies = new ArrayList<>();
        BigDecimal toleranceMultiplier = new BigDecimal("0.5");
        BigDecimal defaultToleranceOverride = null;
        final Map<String, BigDecimal> toleranceOverrides = new HashMap<>();
        final Map<String, BigDecimal> costToleranceByCurrency = new HashMap<>();
        final DisplayContext displayContext = new DisplayContext();
        boolean inferToleranceFromCost;
        boolean allowPipeSeparator;
        final Map<Integer, PadContext> padContextsByEntryId = new HashMap<>();
        final Map<Integer, PostingExtras> postingExtras = new HashMap<>();
        int nextPostingId;
        int nextEntryId;
        final Deque<String> tagStack = new ArrayDeque<>();
        final Deque<MetadataEntry> metadataStack = new ArrayDeque<>();
    }
    private static final class ParsedDirective {
        private final int id;
        private final LocalDate date;
        private final String type;
        private final String sourceFilename;
        private final int sourceLineno;
        private final boolean supported;
        private LedgerEntry ledgerEntry;
        ParsedDirective(
                int id,
                LocalDate date,
                String type,
                String sourceFilename,
                int sourceLineno,
                boolean supported) {
            this.id = id;
            this.date = date;
            this.type = type;
            this.sourceFilename = sourceFilename;
            this.sourceLineno = sourceLineno;
            this.supported = supported;
        }
        String getType() {
            return type;
        }
        void setLedgerEntry(LedgerEntry ledgerEntry) {
            this.ledgerEntry = ledgerEntry;
        }
        LedgerEntry getLedgerEntry() {
            return ledgerEntry;
        }
    }
    private static final class DirectiveDescriptor {
        private final int tempId;
        private final LocalDate date;
        private final String type;
        private final String sourceFilename;
        private final int sourceLineno;
        DirectiveDescriptor(int tempId, LocalDate date, String type, String sourceFilename, int sourceLineno) {
            this.tempId = tempId;
            this.date = date;
            this.type = type;
            this.sourceFilename = sourceFilename;
            this.sourceLineno = sourceLineno;
        }
        int tempId() {
            return tempId;
        }
        LocalDate date() {
            return date;
        }
        String type() {
            return type;
        }
        String sourceFilename() {
            return sourceFilename;
        }
        int sourceLineno() {
            return sourceLineno;
        }
    }
    private static final class PadContext {
        private final PadRecord record;
        private final LedgerEntry padEntry;
        private final SourceLocation location;
        private final Set<String> paddedCurrencies = new HashSet<>();
        PadContext(PadRecord record, LedgerEntry padEntry, SourceLocation location) {
            this.record = record;
            this.padEntry = padEntry;
            this.location = location;
        }
        PadRecord getRecord() {
            return record;
        }
        LedgerEntry getPadEntry() {
            return padEntry;
        }
        String getSourceAccount() {
            return record.getSourceAccount();
        }
        String getAccount() {
            return record.getAccount();
        }
        SourceLocation getLocation() {
            return location;
        }
        boolean canPad(String currency) {
            return currency != null && !paddedCurrencies.contains(currency);
        }
        void recordPadding(String currency) {
            if (currency != null) {
                paddedCurrencies.add(currency);
            }
        }
        void markSeen(String currency) {
            if (currency != null) {
                paddedCurrencies.add(currency);
            }
        }
        void reset() {
            paddedCurrencies.clear();
        }
    }
    private static final class MetadataEntry {
        final String key;
        final String value;
        MetadataEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
    private static final class PostingExtras {
        private final List<SemanticMetadataEntry> metadata;
        private final List<String> comments;
        PostingExtras(List<SemanticMetadataEntry> metadata, List<String> comments) {
            this.metadata = metadata == null ? List.of() : List.copyOf(metadata);
            this.comments = comments == null ? List.of() : List.copyOf(comments);
        }
        List<SemanticMetadataEntry> metadata() {
            return metadata;
        }
        List<String> comments() {
            return comments;
        }
    }
    private static BigDecimal applyCostTolerance(
            AnalyzerState state, String currency, BigDecimal current) {
        BigDecimal baseline = current == null ? BigDecimal.ZERO : current.abs();
        if (currency == null) {
            return baseline;
        }
        BigDecimal costTolerance = state.costToleranceByCurrency.get(currency);
        if (costTolerance != null) {
            return baseline.max(costTolerance.abs());
        }
        return baseline;
    }
    private static Map<String, BigDecimal> computeCostTolerances(
            AnalyzerState state, boolean inferFromCost) {
        Map<String, BigDecimal> tolerances = new HashMap<>();
        if (!inferFromCost) {
            return tolerances;
        }
        BigDecimal multiplier =
                state.toleranceMultiplier != null ? state.toleranceMultiplier : new BigDecimal("0.5");
        for (PostingRecord posting : state.postings) {
            BigDecimal units = posting.getNumber();
            String unitsCurrency = posting.getCurrency();
            if (units == null || unitsCurrency == null) {
                continue;
            }
            BigDecimal normalizedUnits = units.stripTrailingZeros();
            int scale = normalizedUnits.scale();
            if (scale <= 0) {
                continue;
            }
            BigDecimal baseUnit = BigDecimal.ONE.scaleByPowerOfTen(-scale);
            BigDecimal baseTolerance = baseUnit.multiply(multiplier).abs();
            if (posting.getCostNumber() != null && posting.getCostCurrency() != null) {
                BigDecimal costTolerance =
                        baseTolerance.multiply(posting.getCostNumber().abs());
                mergeMaxTolerance(tolerances, posting.getCostCurrency(), costTolerance);
            }
            if (posting.getPriceNumber() != null && posting.getPriceCurrency() != null) {
                BigDecimal priceTolerance =
                        baseTolerance.multiply(posting.getPriceNumber().abs());
                mergeMaxTolerance(tolerances, posting.getPriceCurrency(), priceTolerance);
            }
        }
        return tolerances;
    }
    private static void mergeMaxTolerance(
            Map<String, BigDecimal> tolerances, String currency, BigDecimal tolerance) {
        if (currency == null || tolerance == null) {
            return;
        }
        tolerances.merge(
                currency,
                tolerance.abs(),
                (existing, incoming) -> existing.compareTo(incoming) >= 0 ? existing : incoming);
    }
    private static List<String> accountHierarchy(String account) {
        if (account == null || account.isEmpty()) {
            return List.of();
        }
        List<String> hierarchy = new ArrayList<>();
        String[] parts = account.split(":");
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                current.append(":");
            }
            current.append(parts[i]);
            hierarchy.add(current.toString());
        }
        return hierarchy;
    }
}
