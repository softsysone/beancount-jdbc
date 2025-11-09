package com.beancount.jdbc.loader.semantic;
import com.beancount.jdbc.loader.BeancountAstBuilder;
import com.beancount.jdbc.loader.BeancountParseException;
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
import com.beancount.jdbc.loader.semantic.inventory.BookingMethod;
import com.beancount.jdbc.loader.semantic.inventory.InventoryTracker;
import com.beancount.jdbc.loader.semantic.display.DisplayContext;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
    private static final Set<String> SUPPORTED_DIRECTIVES =
            Set.of("open", "close", "pad", "balance", "note", "document", "event", "query", "price");
    public SemanticAnalysis analyze(Path ledgerPath) throws LoaderException {
        Objects.requireNonNull(ledgerPath, "ledgerPath");
        AnalyzerState state = new AnalyzerState();
        processFile(ledgerPath.toAbsolutePath().normalize(), state);
        finalizeState(state);
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
                Path resolved = resolveInclude(file, include);
                if (resolved == null) {
                    state.messages.add(
                            new LoaderMessage(
                                    LoaderMessage.Level.ERROR,
                                    "Unable to resolve included file: " + include.getPath(),
                                    file.toString(),
                                    include.getLocation().getLine()));
                    continue;
                }
                if (include.isIncludeOnce() && !state.includeOnceVisited.add(resolved)) {
                    continue;
                }
                processFile(resolved, state);
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
    private static Path resolveInclude(Path currentFile, IncludeNode include) {
        Path path = Path.of(include.getPath());
        if (!path.isAbsolute()) {
            Path parent = currentFile.getParent();
            if (parent != null) {
                path = parent.resolve(path);
            } else {
                path = path.toAbsolutePath();
            }
        }
        return path.normalize();
    }
    private void processDirective(Path file, DirectiveNode directive, AnalyzerState state) {
        LocalDate date;
        try {
            date = LocalDate.parse(directive.getDate());
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
        boolean supported = SUPPORTED_DIRECTIVES.contains(normalizedType);
        int entryId = state.nextEntryId++;
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
        if (!supported) {
            state.observedDirectives.add(directive);
            return;
        }
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
            date = LocalDate.parse(transaction.getDate());
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
        LinkedHashSet<String> tagSet = new LinkedHashSet<>();
        for (Iterator<String> it = state.tagStack.descendingIterator(); it.hasNext(); ) {
            tagSet.add(it.next());
        }
        tagSet.addAll(toNonEmptyList(transaction.getTags()));
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
        List<String> tags = new ArrayList<>(tagSet);
        List<String> links = new ArrayList<>(linkSet);
        TransactionPayload payload =
                new TransactionPayload(
                        transaction.getFlag(),
                        transaction.getPayee(),
                        transaction.getNarration(),
                        tags.isEmpty() ? null : String.join(",", tags),
                        links.isEmpty() ? null : String.join(",", links));
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
                            posting.getCostDate(),
                            posting.getCostLabel(),
                            posting.getPriceNumber(),
                            posting.getPriceCurrency());
            postingRecords.add(record);
            state.postings.add(record);
            state.rawPostings.add(record);
            validateAccount(posting.getAccount(), posting.getLocation(), file, state);
            if (!postingMetadata.isEmpty() || !postingComments.isEmpty()) {
                state.postingExtras.put(
                        record.getPostingId(), new PostingExtras(postingMetadata, postingComments));
            }
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
                        toSemanticPostings(postingRecords, state.postingExtras),
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
            case "booking_method": {
                BookingMethod method = BookingMethod.fromString(value);
                if (method == null) {
                    recordWarning(
                            state, file, directive, "invalid booking_method value: " + value);
                    break;
                }
                state.bookingMethod = method;
                state.inventoryTracker.setBookingMethod(method);
                break;
            }
            default:
                logGlobalDirective(
                        file, directive, state, "option directive not yet supported");
        }
    }
    private void handlePlugin(Path file, GlobalDirectiveNode directive, AnalyzerState state) {
        logGlobalDirective(file, directive, state, "plugin directive not yet supported");
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
        String tag = firstArgument(directive);
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
        String requested = firstArgument(directive);
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
            return new BigDecimal(value);
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
    private static List<SemanticPosting> toSemanticPostings(
            List<PostingRecord> postings, Map<Integer, PostingExtras> extrasByPostingId) {
        Map<String, BigDecimal> sums = new HashMap<>();
        Map<String, List<Integer>> missing = new HashMap<>();
        List<SemanticPosting> semanticPostings = new ArrayList<>(postings.size());
        LinkedHashSet<String> observedCurrencies = new LinkedHashSet<>();
        for (PostingRecord record : postings) {
            if (record.getCurrency() != null) {
                observedCurrencies.add(record.getCurrency());
            }
        }
        String defaultCurrency = observedCurrencies.size() == 1 ? observedCurrencies.iterator().next() : null;
        for (int i = 0; i < postings.size(); i++) {
            PostingRecord record = postings.get(i);
            String currency = record.getCurrency();
            if (currency == null) {
                currency = defaultCurrency;
            }
            BigDecimal number = record.getNumber();
            if (currency != null) {
                if (number != null) {
                    sums.merge(currency, number, BigDecimal::add);
                } else {
                    missing.computeIfAbsent(currency, key -> new ArrayList<>()).add(i);
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
            String currency = record.getCurrency() != null ? record.getCurrency() : defaultCurrency;
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
        List<PostingRecord> normalizedPostings = new ArrayList<>();
        List<BalanceRecord> normalizedBalances = new ArrayList<>(state.balances.size());
        for (LedgerEntry entry : state.entries) {
            String type = entry.getType();
            if ("txn".equals(type)) {
                List<PostingRecord> postings = postingsByEntry.get(entry.getId());
                List<PostingRecord> normalized = normalizeTransactionPostings(postings);
                enforceStrictBookingRequirements(state, entry, normalized);
                boolean applied = true;
                try {
                    state.inventoryTracker.applyPostings(normalized);
                } catch (InventoryTracker.StrictBookingException ex) {
                    state.messages.add(
                            new LoaderMessage(
                                    LoaderMessage.Level.ERROR,
                                    ex.getMessage(),
                                    entry.getSourceFilename(),
                                    entry.getSourceLineno()));
                    applied = false;
                }
                if (applied) {
                    normalizedPostings.addAll(normalized);
                    updateRunningBalances(runningBalances, normalized);
                }
            } else if ("pad".equals(type)) {
                PadRecord pad = padByEntry.get(entry.getId());
                if (pad != null) {
                    PadContext context =
                            state.padContextsByEntryId.getOrDefault(
                                    entry.getId(), new PadContext(pad, entry, null));
                    warnIfPadTouchesCostedLot(state, pad, entry, runningBalances);
                    context.reset();
                    activePadContexts.put(pad.getAccount(), context);
                }
            } else if ("balance".equals(type)) {
                BalanceRecord record = balanceByEntry.get(entry.getId());
                if (record != null) {
                    BalanceRecord adjusted =
                            adjustBalanceRecord(state, record, runningBalances, activePadContexts);
                    normalizedBalances.add(adjusted);
                }
            }
        }
        state.postings.clear();
        state.postings.addAll(normalizedPostings);
        state.balances.clear();
        state.balances.addAll(normalizedBalances);
    }
    private static List<PostingRecord> normalizeTransactionPostings(List<PostingRecord> postings) {
        if (postings == null || postings.isEmpty()) {
            return List.of();
        }
        Map<String, BigDecimal> explicitSums = new HashMap<>();
        Map<String, List<Integer>> missingByCurrency = new HashMap<>();
        LinkedHashSet<String> observedCurrencies = new LinkedHashSet<>();
        for (PostingRecord posting : postings) {
            if (posting.getCurrency() != null) {
                observedCurrencies.add(posting.getCurrency());
            }
        }
        String defaultCurrency = observedCurrencies.size() == 1 ? observedCurrencies.iterator().next() : null;
        for (int i = 0; i < postings.size(); i++) {
            PostingRecord posting = postings.get(i);
            String currency = posting.getCurrency();
            if (currency == null) {
                currency = defaultCurrency;
            }
            if (currency == null) {
                continue;
            }
            BigDecimal number = posting.getNumber();
            if (number == null) {
                missingByCurrency.computeIfAbsent(currency, key -> new ArrayList<>()).add(i);
            } else {
                explicitSums.merge(currency, number, BigDecimal::add);
            }
        }
        Map<Integer, BigDecimal> inferredNumbers = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry : missingByCurrency.entrySet()) {
            List<Integer> indices = entry.getValue();
            if (indices.size() == 1) {
                BigDecimal sum = explicitSums.getOrDefault(entry.getKey(), BigDecimal.ZERO);
                inferredNumbers.put(indices.get(0), sum.negate());
            }
        }
        List<PostingRecord> normalized = new ArrayList<>(postings.size());
        for (int i = 0; i < postings.size(); i++) {
            PostingRecord posting = postings.get(i);
            BigDecimal number = posting.getNumber();
            if (number == null && inferredNumbers.containsKey(i)) {
                number = inferredNumbers.get(i);
            }
            String currency = posting.getCurrency() != null ? posting.getCurrency() : defaultCurrency;
            normalized.add(
                    new PostingRecord(
                            posting.getPostingId(),
                            posting.getEntryId(),
                            posting.getFlag(),
                            posting.getAccount(),
                            number,
                            currency,
                            posting.getCostNumber(),
                            posting.getCostCurrency(),
                            posting.getCostDate(),
                            posting.getCostLabel(),
                            posting.getPriceNumber(),
                            posting.getPriceCurrency()));
        }
        return normalized;
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
    private static void warnIfPadTouchesCostedLot(
            AnalyzerState state,
            PadRecord pad,
            LedgerEntry entry,
            Map<String, Map<String, BigDecimal>> runningBalances) {
        Map<String, BigDecimal> balances = runningBalances.get(pad.getAccount());
        if (balances == null || balances.isEmpty()) {
            return;
        }
        for (String currency : balances.keySet()) {
            if (state.inventoryTracker.hasCostedHoldings(pad.getAccount(), currency)) {
                state.messages.add(
                        new LoaderMessage(
                                LoaderMessage.Level.ERROR,
                                "Attempt to pad an entry with cost for balance: " + pad.getAccount(),
                                entry.getSourceFilename(),
                                entry.getSourceLineno()));
                break;
            }
        }
    }
    private static BalanceRecord adjustBalanceRecord(
            AnalyzerState state,
            BalanceRecord record,
            Map<String, Map<String, BigDecimal>> runningBalances,
            Map<String, PadContext> padContexts) {
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
            } else if (context != null && hasCostedLot(state, context.getAccount(), currency)) {
                SourceLocation loc = context.getLocation();
                String sourceName =
                        loc != null ? loc.getSourceName() : context.getPadEntry().getSourceFilename();
                int line = loc != null ? loc.getLine() : context.getPadEntry().getSourceLineno();
                state.messages.add(
                        new LoaderMessage(
                                LoaderMessage.Level.ERROR,
                                "Attempt to pad an entry with cost for balance: " + context.getAccount(),
                                sourceName,
                                line));
                diffNumber = diff;
                diffCurrency = currency;
            } else if (context != null && context.canPad(currency)) {
                BigDecimal adjustment = diff.negate();
                applyAdjustment(runningBalances, record.getAccount(), currency, adjustment);
                applyAdjustment(
                        runningBalances, context.getSourceAccount(), currency, adjustment.negate());
                context.recordPadding(currency);
                padded = true;
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
        String resultDiffCurrency = diffCurrency;
        if (resultDiffCurrency == null) {
            resultDiffCurrency = record.getAmountCurrency();
        }
        return new BalanceRecord(
                record.getEntryId(),
                record.getAccount(),
                record.getAmountNumber(),
                record.getAmountCurrency(),
                diffNumber == null ? null : diffNumber,
                resultDiffCurrency,
                record.getToleranceNumber(),
                record.getToleranceCurrency());
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
        final InventoryTracker inventoryTracker = new InventoryTracker();
        final DisplayContext displayContext = new DisplayContext();
        boolean inferToleranceFromCost;
        final Map<Integer, PadContext> padContextsByEntryId = new HashMap<>();
        final Map<Integer, PostingExtras> postingExtras = new HashMap<>();
        int nextPostingId;
        int nextEntryId;
        BookingMethod bookingMethod = BookingMethod.FIFO;
        final Deque<String> tagStack = new ArrayDeque<>();
        final Deque<MetadataEntry> metadataStack = new ArrayDeque<>();
        AnalyzerState() {
            inventoryTracker.setBookingMethod(bookingMethod);
        }
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
    private static void enforceStrictBookingRequirements(
            AnalyzerState state, LedgerEntry entry, List<PostingRecord> postings) {
        if (state.bookingMethod != BookingMethod.STRICT) {
            return;
        }
        for (PostingRecord posting : postings) {
            BigDecimal number = posting.getNumber();
            if (number == null || number.compareTo(BigDecimal.ZERO) == 0) {
                continue;
            }
            if (!requiresStrictCost(posting.getAccount())) {
                continue;
            }
            if (posting.getCostNumber() == null || posting.getCostCurrency() == null) {
                state.messages.add(
                        new LoaderMessage(
                                LoaderMessage.Level.ERROR,
                                "booking_method STRICT requires explicit cost on posting for account "
                                        + posting.getAccount(),
                                entry.getSourceFilename(),
                                entry.getSourceLineno()));
            }
        }
    }
    private static boolean requiresStrictCost(String account) {
        if (account == null) {
            return false;
        }
        return account.startsWith("Assets:") || account.startsWith("Liabilities:");
    }
    private static boolean hasCostedLot(
            AnalyzerState state, String account, String currency) {
        return state.inventoryTracker.hasCostedHoldings(account, currency);
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
