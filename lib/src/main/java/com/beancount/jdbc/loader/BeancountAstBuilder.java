package com.beancount.jdbc.loader;

import com.beancount.jdbc.loader.ast.BalanceDirectiveNode;
import com.beancount.jdbc.loader.ast.CloseDirectiveNode;
import com.beancount.jdbc.loader.ast.DirectiveNode;
import com.beancount.jdbc.loader.ast.DocumentDirectiveNode;
import com.beancount.jdbc.loader.ast.EventDirectiveNode;
import com.beancount.jdbc.loader.ast.GenericDirectiveNode;
import com.beancount.jdbc.loader.ast.IncludeNode;
import com.beancount.jdbc.loader.ast.GlobalDirectiveNode;
import com.beancount.jdbc.loader.ast.LedgerNode;
import com.beancount.jdbc.loader.ast.NoteDirectiveNode;
import com.beancount.jdbc.loader.ast.OpenDirectiveNode;
import com.beancount.jdbc.loader.ast.PadDirectiveNode;
import com.beancount.jdbc.loader.ast.PostingNode;
import com.beancount.jdbc.loader.ast.PriceDirectiveNode;
import com.beancount.jdbc.loader.ast.QueryDirectiveNode;
import com.beancount.jdbc.loader.ast.SourceLocation;
import com.beancount.jdbc.loader.ast.StatementNode;
import com.beancount.jdbc.loader.ast.TransactionMetadataNode;
import com.beancount.jdbc.loader.ast.TransactionNode;
import com.beancount.jdbc.loader.grammar.BeancountBaseVisitor;
import com.beancount.jdbc.loader.grammar.BeancountLexer;
import com.beancount.jdbc.loader.grammar.BeancountParser;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Locale;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.misc.Interval;

public final class BeancountAstBuilder {

    public LedgerNode parse(String sourceName, String input) throws BeancountParseException {
        CharStream stream = CharStreams.fromString(input, sourceName);
        return parse(sourceName, stream);
    }

    public LedgerNode parse(String sourceName, CharStream input) throws BeancountParseException {
        Objects.requireNonNull(sourceName, "sourceName");
        Objects.requireNonNull(input, "input");

        BeancountLexer lexer = new BeancountLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(ThrowingErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        if (DebugFlags.isTokenDebugEnabled()) {
            tokens.fill();
            DebugFlags.logTokens(tokens, lexer);
            tokens.seek(0);
        }

        BeancountParser parser = new BeancountParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(ThrowingErrorListener.INSTANCE);
        if (DebugFlags.isParserTraceEnabled()) {
            parser.addErrorListener(DebugFlags.diagnosticListener());
        }

        try {
            BeancountParser.LedgerContext context = parser.ledger();
            AstBuildingVisitor visitor = new AstBuildingVisitor(sourceName);
            return visitor.build(context);
        } catch (ParseCancellationException ex) {
            throw new BeancountParseException(ex.getMessage(), ex);
        }
    }

    private static final class AstBuildingVisitor extends BeancountBaseVisitor<Void> {
        private final String sourceName;
        private final List<StatementNode> statements = new ArrayList<>();

        AstBuildingVisitor(String sourceName) {
            this.sourceName = sourceName;
        }

        LedgerNode build(BeancountParser.LedgerContext context) {
            visitLedger(context);
            return new LedgerNode(statements);
        }

        @Override
        public Void visitLedger(BeancountParser.LedgerContext ctx) {
            for (BeancountParser.StatementContext statementContext : ctx.statement()) {
                visit(statementContext);
            }
            return null;
        }

        @Override
        public Void visitDirectiveStatement(BeancountParser.DirectiveStatementContext ctx) {
            Token dateToken = ctx.DATE().getSymbol();
            SourceLocation location =
                    new SourceLocation(
                            sourceName, dateToken.getLine(), dateToken.getCharPositionInLine() + 1);
            String date = ctx.DATE().getText();
            List<DirectiveLine> directiveLines = new ArrayList<>();
            BeancountParser.DirectiveHeadContext head = ctx.directiveHead();
            String directiveType = "";
            if (head.DIRECTIVE_KEY() != null) {
                directiveType = head.DIRECTIVE_KEY().getText();
                if (head.lineContent() != null) {
                    String primary = normalizeLine(originalText(head.lineContent()));
                    if (!primary.isEmpty()) {
                        directiveLines.add(new DirectiveLine(primary, 0, location));
                    }
                }
            } else if (head.FLAG() != null) {
                directiveType = head.FLAG().getText();
                if (head.lineContent() != null) {
                    String primary = normalizeLine(originalText(head.lineContent()));
                    if (!primary.isEmpty()) {
                        directiveLines.add(new DirectiveLine(primary, 0, location));
                    }
                }
            } else if (head.lineContent() != null) {
                String primary = normalizeLine(originalText(head.lineContent()));
                if (!primary.isEmpty()) {
                    int splitIndex = firstWhitespaceIndex(primary);
                    if (splitIndex >= 0) {
                        directiveType = primary.substring(0, splitIndex);
                        String remainder = primary.substring(splitIndex + 1).trim();
                        if (!remainder.isEmpty()) {
                            directiveLines.add(new DirectiveLine(remainder, 0, location));
                        }
                    } else {
                        directiveType = primary;
                    }
                }
            }

            for (BeancountParser.ContinuationStatementContext continuation : ctx.continuationStatement()) {
                int indent = 0;
                if (continuation.INDENT() != null) {
                    indent = continuation.INDENT().getSymbol().getText().length();
                }
                SourceLocation lineLocation =
                        new SourceLocation(
                                sourceName,
                                continuation.getStart().getLine(),
                                continuation.getStart().getCharPositionInLine() + 1);
                BeancountParser.ContinuationContentContext content = continuation.continuationContent();
                if (content != null) {
                    Token contentToken = content.getStart();
                    lineLocation =
                            new SourceLocation(
                                    sourceName, contentToken.getLine(), contentToken.getCharPositionInLine() + 1);
                    String continuationText = normalizeLine(originalText(content));
                    directiveLines.add(new DirectiveLine(continuationText, indent, lineLocation));
                } else {
                    directiveLines.add(new DirectiveLine("", indent, lineLocation));
                }
            }

            if (isTransactionDirectiveType(directiveType)) {
                TransactionNode transaction =
                        buildTransactionNode(location, date, directiveType, directiveLines, ctx);
                statements.add(transaction);
            } else {
                statements.add(buildDirectiveNode(location, date, directiveType, directiveLines));
            }
            return null;
        }

        @Override
        public Void visitIncludeStatement(BeancountParser.IncludeStatementContext ctx) {
            Token includeToken = ctx.INCLUDE_KEY().getSymbol();
            SourceLocation location =
                    new SourceLocation(
                            sourceName, includeToken.getLine(), includeToken.getCharPositionInLine() + 1);
            boolean includeOnce = "include-once".equals(ctx.INCLUDE_KEY().getText());
            String path = unquote(ctx.STRING().getText());
            statements.add(new IncludeNode(location, includeOnce, path));
            return null;
        }

        @Override
        public Void visitCommentStatement(BeancountParser.CommentStatementContext ctx) {
            return null;
        }

        @Override
        public Void visitBlankStatement(BeancountParser.BlankStatementContext ctx) {
            return null;
        }

        @Override
        public Void visitOptionStatement(BeancountParser.OptionStatementContext ctx) {
            Token token = ctx.OPTION().getSymbol();
            statements.add(
                    new GlobalDirectiveNode(
                            toLocation(token),
                            "option",
                            List.of(unquote(ctx.STRING(0).getText()), unquote(ctx.STRING(1).getText()))));
            return null;
        }

        @Override
        public Void visitPluginStatement(BeancountParser.PluginStatementContext ctx) {
            Token token = ctx.PLUGIN().getSymbol();
            List<String> args = new ArrayList<>();
            args.add(unquote(ctx.STRING(0).getText()));
            if (ctx.STRING().size() > 1) {
                args.add(unquote(ctx.STRING(1).getText()));
            }
            statements.add(new GlobalDirectiveNode(toLocation(token), "plugin", args));
            return null;
        }

        @Override
        public Void visitPushtagStatement(BeancountParser.PushtagStatementContext ctx) {
            statements.add(
                    new GlobalDirectiveNode(
                            toLocation(ctx.PUSHTAG().getSymbol()),
                            "pushtag",
                            List.of(normalizeLine(originalText(ctx.lineContent())))));
            return null;
        }

        @Override
        public Void visitPoptagStatement(BeancountParser.PoptagStatementContext ctx) {
            String argument =
                    ctx.lineContent() != null ? normalizeLine(originalText(ctx.lineContent())) : "";
            List<String> arguments = argument.isEmpty() ? List.of() : List.of(argument);
            statements.add(
                    new GlobalDirectiveNode(
                            toLocation(ctx.POPTAG().getSymbol()),
                            "poptag",
                            arguments));
            return null;
        }

        @Override
        public Void visitPushmetaStatement(BeancountParser.PushmetaStatementContext ctx) {
            statements.add(
                    new GlobalDirectiveNode(
                            toLocation(ctx.PUSHMETA().getSymbol()),
                            "pushmeta",
                            List.of(normalizeLine(originalText(ctx.lineContent())))));
            return null;
        }

        @Override
        public Void visitPopmetaStatement(BeancountParser.PopmetaStatementContext ctx) {
            statements.add(
                    new GlobalDirectiveNode(
                            toLocation(ctx.POPMETA().getSymbol()),
                            "popmeta",
                            List.of(normalizeLine(originalText(ctx.lineContent())))));
            return null;
        }

        private TransactionNode buildTransactionNode(
                SourceLocation location,
                String date,
                String directiveType,
                List<DirectiveLine> directiveLines,
                BeancountParser.DirectiveStatementContext statementContext) {
            List<DirectiveLine> lines = new ArrayList<>(directiveLines);
            if (lines.isEmpty()) {
                lines.add(new DirectiveLine("", 0, location));
            }

            DirectiveLine headerLine = lines.get(0);
            TransactionHeader header = parseTransactionHeader(headerLine.text);

            List<TransactionMetadataNode> metadata = new ArrayList<>();
            List<PostingNode> postings = new ArrayList<>();
            List<String> comments = new ArrayList<>();

            PostingNode currentPosting = null;
            List<BeancountParser.ContinuationStatementContext> continuationStatements =
                    statementContext.continuationStatement();
            for (BeancountParser.ContinuationStatementContext continuation : continuationStatements) {
                BeancountParser.ContinuationContentContext content = continuation.continuationContent();
                if (content == null) {
                    continue;
                }
                int indent = continuation.INDENT() != null ? continuation.INDENT().getSymbol().getText().length() : 0;
                SourceLocation lineLocation = toLocation(content.getStart());

                if (content.postingLine() != null) {
                    PostingNode posting = buildPostingNode(content.postingLine(), indent, lineLocation);
                    postings.add(posting);
                    currentPosting = posting;
                    continue;
                }

                if (content.metadataLine() != null) {
                    TransactionMetadataNode metadataNode = buildMetadataNode(content.metadataLine(), lineLocation);
                    if (metadataNode != null) {
                        if (currentPosting != null && indent > currentPosting.getIndent()) {
                            currentPosting.getMetadata().add(metadataNode);
                        } else {
                            metadata.add(metadataNode);
                        }
                    }
                    continue;
                }

                if (content.commentLine() != null) {
                    String comment = extractComment(content.commentLine().COMMENT().getSymbol().getText());
                    if (!comment.isEmpty()) {
                        if (currentPosting != null && indent > currentPosting.getIndent()) {
                            currentPosting.getComments().add(comment);
                        } else {
                            comments.add(comment);
                        }
                    }
                    continue;
                }

                if (content.lineContent() != null) {
                    String comment = normalizeLine(content.lineContent().getText());
                    if (!comment.isEmpty()) {
                        comments.add(comment);
                    }
                }
            }

            return new TransactionNode(
                    location,
                    date,
                    directiveType,
                    header.flag,
                    header.payee,
                    header.narration,
                    header.tags,
                    header.links,
                    metadata,
                    postings,
                    comments);
        }

        private DirectiveNode buildDirectiveNode(
                SourceLocation location,
                String date,
                String directiveType,
                List<DirectiveLine> directiveLines) {
            List<DirectiveLine> lines = new ArrayList<>(directiveLines);
            if (lines.isEmpty()) {
                lines.add(new DirectiveLine("", 0, location));
            }
            List<String> contentLines = new ArrayList<>(lines.size());
            for (DirectiveLine line : lines) {
                contentLines.add(line.text);
            }
            DirectiveLine header = lines.get(0);
            String normalizedType =
                    directiveType == null ? "" : directiveType.toLowerCase(Locale.ROOT);

            return switch (normalizedType) {
                case "open" -> {
                    OpenComponents components = parseOpenComponents(header.text);
                    yield new OpenDirectiveNode(
                            location,
                            date,
                            directiveType,
                            contentLines,
                            components.account(),
                            components.currencies());
                }
                case "close" -> {
                    List<String> tokens = tokenize(header.text, 1);
                    yield new CloseDirectiveNode(
                            location,
                            date,
                            directiveType,
                            contentLines,
                            tokenOrNull(tokens, 0));
                }
                case "pad" -> {
                    List<String> tokens = tokenize(header.text, 2);
                    yield new PadDirectiveNode(
                            location,
                            date,
                            directiveType,
                            contentLines,
                            tokenOrNull(tokens, 0),
                            tokenOrNull(tokens, 1));
                }
                case "balance" -> {
                    BalanceComponents components = parseBalanceComponents(header.text);
                    yield new BalanceDirectiveNode(
                            location,
                            date,
                            directiveType,
                            contentLines,
                            components.account(),
                            components.amountNumber(),
                            components.amountCurrency(),
                            components.diffNumber(),
                            components.diffCurrency(),
                            components.toleranceNumber(),
                            components.toleranceCurrency());
                }
                case "note" -> {
                    List<String> tokens = tokenize(header.text, 2);
                    yield new NoteDirectiveNode(
                            location,
                            date,
                            directiveType,
                            contentLines,
                            tokenOrNull(tokens, 0),
                            tokenOrNull(tokens, 1));
                }
                case "document" -> {
                    List<String> tokens = tokenize(header.text, 2);
                    yield new DocumentDirectiveNode(
                            location,
                            date,
                            directiveType,
                            contentLines,
                            tokenOrNull(tokens, 0),
                            tokenOrNull(tokens, 1));
                }
                case "event" -> {
                    List<String> tokens = tokenize(header.text, 2);
                    yield new EventDirectiveNode(
                            location,
                            date,
                            directiveType,
                            contentLines,
                            tokenOrNull(tokens, 0),
                            tokenOrNull(tokens, 1));
                }
                case "query" -> {
                    List<String> tokens = tokenize(header.text, 2);
                    yield new QueryDirectiveNode(
                            location,
                            date,
                            directiveType,
                            contentLines,
                            tokenOrNull(tokens, 0),
                            tokenOrNull(tokens, 1));
                }
                case "price" -> {
                    PriceComponents components = parsePriceComponents(header.text);
                    yield new PriceDirectiveNode(
                            location,
                            date,
                            directiveType,
                            contentLines,
                            components.currency(),
                            components.amountNumber(),
                            components.amountCurrency());
                }
                default -> new GenericDirectiveNode(location, date, directiveType, contentLines);
            };
        }

        private PostingNode buildPostingNode(
                BeancountParser.PostingLineContext ctx, int indent, SourceLocation location) {
            String flag = ctx.postingFlag() != null ? ctx.postingFlag().FLAG().getText() : null;
            String account = ctx.postingAccount().ACCOUNT_NAME().getText();

            BigDecimal amountNumber = null;
            String amountCurrency = null;
            if (ctx.postingAmount() != null) {
                amountNumber = parseDecimal(ctx.postingAmount().NUMBER().getText());
                if (ctx.postingAmount().postingCurrency() != null) {
                    amountCurrency = ctx.postingAmount().postingCurrency().getText();
                }
            }

            BigDecimal costNumber = null;
            String costCurrency = null;
            LocalDate costDate = null;
            String costLabel = null;
            if (ctx.postingCost() != null) {
                BeancountParser.PostingAmountContext baseCost = ctx.postingCost().postingAmount();
                if (baseCost != null) {
                    costNumber = parseDecimal(baseCost.NUMBER().getText());
                    if (baseCost.postingCurrency() != null) {
                        costCurrency = baseCost.postingCurrency().getText();
                    }
                }
                for (BeancountParser.PostingCostComponentContext component : ctx.postingCost().postingCostComponent()) {
                    if (component.postingAmount() != null) {
                        if (costNumber == null) {
                            costNumber = parseDecimal(component.postingAmount().NUMBER().getText());
                        }
                        if (costCurrency == null && component.postingAmount().postingCurrency() != null) {
                            costCurrency = component.postingAmount().postingCurrency().getText();
                        }
                    } else if (component.DATE() != null) {
                        costDate = LocalDate.parse(component.DATE().getText());
                    } else if (component.STRING() != null) {
                        costLabel = unescapeQuoted(component.STRING().getText().substring(1, component.STRING().getText().length() - 1));
                    }
                }
            }

            BigDecimal priceNumber = null;
            String priceCurrency = null;
            if (ctx.postingPrice() != null) {
                BeancountParser.PostingAmountContext priceAmount = ctx.postingPrice().postingAmount();
                priceNumber = parseDecimal(priceAmount.NUMBER().getText());
                if (priceAmount.postingCurrency() != null) {
                    priceCurrency = priceAmount.postingCurrency().getText();
                }
            }

            List<TransactionMetadataNode> postingMetadata = new ArrayList<>();
            List<String> postingComments = new ArrayList<>();
            if (ctx.postingComment() != null) {
                postingComments.add(extractComment(ctx.postingComment().COMMENT().getSymbol().getText()));
            }

            return new PostingNode(
                    location,
                    flag,
                    account,
                    amountNumber,
                    amountCurrency,
                    costNumber,
                    costCurrency,
                    costDate,
                    costLabel,
                    priceNumber,
                    priceCurrency,
                    indent,
                    postingMetadata,
                    postingComments);
        }

        private TransactionMetadataNode buildMetadataNode(
                BeancountParser.MetadataLineContext ctx, SourceLocation location) {
            String key = ctx.metadataKey().getText().trim();
            String value = "";
            if (ctx.metadataValue() != null) {
                value = normalizeLine(originalText(ctx.metadataValue()));
                if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
                    value = unescapeQuoted(value.substring(1, value.length() - 1));
                }
            }
            return new TransactionMetadataNode(key, value, location);
        }

        private String extractComment(String rawComment) {
            if (rawComment == null || rawComment.isEmpty()) {
                return "";
            }
            String trimmed = rawComment.substring(1).trim();
            return trimmed;
        }

        private String originalText(ParserRuleContext context) {
            if (context == null || context.getStart() == null || context.getStop() == null) {
                return "";
            }
            int start = context.getStart().getStartIndex();
            int stop = context.getStop().getStopIndex();
            return context.getStart().getInputStream().getText(Interval.of(start, stop));
        }

        private OpenComponents parseOpenComponents(String headerText) {
            String text = stripComment(headerText).trim();
            if (text.isEmpty()) {
                return new OpenComponents("", List.of());
            }
            int splitIndex = firstWhitespaceIndex(text);
            String account;
            String remainder = "";
            if (splitIndex >= 0) {
                account = text.substring(0, splitIndex);
                remainder = text.substring(splitIndex + 1).trim();
            } else {
                account = text;
            }
            return new OpenComponents(account, parseOpenCurrencies(remainder));
        }

        private int firstWhitespaceIndex(String text) {
            for (int i = 0; i < text.length(); i++) {
                if (Character.isWhitespace(text.charAt(i))) {
                    return i;
                }
            }
            return -1;
        }

        private List<String> parseOpenCurrencies(String text) {
            if (text == null || text.isEmpty()) {
                return List.of();
            }
            String working = text.trim();
            if (working.startsWith("[") && working.endsWith("]") && working.length() >= 2) {
                working = working.substring(1, working.length() - 1);
            }
            String[] pieces;
            if (working.contains(",")) {
                pieces = working.split(",");
            } else {
                pieces = working.split("\\s+");
            }
            List<String> currencies = new ArrayList<>();
            for (String piece : pieces) {
                String trimmed = piece.trim();
                if (!trimmed.isEmpty()) {
                    currencies.add(trimmed);
                }
            }
            return currencies;
        }

        private BalanceComponents parseBalanceComponents(String headerText) {
            List<String> tokens = tokenize(headerText, 0);
            if (tokens.isEmpty()) {
                return new BalanceComponents(null, null, null, null, null, null, null);
            }
            int index = 0;
            String account = tokens.get(index++);
            BigDecimal amountNumber = null;
            String amountCurrency = null;
            if (index < tokens.size()) {
                amountNumber = parseDecimal(tokens.get(index++));
            }
            if (index < tokens.size()) {
                amountCurrency = tokens.get(index++);
            }

            BigDecimal toleranceNumber = null;
            String toleranceCurrency = null;
            if (index < tokens.size() && "~".equals(tokens.get(index))) {
                index++;
                if (index < tokens.size()) {
                    toleranceNumber = parseDecimal(tokens.get(index++));
                }
                if (index < tokens.size()) {
                    toleranceCurrency = tokens.get(index++);
                }
            }

            BigDecimal diffNumber = null;
            String diffCurrency = null;
            if (index < tokens.size()) {
                diffNumber = parseDecimal(tokens.get(index++));
                if (index < tokens.size()) {
                    diffCurrency = tokens.get(index++);
                }
            }

            return new BalanceComponents(
                    account, amountNumber, amountCurrency, diffNumber, diffCurrency, toleranceNumber, toleranceCurrency);
        }

        private PriceComponents parsePriceComponents(String headerText) {
            List<String> tokens = tokenize(headerText, 3);
            String currency = tokenOrNull(tokens, 0);
            BigDecimal amountNumber = parseDecimal(tokenOrNull(tokens, 1));
            String amountCurrency = tokenOrNull(tokens, 2);
            return new PriceComponents(currency, amountNumber, amountCurrency);
        }

        private List<String> tokenize(String text, int maxParts) {
            if (text == null) {
                return List.of();
            }
            String working = stripComment(text).trim();
            if (working.isEmpty()) {
                return List.of();
            }
            List<String> tokens = splitPreservingQuotes(working);
            if (maxParts > 0 && tokens.size() > maxParts) {
                List<String> limited = new ArrayList<>(maxParts);
                for (int i = 0; i < maxParts - 1; i++) {
                    limited.add(tokens.get(i));
                }
                StringBuilder remainder = new StringBuilder();
                for (int i = maxParts - 1; i < tokens.size(); i++) {
                    if (remainder.length() > 0) {
                        remainder.append(' ');
                    }
                    remainder.append(tokens.get(i));
                }
                limited.add(remainder.toString());
                return limited;
            }
            return tokens;
        }

        private String stripComment(String text) {
            boolean inQuotes = false;
            char quoteChar = 0;
            boolean escape = false;
            for (int i = 0; i < text.length(); i++) {
                char ch = text.charAt(i);
                if (escape) {
                    escape = false;
                    continue;
                }
                if (inQuotes) {
                    if (ch == '\\') {
                        escape = true;
                    } else if (ch == quoteChar) {
                        inQuotes = false;
                    }
                    continue;
                }
                if (ch == '"' || ch == '\'') {
                    inQuotes = true;
                    quoteChar = ch;
                    continue;
                }
                if (ch == ';') {
                    return text.substring(0, i);
                }
            }
            return text;
        }

        private List<String> splitPreservingQuotes(String text) {
            List<String> tokens = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            boolean inQuotes = false;
            char quoteChar = 0;
            boolean escape = false;
            for (int i = 0; i < text.length(); i++) {
                char ch = text.charAt(i);
                if (escape) {
                    current.append(ch);
                    escape = false;
                    continue;
                }
                if (inQuotes) {
                    if (ch == '\\') {
                        escape = true;
                    } else if (ch == quoteChar) {
                        inQuotes = false;
                    } else {
                        current.append(ch);
                    }
                    continue;
                }
                if (ch == '"' || ch == '\'') {
                    inQuotes = true;
                    quoteChar = ch;
                    continue;
                }
                if (Character.isWhitespace(ch)) {
                    if (current.length() > 0) {
                        tokens.add(current.toString());
                        current.setLength(0);
                    }
                    continue;
                }
                current.append(ch);
            }
            if (current.length() > 0) {
                tokens.add(current.toString());
            }
            return tokens;
        }

        private String tokenOrNull(List<String> tokens, int index) {
            if (tokens == null || index < 0 || index >= tokens.size()) {
                return null;
            }
            String value = tokens.get(index);
            return value == null || value.isEmpty() ? null : value;
        }

        private TransactionHeader parseTransactionHeader(String text) {
            List<String> tokens = tokenizeHeader(text);
            String flag = null;
            String payee = null;
            String narration = null;
            List<String> tags = new ArrayList<>();
            List<String> links = new ArrayList<>();

            for (String token : tokens) {
                if (token.isEmpty()) {
                    continue;
                }
                if (flag == null && token.length() == 1 && isFlagChar(token.charAt(0))) {
                    flag = token;
                    continue;
                }
                if (token.startsWith("#") && token.length() > 1) {
                    tags.add(token.substring(1));
                    continue;
                }
                if (token.startsWith("^") && token.length() > 1) {
                    links.add(token.substring(1));
                    continue;
                }
                if (payee == null) {
                    payee = token;
                } else if (narration == null) {
                    narration = token;
                } else {
                    narration = narration + " " + token;
                }
            }
            return new TransactionHeader(flag, payee, narration, tags, links);
        }

        private List<String> tokenizeHeader(String text) {
            List<String> tokens = new ArrayList<>();
            if (text == null || text.isEmpty()) {
                return tokens;
            }
            int i = 0;
            while (i < text.length()) {
                char ch = text.charAt(i);
                if (Character.isWhitespace(ch)) {
                    i++;
                    continue;
                }
                if (ch == '"') {
                    StringBuilder sb = new StringBuilder();
                    i++;
                    while (i < text.length()) {
                        char c = text.charAt(i);
                        if (c == '\\' && i + 1 < text.length()) {
                            sb.append(text.charAt(i + 1));
                            i += 2;
                            continue;
                        }
                        if (c == '"') {
                            break;
                        }
                        sb.append(c);
                        i++;
                    }
                    if (i < text.length() && text.charAt(i) == '"') {
                        i++;
                    }
                    tokens.add(sb.toString());
                } else {
                    int start = i;
                    while (i < text.length() && !Character.isWhitespace(text.charAt(i))) {
                        i++;
                    }
                    tokens.add(text.substring(start, i));
                }
            }
            return tokens;
        }

        private BigDecimal parseDecimal(String token) {
            if (token == null || token.isEmpty()) {
                return null;
            }
            try {
                return new BigDecimal(token);
            } catch (NumberFormatException ex) {
                return null;
            }
        }


        private String unescapeQuoted(String value) {
            StringBuilder sb = new StringBuilder(value.length());
            for (int i = 0; i < value.length(); i++) {
                char ch = value.charAt(i);
                if (ch == '\\' && i + 1 < value.length()) {
                    sb.append(value.charAt(i + 1));
                    i++;
                } else {
                    sb.append(ch);
                }
            }
            return sb.toString();
        }

        private boolean isTransactionDirectiveType(String directiveType) {
            if (directiveType == null || directiveType.isEmpty()) {
                return false;
            }
            String lower = directiveType.toLowerCase(Locale.ROOT);
            if ("txn".equals(lower)) {
                return true;
            }
            if (directiveType.length() == 1) {
                return !Character.isLetterOrDigit(directiveType.charAt(0));
            }
            return false;
        }

        private static String normalizeLine(String text) {
            return text == null ? "" : text.trim();
        }

        private static String unquote(String text) {
            if (text == null || text.length() < 2) {
                return text;
            }
            char first = text.charAt(0);
            char last = text.charAt(text.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                String body = text.substring(1, text.length() - 1);
                return body.replace("\\\"", "\"").replace("\\'", "'").replace("\\\\", "\\");
            }
            return text;
        }

        private SourceLocation toLocation(Token token) {
            return new SourceLocation(
                    sourceName, token.getLine(), token.getCharPositionInLine() + 1);
        }

        private boolean isFlagChar(char ch) {
            return ch == '!' || ch == '?' || ch == '*';
        }

        private record OpenComponents(String account, List<String> currencies) {}

        private record BalanceComponents(
                String account,
                BigDecimal amountNumber,
                String amountCurrency,
                BigDecimal diffNumber,
                String diffCurrency,
                BigDecimal toleranceNumber,
                String toleranceCurrency) {}

        private record PriceComponents(String currency, BigDecimal amountNumber, String amountCurrency) {}

        private static final class DirectiveLine {
            final String text;
            final int indent;
            final SourceLocation location;

            DirectiveLine(String text, int indent, SourceLocation location) {
                this.text = text == null ? "" : text.trim();
                this.indent = indent;
                this.location = location;
            }
        }

        private static final class TransactionHeader {
            final String flag;
            final String payee;
            final String narration;
            final List<String> tags;
            final List<String> links;

            TransactionHeader(String flag, String payee, String narration, List<String> tags, List<String> links) {
                this.flag = flag;
                this.payee = payee;
                this.narration = narration;
                this.tags = tags;
                this.links = links;
            }
        }
    }
}
