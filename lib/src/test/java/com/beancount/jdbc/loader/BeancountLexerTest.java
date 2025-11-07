package com.beancount.jdbc.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.beancount.jdbc.loader.grammar.BeancountLexer;
import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.junit.jupiter.api.Test;

class BeancountLexerTest {

    @Test
    void includeDirectiveProducesIncludeAndStringTokens() {
        String ledger = "include \"path/to/file.bean\"\n";
        BeancountLexer lexer = new BeancountLexer(CharStreams.fromString(ledger, "test"));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();

        List<String> symbolic = new ArrayList<>();
        for (Token token : tokens.getTokens()) {
            if (token.getType() == Token.EOF) {
                symbolic.add("EOF");
            } else {
                symbolic.add(lexer.getVocabulary().getSymbolicName(token.getType()));
            }
        }

        assertEquals(List.of("INCLUDE_KEY", "STRING", "NEWLINE", "EOF"), symbolic);
    }

    @Test
    void directiveLineEmitsStringTokenAlongsideLineContent() {
        String ledger = "2024-01-01 note Assets:Cash \"Memo\"\n";
        BeancountLexer lexer = new BeancountLexer(CharStreams.fromString(ledger, "test"));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();

        List<String> symbolic = new ArrayList<>();
        for (Token token : tokens.getTokens()) {
            if (token.getType() == Token.EOF) {
                symbolic.add("EOF");
            } else {
                symbolic.add(lexer.getVocabulary().getSymbolicName(token.getType()));
            }
        }

        assertEquals(
                List.of("DATE", "DIRECTIVE_KEY", "LINE_CONTENT", "STRING", "NEWLINE", "EOF"), symbolic);
    }
}
