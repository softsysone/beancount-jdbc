package com.beancount.jdbc.loader;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.misc.ParseCancellationException;

final class ThrowingErrorListener extends BaseErrorListener {
    static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

    private ThrowingErrorListener() {}

    @Override
    public void syntaxError(
            Recognizer<?, ?> recognizer,
            Object offendingSymbol,
            int line,
            int charPositionInLine,
            String msg,
            RecognitionException e) {
        throw new ParseCancellationException(
                "line " + line + ":" + (charPositionInLine + 1) + " " + msg, e);
    }
}
