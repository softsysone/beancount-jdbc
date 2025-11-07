grammar Beancount;

@header {
package com.beancount.jdbc.loader.grammar;
}

ledger
    : statement* EOF
    ;

statement
    : directiveStatement
    | optionStatement
    | pluginStatement
    | pushtagStatement
    | poptagStatement
    | pushmetaStatement
    | popmetaStatement
    | includeStatement
    | commentStatement
    | blankStatement
    ;

optionStatement
    : OPTION STRING STRING NEWLINE?
    ;

pluginStatement
    : PLUGIN STRING (STRING)? NEWLINE?
    ;

pushtagStatement
    : PUSHTAG lineContent NEWLINE?
    ;

poptagStatement
    : POPTAG lineContent NEWLINE?
    ;

pushmetaStatement
    : PUSHMETA lineContent NEWLINE?
    ;

popmetaStatement
    : POPMETA lineContent NEWLINE?
    ;

directiveStatement
    : DATE directiveHead NEWLINE continuationStatement*
    ;

directiveHead
    : (DIRECTIVE_KEY | FLAG) directiveBody?
    | LINE_CONTENT directiveBody?
    ;

directiveBody
    : lineContent
    ;

continuationStatement
    : INDENT continuationContent? NEWLINE
    ;

continuationContent
    : postingLine
    | metadataLine
    | commentLine
    | lineContent
    ;

postingLine
    : postingFlag? postingAccount postingAmount? postingCost? postingPrice? postingComment?
    ;

postingFlag
    : FLAG
    ;

postingAccount
    : ACCOUNT_NAME
    ;

postingAmount
    : NUMBER postingCurrency?
    ;

postingCurrency
    : CURRENCY
    ;

postingCost
    : LBRACE postingAmount (COMMA postingCostComponent)* RBRACE
    ;

postingCostComponent
    : postingAmount
    | DATE
    | STRING
    ;

postingPrice
    : (ATAT | AT) postingAmount
    ;

postingComment
    : COMMENT
    ;

metadataLine
    : metadataKey COLON metadataValue?
    ;

metadataKey
    : LINE_CONTENT
    ;

metadataValue
    : lineContent
    ;

commentLine
    : COMMENT
    ;

includeStatement
    : INCLUDE_KEY STRING NEWLINE?
    ;

commentStatement
    : COMMENT NEWLINE?
    ;

blankStatement
    : NEWLINE
    ;

lineContent
    : (LINE_CONTENT | STRING | ACCOUNT_NAME | CURRENCY | NUMBER | FLAG | COLON | COMMA | LBRACE | RBRACE | AT | ATAT | TILDE)+
    ;

DATE
    : DIGIT DIGIT DIGIT DIGIT '-' DIGIT DIGIT '-' DIGIT DIGIT
    ;

INCLUDE_KEY
    : 'include-once'
    | 'include'
    ;

COMMENT
    : (';' | '#') ~[\r\n]*
    ;

STRING
    : '"' (ESC_SEQ | ~["\\\r\n])* '"'
    | '\'' (ESC_SEQ | ~['\\\r\n])* '\''
    ;

OPTION
    : 'option'
    ;

PLUGIN
    : 'plugin'
    ;

POPT
    : 'popt'
    ;

PUSHTAG
    : 'pushtag'
    ;

POPTAG
    : 'poptag'
    ;

PUSHMETA
    : 'pushmeta'
    ;

POPMETA
    : 'popmeta'
    ;

PUSH
    : 'push'
    ;

POP
    : 'pop'
    ;

INLINE_WS
    : {getCharPositionInLine() > 0}? [ \t]+ -> skip
    ;

FLAG
    : [*!?]
    ;

ATAT
    : '@@'
    ;

AT
    : '@'
    ;

LBRACE
    : '{'
    ;

RBRACE
    : '}'
    ;

COMMA
    : ','
    ;

TILDE
    : '~'
    ;

COLON
    : ':'
    ;

NUMBER
    : '-'? DIGIT+ ('.' DIGIT+)?
    ;

CURRENCY
    : {getCharPositionInLine() > 0}? [A-Z][A-Z0-9.\-]*
    ;

ACCOUNT_NAME
    : {getCharPositionInLine() > 0}? [A-Z][A-Za-z0-9\-]* (':' [A-Za-z0-9\-]+)+
    ;

DIRECTIVE_KEY
    : ~[ \t\r\n]+
    ;

// Matches continuation text after the first column only, stopping before quoted strings.
// Mirrors the column-sensitive logic in beancount/parser/lexer.l, which emits separate STRING tokens.
LINE_CONTENT
    : {getCharPositionInLine() > 0}? ~["\r\n]+
    ;

INDENT
    : {getCharPositionInLine() == 0}? [ \t]+
    ;

NEWLINE
    : '\r'? '\n'
    ;

fragment DIGIT
    : [0-9]
    ;

fragment ESC_SEQ
    : '\\' ['"\\]
    ;

WS
    : [\u000B\u000C]+ -> skip
    ;
