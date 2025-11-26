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
    | orgHeadingStatement
    | queryTerminatorStatement
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
    : POPTAG lineContent? NEWLINE?
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
    : (DIRECTIVE_KEY | FLAG | OPTION) lineContent?
    | lineContent
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
    : DIRECTIVE_KEY
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

orgHeadingStatement
    : ORG_HEADING NEWLINE?
    ;

queryTerminatorStatement
    : QUERY_TERMINATOR NEWLINE?
    ;

blankStatement
    : NEWLINE
    ;

lineContent
    : (STRING | ACCOUNT_NAME | CURRENCY | NUMBER | DATE | FLAG | COLON | COMMA | LBRACE | RBRACE | AT | ATAT | TILDE | COMMENT | DIRECTIVE_KEY)+
    ;

DATE
    : DIGIT DIGIT DIGIT DIGIT '-' DIGIT DIGIT? '-' DIGIT DIGIT?
    ;

INCLUDE_KEY
    : 'include-once'
    | 'include'
    ;

COMMENT
    : ';' ~[\r\n]*
    | {getCharPositionInLine() == 0}? '#' ~[\r\n]*
    ;

ORG_HEADING
    : {getCharPositionInLine() == 0}? '*'+ ~[\r\n]*
    ;

QUERY_TERMINATOR
    : {getCharPositionInLine() == 0}? '"' [ \t]*
    ;

STRING
    : '"' (ESC_SEQ | ~["\\])* '"'
    | '\'' (ESC_SEQ | ~['\\])* '\''
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
    : '-'? DIGIT+ (('.' | ',') DIGIT+)?
    ;

CURRENCY
    : {getCharPositionInLine() > 0}? [A-Z][A-Z0-9.\-]*
    ;

ACCOUNT_NAME
    : {getCharPositionInLine() > 0}? [A-Z][A-Za-z0-9\-]* (':' [A-Za-z0-9\-]+)+
    ;

DIRECTIVE_KEY
    : ~[ \t\r\n:,@{}]+
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
