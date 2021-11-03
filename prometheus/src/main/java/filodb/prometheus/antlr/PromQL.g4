
/*
 The Antlr Java source files were generated by running:

 java org.antlr.v4.Tool -package filodb.prometheus.antlr -visitor -no-listener PromQL.g4
*/

grammar PromQL;

expression: vectorExpression EOF;

// Note that the labels that begin with '#' characters aren't comments. They define explicit
// visitor methods.

vectorExpression
    : <assoc=right> vectorExpression powOp grouping? vectorExpression  #binaryOperation
    | unaryOp vectorExpression                                         #unaryOperation
    | vectorExpression multOp grouping? vectorExpression               #binaryOperation
    | vectorExpression addOp grouping? vectorExpression                #binaryOperation
    | vectorExpression compareOp grouping? vectorExpression            #binaryOperation
    | vectorExpression andUnlessOp grouping? vectorExpression          #binaryOperation
    | vectorExpression orOp grouping? vectorExpression                 #binaryOperation
    | vectorExpression subquery offset?                                #subqueryOperation
    | vectorExpression limit                                           #limitOperation
    | vector                                                           #vectorOperation
    ;

unaryOp:     ADD | SUB;
powOp:       POW;
multOp:      MUL | DIV | MOD;
addOp:       ADD | SUB;
compareOp:   (DEQ | NE | GT | LT | GE | LE) BOOL?;
andUnlessOp: AND | UNLESS;
orOp:        OR;

vector
    : function
    | aggregation
    | instantOrRangeSelector
    | literal
    | parens
    ;

parens: '(' vectorExpression ')';

// TODO: Make offset applicable to any expression.
instantOrRangeSelector: instantSelector window? (offset | at | offset at | at offset)? ;

instantSelector
    : metricName ('{' labelMatcherList? '}')?
    | '{' labelMatcherList '}'
    ;

window: '[' DURATION ']';

offset: OFFSET DURATION;
at: AT TIMESTAMP;

limit: LIMIT NUMBER;

subquery: '[' DURATION ':' DURATION? ']';

labelMatcher:     labelName labelMatcherOp STRING;
labelMatcherOp:   EQ | NE | RE | NRE;
labelMatcherList: labelMatcher (',' labelMatcher)*;

function: IDENTIFIER parameterList;

parameter:     literal | vectorExpression;
parameterList: '(' (parameter (',' parameter)*)? ')';

aggregation
    : AGGREGATION_OP parameterList
    | AGGREGATION_OP (by | without) parameterList
    | AGGREGATION_OP parameterList ( by | without)
    ;
by:      BY labelNameList;
without: WITHOUT labelNameList;

grouping:   (on | ignoring) (groupLeft | groupRight)?;
on:         ON labelNameList;
ignoring:   IGNORING labelNameList;
groupLeft:  GROUP_LEFT labelNameList?;
groupRight: GROUP_RIGHT labelNameList?;

metricName: metricKeyword | IDENTIFIER | IDENTIFIER_EXTENDED;

// Keywords which are also accepted as metric names.
metricKeyword
    : AND
    | OR
    | UNLESS
    | BY
    | WITHOUT
    | OFFSET
    | LIMIT
    | AGGREGATION_OP
    ;

labelName:     labelKeyword | IDENTIFIER;
labelNameList: '(' (labelName (',' labelName)*)? ')';

// Keywords which are also accepted as label names.
labelKeyword
    : AND
    | OR
    | UNLESS
    | BY
    | WITHOUT
    | ON
    | IGNORING
    | GROUP_LEFT
    | GROUP_RIGHT
    | OFFSET
    | LIMIT
    | BOOL
    | AGGREGATION_OP
    ;


literal: NUMBER | STRING;

TIMESTAMP: INTEGER;

// Number format is non-standard.
// It doesn't support hex, NaN, Inf, and it doesn't require a digit after the decimal point.
NUMBER: (
      [0-9]* '.'? [0-9]+ ([eE][-+]?[0-9]+)?
    | [0-9]+ '.'
    | '0' [xX] [0-9a-fA-F]+
    | INTEGER
);

INTEGER: [0-9]+;

STRING
    : '\'' (~('\'' | '\\') | '\\' .)* '\''
    | '"' (~('"' | '\\') | '\\' .)* '"'
    ;

ADD:  '+';
SUB:  '-';
MUL:  '*';
DIV:  '/';
MOD:  '%';
POW:  '^';

EQ:  '=';
DEQ: '==';
NE:  '!=';
GT:  '>';
LT:  '<';
GE:  '>=';
LE:  '<=';
RE:  '=~';
NRE: '!~';

AT: '@';

// See section below: "Magic for case-insensitive matching."
AND:         A N D;
OR:          O R;
UNLESS:      U N L E S S;
BY:          B Y;
WITHOUT:     W I T H O U T;
ON:          O N;
IGNORING:    I G N O R I N G;
GROUP_LEFT:  G R O U P '_' L E F T;
GROUP_RIGHT: G R O U P '_' R I G H T;
OFFSET:      O F F S E T;
LIMIT:       L I M I T;
BOOL:        B O O L;

// See section below: "Magic for case-insensitive matching."
AGGREGATION_OP
    : S U M
    | M I N
    | M A X
    | A V G
    | G R O U P
    | S T D D E V
    | S T D V A R
    | C O U N T
    | C O U N T '_' V A L U E S
    | B O T T O M K
    | T O P K
    | Q U A N T I L E
    | G R O U P
    ;

// The special 'i' form is for "Step Multiple Notation for PromQL Lookback (#821)"
DURATION: NUMBER ('s' | 'm' | 'h' | 'd' | 'w' | 'y' | 'i');

// Used for functions, labels, and metric names.
IDENTIFIER: [a-zA-Z_] [a-zA-Z0-9_]*;

// Used for metric names.
IDENTIFIER_EXTENDED: [_:]* [a-zA-Z] [a-zA-Z0-9_:\-.]*;

// Magic for case-insensitive matching.
fragment A : [aA]; // match either an 'a' or 'A'
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];

WS: [\r\t\n ]+ -> skip;
COMMENT: '#' ~[\r\n]* -> skip;
