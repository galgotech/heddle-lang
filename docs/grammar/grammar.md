# Heddle Lang Grammar (v0.1.0)

This document defines the formal grammar specification for the Heddle language. It specifies the lexical tokens and syntactic rules required to parse Heddle code. It does not cover language semantics, compiler implementation, or architectural concepts. 

Heddle is a strictly-typed, domain-specific language (DSL) built for high-performance data orchestration. The language integrates a declarative configuration structure with native Pipelined Relational Query Language (PRQL) and strict type contracts.

## 1. Lexical Grammar

The lexical grammar defines the fundamental tokens that the compiler recognizes during parsing.

### 1.1. Keywords
The language reserves the following keywords for core constructs:
```
import    resource    step      handler workflow
true      false       null      int     float     string  boolean 
```

### 1.2. Operators and Punctuation
The language uses the following characters for operators and punctuation:
```
=         :         {         }         (         )
[         ]         *         >         <         |
?         .
```

### 1.3. Identifiers and Literals
The language defines specific patterns for identifiers and literal values:
- **IDENTIFIER**: Matches the regular expression `/[a-zA-Z_][a-zA-Z0-9\-_]*/`. It supports alphanumeric characters, underscores, and hyphens.
- **ESCAPED_STRING**: Represents a standard double-quoted string that supports backslash escapes.
- **SIGNED_NUMBER**: Represents an integer or a floating-point number.
- **PRQL_BLOCK**: Encloses a block of PRQL code within parentheses, such as `(from input select {id, email})`.

### 1.4. Layout
The compiler uses the following layout tokens to manage document structure and block scoping:
- **NEWLINE**: Represents one or more physical line breaks.
- **INDENT**: Increases the indentation level to open a block scope.
- **DEDENT**: Decreases the indentation level to close a block scope.

---

## 2. Syntactic Grammar

The syntactic grammar uses Extended Backus-Naur Form (EBNF) to define how lexical tokens combine into valid Heddle language constructs. The grammar definitions are categorized as follows:

- **Program Structure**: A program consists of imports, declarations, and routines.
- **Declarations**: Declarations bind external resources or functions to identifiers.
- **Routines**: Routines define execution workflows and error handlers.
- **Pipelines**: Pipelines define the flow of data between steps and relational queries.
- **Data and Literals**: Data structures define inline dataframes, dictionaries, and lists.

### Formal EBNF Specification

```ebnf
// --- Program Structure ---
program     = { ( import_stmt | declaration | routine ) { newline } } ;

import_stmt = "import" ESCAPED_STRING IDENTIFIER ;

// --- Declarations ---
declaration      = resource_binding
                 | step_binding ;

resource_binding = "resource" IDENTIFIER "=" function_ref ;

step_binding     = "step" IDENTIFIER "=" function_ref ;

function_ref     = [ resource_ref ] [ IDENTIFIER "." ] IDENTIFIER [ function_config ] ;

resource_ref     = "<" IDENTIFIER "=" IDENTIFIER { "," IDENTIFIER "=" IDENTIFIER } ">" ;

function_config  = dict ;

// --- Routines ---
routine        = handler_def | workflow_def ;

handler_def    = "handler" IDENTIFIER handler_block ;
handler_block  = "{" newline indent [ { newline } handler_stmt { newline handler_stmt } [ newline ] ] dedent newline "}" ;
handler_stmt   = [ "*" ] pipeline_stmt ;

workflow_def   = "workflow" IDENTIFIER [ trap_handler ] workflow_block ;
workflow_block = "{" newline indent [ { newline } pipeline_stmt { newline pipeline_stmt } [ newline ] ] dedent newline "}" ;

// --- Pipelines ---
pipeline_stmt   = ( dataframe | pipe_chain ) [ assignment_stmt ] ;

assignment_stmt = newline ">" IDENTIFIER ;

pipe_chain      = call_expr { pipe_op call_expr } ;

pipe_op         = newline indent "|" ;

call_expr       = ( step_call | query_block ) [ trap_handler ] ;

step_call       = IDENTIFIER ;

trap_handler    = "?" IDENTIFIER ;

query_block     = PRQL_BLOCK ;

// --- Data and Literals ---
dataframe      = "[" [ newline ] [ dataframe_dict { [ "," ] [ newline ] dataframe_dict } [ newline ] ] "]" ;
dataframe_dict = "{" newline indent [ dataframe_pair { [ "," ] newline dataframe_pair } [ "," ] [ newline ] ] dedent newline "}" ;
dataframe_pair = IDENTIFIER ":" dataframe_primitive ;
dataframe_primitive = ESCAPED_STRING | SIGNED_NUMBER | "true" | "false" | "null" ;

dict           = "{" newline indent [ pair { [ "," ] newline pair } [ "," ] [ newline ] ] dedent newline "}" ;
pair           = IDENTIFIER ":" literal ;

list           = "[" [ newline ] [ literal { [ "," ] [ newline ] literal } [ newline ] ] "]" ;

literal        = dict
               | list
               | ESCAPED_STRING
               | SIGNED_NUMBER
               | "true"
               | "false"
               | "null" ;
```
