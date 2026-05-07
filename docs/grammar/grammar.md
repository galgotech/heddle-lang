# Heddle Lang Grammar (v0.1.0)

This document specifies the formal grammar for Heddle Lang, a strictly-typed, domain-specific language (DSL) designed for high-performance data orchestration. Heddle combines a declarative structure with native PRQL integration and strict type contracts.

## 1. Lexical Grammar

### 1.1. Keywords
```
import    resource    step      handler workflow
true      false       null      int     float     string  boolean 
```

### 1.2. Operators & Punctuation
```
=         :         {         }         (         )
[         ]         *         >         <         |
?         .
```

### 1.3. Identifiers & Literals
- **IDENTIFIER**: `/[a-zA-Z_][a-zA-Z0-9\-_]*/` (Supports alphanumeric characters, underscores, and hyphens).
- **ESCAPED_STRING**: Standard double-quoted string with backslash escapes.
- **SIGNED_NUMBER**: Integer or floating-point number.
- **PRQL_BLOCK**: A parenthesized block of PRQL code, e.g., `(from input select {id, email})`.

### 1.4. Layout
- **NEWLINE**: One or more physical line breaks.
- **INDENT**: Increase in indentation level (used for block scoping).
- **DEDENT**: Decrease in indentation level.

---

## 2. Syntactic Grammar

### 2.1. Program Structure
```ebnf
program     = { ( import_stmt | declaration | routine ) { newline } } ;

import_stmt = "import" ESCAPED_STRING IDENTIFIER ;
```

### 2.2. Declarations
```ebnf
declaration      = resource_binding
                 | step_binding ;

resource_binding = "resource" IDENTIFIER "=" function_ref ;

step_binding     = "step" IDENTIFIER "=" function_ref ;

function_ref     = [ IDENTIFIER "." ] IDENTIFIER [ resource_ref ] [ function_config ] ;

resource_ref     = "<" IDENTIFIER "=" IDENTIFIER { "," IDENTIFIER "=" IDENTIFIER } ">" ;

function_config  = dict ;
```

### 2.3. Routines (Workflows & Handlers)
```ebnf
routine        = handler_def | workflow_def ;

handler_def    = "handler" IDENTIFIER handler_block ;
handler_block  = "{" newline indent [ { newline } handler_stmt { newline handler_stmt } [ newline ] ] dedent newline "}" ;
handler_stmt   = [ "*" ] pipeline_stmt ;

workflow_def   = "workflow" IDENTIFIER [ trap_handler ] workflow_block ;
workflow_block = "{" newline indent [ { newline } pipeline_stmt { newline pipeline_stmt } [ newline ] ] dedent newline "}" ;
```

### 2.4. Pipelines
```ebnf
pipeline_stmt   = ( dataframe | pipe_chain ) [ assignment_stmt ] ;

assignment_stmt = newline ">" IDENTIFIER ;

pipe_chain      = call_expr { pipe_op call_expr } ;

pipe_op         = newline indent "|" ;

call_expr       = ( step_call | query_block ) [ trap_handler ] ;

step_call       = IDENTIFIER ;

trap_handler    = "?" IDENTIFIER ;

query_block     = PRQL_BLOCK ;
```

### 2.5. Data & Literals
```ebnf
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
