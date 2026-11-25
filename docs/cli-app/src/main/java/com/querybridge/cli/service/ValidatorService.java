package com.querybridge.cli.service;   // CLI services

import com.querybridge.cli.model.SchemaContext;
import com.querybridge.cli.model.TableInfo;
import net.sf.jsqlparser.JSQLParserException;                  // thrown when SQL can't be parsed
import net.sf.jsqlparser.expression.Expression;                 // any expression node: a column, 1+2, COUNT(x), a = b ...
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;   // a visitor that walks an expression tree, doing nothing by default
import net.sf.jsqlparser.parser.CCJSqlParserUtil;               // the parser entry point: parse(String) -> Statement
import net.sf.jsqlparser.schema.Column;                         // a column reference node, e.g. products.name
import net.sf.jsqlparser.schema.Table;                          // a table reference node, e.g. products
import net.sf.jsqlparser.statement.Statement;                   // the root of any parsed SQL statement
import net.sf.jsqlparser.statement.select.FromItem;             // anything that can appear after FROM / JOIN
import net.sf.jsqlparser.statement.select.Join;                 // one JOIN clause
import net.sf.jsqlparser.statement.select.ParenthesedSelect;    // a "(SELECT ...)" in parentheses
import net.sf.jsqlparser.statement.select.PlainSelect;          // an ordinary SELECT ... FROM ... WHERE ...
import net.sf.jsqlparser.statement.select.Select;               // base class of all SELECT shapes
import net.sf.jsqlparser.statement.select.SelectItem;           // one item of the SELECT list
import net.sf.jsqlparser.statement.select.SetOperationList;      // SELECT ... UNION SELECT ...
import net.sf.jsqlparser.util.TablesNamesFinder;                // utility: list every table name in a statement
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

// ============================================================================
// ValidatorService — checks LLM-generated SQL against the REAL schema before
// anything runs. (guide step 37 — the heart of the "harness"; take your time)
//
// WHAT IT CHECKS
//   1. The SQL is not empty.
//   2. It PARSES (JSqlParser understands it).
//   3. It is a SELECT (not INSERT/UPDATE/DELETE/DROP/...).
//   4. Every TABLE it mentions exists in the schema.
//   5. Every COLUMN it mentions exists — in the named table if qualified
//      (products.price), or in at least one of the query's tables if not (price).
// It returns a LIST OF ERROR STRINGS (empty list = valid). Those exact strings
// are what get fed back to the LLM in a repair request, so they are written to
// be understandable by a model: "Unknown column 'nme' in table 'products'".
//
// WHAT IS JSqlParser AND WHAT IS A "PARSE TREE"?
//   JSqlParser reads an SQL string and builds a tree of Java objects that
//   mirrors the query's structure ("AST" = Abstract Syntax Tree). For
//       SELECT p.name FROM products p WHERE p.price > 10
//   you get, roughly:
//       PlainSelect
//         ├─ selectItems: [ SelectItem( Column(table=p, name=name) ) ]
//         ├─ fromItem:    Table(name=products, alias=p)
//         └─ where:       GreaterThan( Column(table=p, name=price), LongValue(10) )
//   Working on this tree is FAR more reliable than regexes on the text.
//
// THE VISITOR PATTERN (how we find all Column nodes in an expression)
//   Expressions nest arbitrarily: (a + b) * COUNT(c) > 5 AND d IS NULL.
//   Instead of writing a recursive walk with a big instanceof chain, JSqlParser
//   gives every node an accept(visitor) method that calls back
//   visitor.visit(<the node's concrete type>). ExpressionVisitorAdapter is a
//   ready-made visitor that visits every child of every node and does nothing
//   else. We SUBCLASS it (ColumnCollector, at the bottom) and override only
//   visit(Column) — so we get called for every column anywhere in the
//   expression, for free.
//
// ⚠ KNOWN LIMITATIONS — I RAN THESE AGAINST THE ACTUAL JAR (JSqlParser 4.9)
//   with a schema of products(id,name,category_id,price,created_at) and
//   categories(id,name), so these are observed results, not guesses:
//
//   FALSE ALARMS (valid SQL that gets rejected -> wasted repair attempts):
//     SELECT p.name FROM products p
//        -> "Unknown table in column reference: p"         (table ALIASES not understood)
//     SELECT name, COUNT(*) AS cnt FROM products GROUP BY name ORDER BY cnt DESC
//        -> "Unknown column 'cnt' in referenced tables"    (SELECT-list aliases not understood)
//     SELECT name FROM products WHERE true
//        -> "Unknown column 'true' in referenced tables"   (the parser treats `true` as a column)
//     SELECT sub.n FROM (SELECT name AS n FROM products) sub
//        -> "Unknown table in column reference: sub"       (subquery aliases)
//     WITH x AS (SELECT COUNT(*) AS c FROM products) SELECT c FROM x
//        -> "Unknown column 'c' in referenced tables"      (CTE output columns)
//   The alias one matters most, because LlmService's prompt literally tells the
//   model to "qualify column names with table names/aliases". After seeing the
//   error, the model usually rewrites with full table names, so it ends up
//   working — at the cost of an extra LLM round-trip (and up to 3 tries).
//
//   MISSED ERRORS (invalid SQL that gets accepted -> fails later, in the DB):
//     SELECT name FROM products WHERE category_id IN (SELECT id FROM categories WHERE nme = 'x')
//        -> accepted, although `nme` doesn't exist: columns inside sub-selects in
//           WHERE/SELECT are not visited. (Tables inside them ARE checked.)
//   Not fatal: the database rejects it and the user sees "Query execution failed".
//
//   SAFETY GAPS in the "SELECT only" rule:
//     SELECT * INTO t2 FROM products
//        -> accepted. In PostgreSQL, SELECT ... INTO creates a NEW TABLE t2!
//     SELECT name FROM products FOR UPDATE
//        -> accepted (takes row locks; mostly harmless in autocommit).
//     SELECT pg_terminate_backend(id) FROM products
//        -> accepted (functions with side effects are still "SELECT"s).
//   Correctly rejected: "DELETE FROM products" (not a SELECT),
//   "SELECT 1; DROP TABLE products" (parse error — only one statement allowed),
//   "WITH d AS (DELETE ... RETURNING id) SELECT ..." (JSqlParser 4.9 can't parse it).
//   The robust fix for safety is at the DATABASE level (a read-only role),
//   not in a parser — see QueryService.java.
// ============================================================================
@Service
public class ValidatorService {

    // ------------------------------------------------------------------------
    // validate(sql, schema) -> list of error messages (empty = OK)
    //
    // Stops early (returns immediately) on the three "fatal" problems — empty,
    // unparseable, not a SELECT — because nothing after them makes sense.
    // Otherwise collects ALL table/column errors in one pass, so the LLM can
    // fix everything in a single repair round.
    // ------------------------------------------------------------------------
    public List<String> validate(String sql, SchemaContext schema) {
        List<String> errors = new ArrayList<>();   // we'll add problems here

        // CHECK 1: empty?
        if (sql == null || sql.isBlank()) {
            errors.add("SQL is empty");   // e.g. the LLM's JSON had no "sql" key
            return errors;
        }

        // CHECK 2: does it parse?
        Statement statement;
        try {
            // parse() expects exactly ONE statement (a trailing ";" is fine).
            // Two statements ("SELECT 1; DROP TABLE x") -> JSQLParserException.
            statement = CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException e) {
            // The parser's message says where it got confused, e.g.
            // 'Encountered unexpected token: "DROP" ... at line 1, column 11.'
            // Good feedback for the LLM. (Caveat: JSqlParser doesn't support every
            // PostgreSQL-specific syntax, so occasionally VALID Postgres SQL fails
            // here too.)
            errors.add("Parse error: " + e.getMessage());
            return errors;
        }

        // CHECK 3: is it a SELECT?
        // In JSqlParser 4.x, Select is the common parent class of PlainSelect,
        // SetOperationList (UNION...), ParenthesedSelect, etc. Insert, Update,
        // Delete, Drop... are different Statement classes, so they fail here.
        if (!(statement instanceof Select)) {
            errors.add("Only SELECT statements are allowed");
            return errors;
        }

        Select select = (Select) statement;   // safe cast: we just checked the type

        // CHECK 4: tables.
        // TablesNamesFinder walks the WHOLE statement (including subqueries and
        // joins) and returns every table name it finds, as strings (possibly
        // schema-qualified, e.g. "public.products"). From the tests: it does NOT
        // list CTE names (the `x` in WITH x AS ...) — good, those aren't real tables.
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        // The (Statement) cast just picks the getTableList(Statement) overload.
        List<String> tableNames = tablesNamesFinder.getTableList((Statement) select);

        // Valid tables used by the query (lower-cased) — needed for CHECK 5.
        Set<String> referencedTables = new HashSet<>();
        for (String tableName : tableNames) {
            String normalized = normalizeIdentifier(tableName);   // "public.\"Products\"" -> "Products"
            if (!schema.hasTable(normalized)) {
                errors.add("Unknown table: " + normalized);        // e.g. the LLM invented "customers_v2"
            } else {
                referencedTables.add(normalized.toLowerCase());    // remember it for column checks
            }
        }

        // A SELECT without any (known) table: e.g. "SELECT 1", or every table was
        // unknown. Rejected, since a database question should touch some table.
        if (referencedTables.isEmpty()) {
            errors.add("No tables referenced in query");
        }

        // CHECK 5: columns.
        // Walk the parse tree and collect every column reference as a
        // (tableName-or-null, columnName) pair.
        List<ColumnReference> columnReferences = collectColumnReferences(select);
        for (ColumnReference ref : columnReferences) {
            if (ref.tableName() != null) {
                // QUALIFIED column, e.g. products.price -> check in THAT table.
                String tableName = normalizeIdentifier(ref.tableName());
                Optional<TableInfo> table = schema.findTable(tableName);
                if (table.isEmpty()) {
                    // The qualifier isn't a real table. This is where ALIASES
                    // ("p" in p.name) get wrongly rejected — the code never looks
                    // at what alias each table was given in the FROM clause.
                    errors.add("Unknown table in column reference: " + tableName);
                    continue;   // no point checking the column; go to the next reference
                }
                if (table.get().findColumn(ref.columnName()).isEmpty()) {
                    errors.add("Unknown column '" + ref.columnName() + "' in table '" + tableName + "'");
                }
            } else {
                // UNQUALIFIED column, e.g. just "price". Accept it if ANY table used
                // by the query has a column with that name.
                //   stream over referencedTables
                //   -> findTable(t)                     Optional<TableInfo>
                //   -> flatMap(table.findColumn(...))   Optional<ColumnInfo>
                //   -> isPresent()                      found in this table?
                // anyMatch stops at the first table that has it.
                // (This doesn't detect AMBIGUOUS columns — "id" in a join of two
                //  tables that both have id — PostgreSQL would reject that, the
                //  validator lets it through.)
                boolean found = referencedTables.stream()
                        .anyMatch(t -> schema.findTable(t)
                                .flatMap(table -> table.findColumn(ref.columnName()))
                                .isPresent());
                // Only report it if there ARE valid tables; otherwise CHECK 4 already
                // produced errors and this one would just be noise.
                if (!found && !referencedTables.isEmpty()) {
                    errors.add("Unknown column '" + ref.columnName() + "' in referenced tables");
                }
            }
        }

        return errors;   // empty list = valid
    }

    // ------------------------------------------------------------------------
    // collectColumnReferences(select)
    //
    // Creates an empty list and a ColumnCollector that appends to it, walks the
    // query with it, and returns the filled list.
    // ------------------------------------------------------------------------
    private List<ColumnReference> collectColumnReferences(Select select) {
        List<ColumnReference> references = new ArrayList<>();
        ColumnCollector collector = new ColumnCollector(references);   // the visitor writes into `references`
        visitSelect(select, collector);
        return references;
    }

    // ------------------------------------------------------------------------
    // THE WALK. Three small mutually-recursive methods handle the possible
    // SHAPES of a SELECT:
    //
    //   visitSelect      — unwraps a parenthesised "(SELECT ...)"
    //   visitSelectBody  — PlainSelect? visit it. UNION/INTERSECT/EXCEPT?
    //                      visit each side.
    //   visitPlainSelect — visits the expressions of each clause:
    //                      SELECT list, FROM, JOIN ... ON, WHERE, HAVING,
    //                      GROUP BY, ORDER BY.
    //
    // What is NOT visited: WITH (CTE) bodies, sub-selects nested inside
    // expressions (WHERE x IN (SELECT ...)), LIMIT/OFFSET. That's the source of
    // the "missed errors" listed at the top.
    // ------------------------------------------------------------------------
    private void visitSelect(Select select, ColumnCollector collector) {
        if (select instanceof ParenthesedSelect parenthesedSelect) {   // pattern matching for instanceof (Java 16+): test AND cast in one go
            visitSelectBody(parenthesedSelect.getSelect(), collector);  // go inside the parentheses
            return;
        }
        visitSelectBody(select, collector);
    }

    private void visitSelectBody(Select select, ColumnCollector collector) {
        // getSelectBody() is an older JSqlParser API kept for compatibility; in
        // 4.x a Select generally IS its own body (it returns `this`).
        if (select.getSelectBody() instanceof PlainSelect plainSelect) {
            visitPlainSelect(plainSelect, collector);                 // ordinary SELECT
        } else if (select.getSelectBody() instanceof SetOperationList setOperationList) {
            for (Select body : setOperationList.getSelects()) {       // each SELECT of a UNION/INTERSECT/EXCEPT
                visitSelect(body, collector);                          // recurse (each may itself be parenthesised)
            }
        }
        // Any other shape: silently skipped.
    }

    // ------------------------------------------------------------------------
    // visitPlainSelect — visit each clause's expressions. For every
    // Expression, `expression.accept(collector)` starts the visitor, which
    // then walks into function arguments, arithmetic, comparisons, AND/OR, ...
    // and calls ColumnCollector.visit(Column) for each column it meets.
    // ------------------------------------------------------------------------
    private void visitPlainSelect(PlainSelect plainSelect, ColumnCollector collector) {
        // SELECT list: "SELECT a, b + 1 AS c, COUNT(d)" -> a, b, d
        if (plainSelect.getSelectItems() != null) {
            for (SelectItem<?> item : plainSelect.getSelectItems()) {   // SelectItem is generic in 4.9, hence <?>
                Expression expression = item.getExpression();
                if (expression != null) {
                    expression.accept(collector);   // `*` is an AllColumns node, not a Column, so it's ignored
                }
            }
        }

        // FROM: only interesting if it's a sub-select "(SELECT ...) alias";
        // a plain table name has no columns to check (tables were done in CHECK 4).
        if (plainSelect.getFromItem() != null) {
            visitFromItem(plainSelect.getFromItem(), collector);
        }

        // JOINs: the joined item (maybe a sub-select) and its ON conditions.
        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                if (join.getFromItem() != null) {
                    visitFromItem(join.getFromItem(), collector);   // the thing being joined
                }
                if (join.getOnExpressions() != null) {               // ON a.x = b.y (a list in 4.x)
                    for (Expression onExpr : join.getOnExpressions()) {
                        onExpr.accept(collector);
                    }
                }
                // (JOIN ... USING (col) is not checked — getUsingColumns() isn't visited.)
            }
        }

        // WHERE and HAVING are single (possibly huge) expressions.
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(collector);
        }
        if (plainSelect.getHaving() != null) {
            plainSelect.getHaving().accept(collector);
        }
        // GROUP BY: a list of items; only Expression items are visited.
        if (plainSelect.getGroupBy() != null && plainSelect.getGroupBy().getGroupByExpressionList() != null) {
            plainSelect.getGroupBy().getGroupByExpressionList().forEach(item -> {
                if (item instanceof Expression expression) {   // pattern-matching instanceof again
                    expression.accept(collector);
                }
            });
        }
        // ORDER BY: each element wraps an expression. This is where SELECT-list
        // aliases ("ORDER BY cnt") get reported as unknown columns.
        if (plainSelect.getOrderByElements() != null) {
            plainSelect.getOrderByElements().forEach(order -> order.getExpression().accept(collector));
        }
    }

    // ------------------------------------------------------------------------
    // visitFromItem — recurse into sub-selects used as tables:
    //     FROM (SELECT nme FROM products) sub
    // Columns INSIDE get checked (nme -> error, observed). Columns that
    // reference the sub-select from OUTSIDE (sub.n) are wrongly rejected,
    // because "sub" is an alias, not a table.
    // ------------------------------------------------------------------------
    private void visitFromItem(FromItem fromItem, ColumnCollector collector) {
        if (fromItem instanceof ParenthesedSelect parenthesedSelect) {
            visitSelect(parenthesedSelect.getSelect(), collector);
        }
        // A plain Table: nothing to do.
    }

    // ------------------------------------------------------------------------
    // normalizeIdentifier — turn a raw table name from the parser into a bare
    // name we can look up:
    //     "products"            -> "products"
    //     "public.products"     -> "products"     (drop schema prefix)
    //     "\"Products\""        -> "Products"     (drop quotes; lookup ignores case anyway)
    // Dropping the schema prefix means "other_schema.products" would be
    // accepted if public has a "products" — a small leniency.
    // ------------------------------------------------------------------------
    private String normalizeIdentifier(String identifier) {
        if (identifier == null) {
            return "";
        }
        String normalized = identifier.trim();
        if (normalized.contains(".")) {
            normalized = normalized.substring(normalized.lastIndexOf('.') + 1);   // keep only the part after the last dot
        }
        return normalized.replace("\"", "");
    }

    // ------------------------------------------------------------------------
    // ColumnReference — a tiny private RECORD: an immutable (tableName,
    // columnName) pair. Records (Java 16+) auto-generate the constructor, the
    // accessors tableName() / columnName() (note: no "get" prefix), equals,
    // hashCode and toString. tableName is null for unqualified columns.
    // ------------------------------------------------------------------------
    private record ColumnReference(String tableName, String columnName) {
    }

    // ------------------------------------------------------------------------
    // ColumnCollector — the visitor. Extends ExpressionVisitorAdapter (which
    // already walks every child node) and overrides ONLY visit(Column).
    //
    // `static` nested class: it doesn't need a reference to the enclosing
    // ValidatorService instance; it only writes into the list it was given.
    // ------------------------------------------------------------------------
    private static class ColumnCollector extends ExpressionVisitorAdapter {

        private final List<ColumnReference> references;   // the shared output list

        ColumnCollector(List<ColumnReference> references) {
            this.references = references;
        }

        // Called by JSqlParser for every Column node anywhere in a visited expression.
        @Override
        public void visit(Column column) {
            String columnName = normalizeColumnName(column.getColumnName());   // "\"price\"" -> "price"
            Table table = column.getTable();                                   // the qualifier, if any ("products" in products.price)
            // getName() gives just the table part (no schema). null table -> unqualified.
            String tableName = table != null ? normalizeColumnName(table.getName()) : null;
            if (columnName != null && !columnName.equals("*")) {   // defensive: skip a bare "*"
                references.add(new ColumnReference(tableName, columnName));
            }
        }

        // Strip double quotes from an identifier (null-safe).
        private String normalizeColumnName(String name) {
            if (name == null) {
                return null;
            }
            return name.replace("\"", "");
        }
    }
}
