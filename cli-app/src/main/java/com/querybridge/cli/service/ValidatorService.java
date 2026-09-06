package com.querybridge.cli.service;

import com.querybridge.cli.model.SchemaContext;
import com.querybridge.cli.model.TableInfo;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Service
public class ValidatorService {

    public List<String> validate(String sql, SchemaContext schema) {
        List<String> errors = new ArrayList<>();

        if (sql == null || sql.isBlank()) {
            errors.add("SQL is empty");
            return errors;
        }

        Statement statement;
        try {
            statement = CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException e) {
            errors.add("Parse error: " + e.getMessage());
            return errors;
        }

        if (!(statement instanceof Select)) {
            errors.add("Only SELECT statements are allowed");
            return errors;
        }

        Select select = (Select) statement;
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableNames = tablesNamesFinder.getTableList((Statement) select);

        Set<String> referencedTables = new HashSet<>();
        for (String tableName : tableNames) {
            String normalized = normalizeIdentifier(tableName);
            if (!schema.hasTable(normalized)) {
                errors.add("Unknown table: " + normalized);
            } else {
                referencedTables.add(normalized.toLowerCase());
            }
        }

        if (referencedTables.isEmpty()) {
            errors.add("No tables referenced in query");
        }

        List<ColumnReference> columnReferences = collectColumnReferences(select);
        for (ColumnReference ref : columnReferences) {
            if (ref.tableName() != null) {
                String tableName = normalizeIdentifier(ref.tableName());
                Optional<TableInfo> table = schema.findTable(tableName);
                if (table.isEmpty()) {
                    errors.add("Unknown table in column reference: " + tableName);
                    continue;
                }
                if (table.get().findColumn(ref.columnName()).isEmpty()) {
                    errors.add("Unknown column '" + ref.columnName() + "' in table '" + tableName + "'");
                }
            } else {
                boolean found = referencedTables.stream()
                        .anyMatch(t -> schema.findTable(t)
                                .flatMap(table -> table.findColumn(ref.columnName()))
                                .isPresent());
                if (!found && !referencedTables.isEmpty()) {
                    errors.add("Unknown column '" + ref.columnName() + "' in referenced tables");
                }
            }
        }

        return errors;
    }

    private List<ColumnReference> collectColumnReferences(Select select) {
        List<ColumnReference> references = new ArrayList<>();
        ColumnCollector collector = new ColumnCollector(references);
        visitSelect(select, collector);
        return references;
    }

    private void visitSelect(Select select, ColumnCollector collector) {
        if (select instanceof ParenthesedSelect parenthesedSelect) {
            visitSelectBody(parenthesedSelect.getSelect(), collector);
            return;
        }
        visitSelectBody(select, collector);
    }

    private void visitSelectBody(Select select, ColumnCollector collector) {
        if (select.getSelectBody() instanceof PlainSelect plainSelect) {
            visitPlainSelect(plainSelect, collector);
        } else if (select.getSelectBody() instanceof SetOperationList setOperationList) {
            for (Select body : setOperationList.getSelects()) {
                visitSelect(body, collector);
            }
        }
    }

    private void visitPlainSelect(PlainSelect plainSelect, ColumnCollector collector) {
        if (plainSelect.getSelectItems() != null) {
            for (SelectItem<?> item : plainSelect.getSelectItems()) {
                Expression expression = item.getExpression();
                if (expression != null) {
                    expression.accept(collector);
                }
            }
        }

        if (plainSelect.getFromItem() != null) {
            visitFromItem(plainSelect.getFromItem(), collector);
        }

        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                if (join.getFromItem() != null) {
                    visitFromItem(join.getFromItem(), collector);
                }
                if (join.getOnExpressions() != null) {
                    for (Expression onExpr : join.getOnExpressions()) {
                        onExpr.accept(collector);
                    }
                }
            }
        }

        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(collector);
        }
        if (plainSelect.getHaving() != null) {
            plainSelect.getHaving().accept(collector);
        }
        if (plainSelect.getGroupBy() != null && plainSelect.getGroupBy().getGroupByExpressionList() != null) {
            plainSelect.getGroupBy().getGroupByExpressionList().forEach(item -> {
                if (item instanceof Expression expression) {
                    expression.accept(collector);
                }
            });
        }
        if (plainSelect.getOrderByElements() != null) {
            plainSelect.getOrderByElements().forEach(order -> order.getExpression().accept(collector));
        }
    }

    private void visitFromItem(FromItem fromItem, ColumnCollector collector) {
        if (fromItem instanceof ParenthesedSelect parenthesedSelect) {
            visitSelect(parenthesedSelect.getSelect(), collector);
        }
    }

    private String normalizeIdentifier(String identifier) {
        if (identifier == null) {
            return "";
        }
        String normalized = identifier.trim();
        if (normalized.contains(".")) {
            normalized = normalized.substring(normalized.lastIndexOf('.') + 1);
        }
        return normalized.replace("\"", "");
    }

    private record ColumnReference(String tableName, String columnName) {
    }

    private static class ColumnCollector extends ExpressionVisitorAdapter {

        private final List<ColumnReference> references;

        ColumnCollector(List<ColumnReference> references) {
            this.references = references;
        }

        @Override
        public void visit(Column column) {
            String columnName = normalizeColumnName(column.getColumnName());
            Table table = column.getTable();
            String tableName = table != null ? normalizeColumnName(table.getName()) : null;
            if (columnName != null && !columnName.equals("*")) {
                references.add(new ColumnReference(tableName, columnName));
            }
        }

        private String normalizeColumnName(String name) {
            if (name == null) {
                return null;
            }
            return name.replace("\"", "");
        }
    }
}
