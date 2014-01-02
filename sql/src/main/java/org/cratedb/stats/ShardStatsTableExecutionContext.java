package org.cratedb.stats;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.collect.Expression;
import org.cratedb.action.collect.FieldLookupExpression;
import org.cratedb.action.sql.ITableExecutionContext;
import org.cratedb.index.ColumnDefinition;
import org.cratedb.lucene.LuceneFieldMapper;
import org.cratedb.lucene.fields.LuceneField;
import org.cratedb.mapper.FieldMapper;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.parser.NodeType;
import org.cratedb.sql.parser.parser.ValueNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ShardStatsTableExecutionContext implements ITableExecutionContext {

    public static final String SCHEMA_NAME = "STATS";

    private final ImmutableMap<String, ColumnDefinition> columnDefinitions;

    public ShardStatsTableExecutionContext() {
        ImmutableMap.Builder<String, ColumnDefinition> builder = new ImmutableMap.Builder<>();
        int position = 0;
        for (Map.Entry<String, LuceneField> entry : luceneFieldMapper().entrySet()) {
            builder.put(entry.getKey(), entry.getValue().getColumnDefinition(SCHEMA_NAME,
                    position++));
        }
        this.columnDefinitions = builder.build();
    }

    @Override
    public LuceneFieldMapper luceneFieldMapper() {
        return ShardStatsTable.fieldMapper();
    }

    @Override
    public FieldMapper mapper() {
        return luceneFieldMapper();
    }

    @Override
    public Object mappedValue(String name, Object value) {
        return value;
    }

    @Override
    public List<String> primaryKeys() {
        return new ArrayList<>(0);
    }

    @Override
    public List<String> primaryKeysIncludingDefault() {
        return new ArrayList<>(0);
    }

    @Override
    public Iterable<String> allCols() {
        return ShardStatsTable.cols();
    }

    @Override
    public boolean hasCol(String name) {
        return luceneFieldMapper().containsKey(name);
    }

    @Override
    public ColumnDefinition getColumnDefinition(String name) {
        return columnDefinitions.get(name);
    }

    @Override
    public Boolean isRouting(String name) {
        return false;
    }

    public boolean tableIsAlias() {
        return false;
    }

    @Override
    public boolean isMultiValued(String columnName) {
        LuceneField field =  luceneFieldMapper().get(columnName);
        return field != null && field.allowMultipleValues;
    }

    @Override
    public Expression getCollectorExpression(ValueNode node) {
        if (node.getNodeType() != NodeType.COLUMN_REFERENCE &&
                node.getNodeType() != NodeType.NESTED_COLUMN_REFERENCE) {
            return null;
        }

        ColumnDefinition columnDefinition = getColumnDefinition(node.getColumnName());
        if (columnDefinition != null) {
            return FieldLookupExpression.create(columnDefinition);
        }
        throw new SQLParseException(String.format("Unknown column '%s'", node.getColumnName()));
    }
}