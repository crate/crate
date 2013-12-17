package org.cratedb.action.sql;

import com.google.common.base.Optional;
import org.cratedb.action.collect.Expression;
import org.cratedb.index.ColumnDefinition;
import org.cratedb.lucene.LuceneFieldMapper;
import org.cratedb.mapper.FieldMapper;
import org.cratedb.sql.parser.parser.ValueNode;

import java.util.List;

public interface ITableExecutionContext {

    public FieldMapper mapper();
    public LuceneFieldMapper luceneFieldMapper();
    public Object mappedValue(String name, Object value);
    public List<String> primaryKeys();
    public List<String> primaryKeysIncludingDefault();
    public Iterable<String> allCols();
    public boolean hasCol(String name);
    public ColumnDefinition getColumnDefinition(String name);
    public Boolean isRouting(String name);
    public boolean tableIsAlias();
    public boolean isMultiValued(String columnName);

    public Expression getCollectorExpression(ValueNode node);
}
