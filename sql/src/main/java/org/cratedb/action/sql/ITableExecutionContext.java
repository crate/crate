package org.cratedb.action.sql;

import org.cratedb.index.ColumnDefinition;
import org.cratedb.lucene.LuceneFieldMapper;
import org.cratedb.mapper.FieldMapper;

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
}
