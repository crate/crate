package org.cratedb.action.sql;

import org.cratedb.sql.types.SQLFieldMapper;

import java.util.List;

public interface ITableExecutionContext {

    public SQLFieldMapper mapper();
    public Object mappedValue(String name, Object value);
    public List<String> primaryKeys();
    public List<String> primaryKeysIncludingDefault();
    public Iterable<String> allCols();
    public boolean hasCol(String name);
    public Boolean isRouting(String name);
    public boolean tableIsAlias();
}
