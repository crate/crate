package org.cratedb.action.sql;

import org.elasticsearch.index.mapper.DocumentMapper;

import java.util.List;

public interface ITableExecutionContext {

    public List<String> primaryKeys();
    public List<String> primaryKeysIncludingDefault();
    public Iterable<String> allCols();
    public Boolean isRouting(String name);
}
