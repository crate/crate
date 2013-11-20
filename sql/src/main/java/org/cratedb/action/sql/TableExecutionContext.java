package org.cratedb.action.sql;

import org.cratedb.core.IndexMetaDataExtractor;
import org.cratedb.sql.ValidationException;
import org.cratedb.sql.types.SQLFieldMapper;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class TableExecutionContext implements ITableExecutionContext {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final IndexMetaDataExtractor indexMetaDataExtractor;
    private final String tableName;
    private SQLFieldMapper sqlFieldMapper;
    private boolean tableIsAlias = false;

    TableExecutionContext(String name, IndexMetaDataExtractor indexMetaDataExtractor,
                          SQLFieldMapper sqlFieldMapper, boolean tableIsAlias) {
        this.indexMetaDataExtractor = indexMetaDataExtractor;
        this.tableName = name;
        this.sqlFieldMapper = sqlFieldMapper;
        this.tableIsAlias = tableIsAlias;
    }


    protected Map<String, Object> mapping() {
        return indexMetaDataExtractor.getDefaultMappingMap();
    }

    @Override
    public SQLFieldMapper mapper() {
        return sqlFieldMapper;
    }

    /**
     *
     * @param name the name of the column
     * @param value the value to be mapped
     * @return the value converted to the proper type
     */
    public Object mappedValue(String name, Object value) throws ValidationException {
        if (sqlFieldMapper == null) {
            return value;
        }
        return sqlFieldMapper.convertToXContentValue(name, value);
    }

    /**
     * Returns the ``primary key`` column names defined at index creation under the ``_meta``
     * key. If not defined, return empty list.
     *
     * @return a list of primary key column names
     */
    @SuppressWarnings("unchecked")
    public List<String> primaryKeys() {
        return indexMetaDataExtractor.getPrimaryKeys();
    }

    /**
     * Returns the ``primary key`` column names defined at index creation under the ``_meta``
     * key. If none defined, add ``_id`` as primary key(Default).
     *
     * @return a list of primary key column names
     */
    public List<String> primaryKeysIncludingDefault() {
        List<String> primaryKeys = primaryKeys();
        if (primaryKeys.isEmpty()) {
            primaryKeys.add("_id"); // Default Primary Key (only for optimization, not for consistency checks)
        }
        return primaryKeys;
    }


    /**
     * returns all columns defined in the mapping as a sorted sequence
     * to be used in "*" selects.
     *
     * @return a sequence of column names
     */
    @SuppressWarnings("unchecked")
    public Iterable<String> allCols() {
        Set<String> res = new TreeSet<>();
        if (mapping().size() > 0 && mapping().containsKey("properties")) {
            for (String columnName : ((Map<String, Object>)mapping().get("properties")).keySet()) {
                // don't add internal or sub object field names
                if (columnName.startsWith("_") || columnName.contains(".")) {
                    continue;
                }

                res.add(columnName);
            }
        }
        return res;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean hasCol(String colName) {
        return indexMetaDataExtractor.getColumnDefinitionsMap().get(colName) != null;
    }

    /**
     * Check if given name is equal to defined routing name.
     *
     * @param name
     * @return
     */
    public Boolean isRouting(String name) {
        String routingPath = indexMetaDataExtractor.getRoutingColumn();
        if (routingPath == null) {
            // the primary key(s) values are saved under _id, so they are used as default
            // routing values
            if (primaryKeys().contains(name)) {
                return true;
            }
            routingPath = "_id";
        }
        return routingPath.equals(name);
    }

    public boolean tableIsAlias() {
        return tableIsAlias;
    }
}
