package org.cratedb.action.sql;

import com.google.common.base.Optional;
import org.cratedb.action.collect.*;
import org.cratedb.index.ColumnDefinition;
import org.cratedb.index.IndexMetaDataExtractor;
import org.cratedb.lucene.LuceneFieldMapper;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.ValidationException;
import org.cratedb.sql.parser.parser.NodeTypes;
import org.cratedb.sql.parser.parser.ValueNode;
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
    public final String tableName;
    private final Map<String, ColumnDefinition> columnDefinitions;
    private SQLFieldMapper sqlFieldMapper;
    private boolean tableIsAlias = false;

    public TableExecutionContext(String name, IndexMetaDataExtractor indexMetaDataExtractor,
                          SQLFieldMapper sqlFieldMapper, boolean tableIsAlias) {
        this.indexMetaDataExtractor = indexMetaDataExtractor;
        this.tableName = name;
        this.sqlFieldMapper = sqlFieldMapper;
        this.tableIsAlias = tableIsAlias;
        this.columnDefinitions = indexMetaDataExtractor.getColumnDefinitionsMap();
    }


    protected Map<String, Object> mapping() {
        return indexMetaDataExtractor.getDefaultMappingMap();
    }

    @Override
    public SQLFieldMapper mapper() {
        return sqlFieldMapper;
    }

    @Override
    public LuceneFieldMapper luceneFieldMapper() {
        throw new UnsupportedOperationException("Generic table currently has no " +
                "LuceneFieldMapper");
    }

    public boolean isMultiValued(String columnName) {
        return columnDefinitions.get(columnName) != null
            && columnDefinitions.get(columnName).isMultiValued();
    }

    public Optional<ColumnDefinition> getColumnDefinition(String columnName) {
        return Optional.fromNullable(columnDefinitions.get(columnName));
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
        return sqlFieldMapper.mappedValue(name, value);
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
        return getColumnDefinition(colName).isPresent();
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

    @Override
    public Expression getCollectorExpression(ValueNode node) {
        if (node.getNodeType()!=NodeTypes.COLUMN_REFERENCE &&
                node.getNodeType() != NodeTypes.NESTED_COLUMN_REFERENCE){
            return null;
        }

        Optional<ColumnDefinition> optColumnDefinition = getColumnDefinition(node.getColumnName());
        if (!optColumnDefinition.isPresent()) {
            throw new SQLParseException(String.format("Unknown column '%s'", node.getColumnName()));
        }
        ColumnDefinition columnDefinition = optColumnDefinition.get();
        switch (columnDefinition.dataType) {
            case STRING:
                return new BytesRefColumnReference(columnDefinition.columnName);
            case DOUBLE:
                return new DoubleColumnReference(columnDefinition.columnName);
            case BOOLEAN:
                return new BooleanColumnReference(columnDefinition.columnName);
            case CRATY:
                return new CratyColumnReference(columnDefinition.columnName);
            case FLOAT:
                return new FloatColumnReference(columnDefinition.columnName);
            case SHORT:
                return new ShortColumnReference(columnDefinition.columnName);
            case LONG:
            case TIMESTAMP:
                return new LongColumnReference(columnDefinition.columnName);
            case INTEGER:
                return new IntegerColumnReference(columnDefinition.columnName);
            default:
                throw new SQLParseException(
                        String.format("Invalid column reference type '%s'",
                                columnDefinition.dataType));
        }
    }
}



