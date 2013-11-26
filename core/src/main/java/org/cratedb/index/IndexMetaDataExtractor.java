package org.cratedb.index;

import com.google.common.base.Joiner;
import org.cratedb.Constants;
import org.cratedb.DataType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.collect.Tuple;

import java.io.IOException;
import java.util.*;

/**
 * Helper class to extract frequently needed attributes from IndexMetaData
 */
public class IndexMetaDataExtractor {

    public static class Index {
        public final String tableName;
        public final String indexName;
        public final String columnName;
        public final String method;
        public final List<String> columns;
        public final Map<String, String> properties;

        protected Index(String tableName, String indexName, String columnName, String method,
                     List<String> columns, Map<String, String> properties) {
            this.tableName = tableName;
            this.indexName = indexName;
            this.columnName = columnName;
            this.method = method;
            this.columns = columns;
            this.properties = properties;
        }

        /**
         * returns the expressions over which the index is defined as a comma-separated string
         * @return the index-expressions as a comma-separated string
         */
        public String getColumnsString() {
            return Joiner.on(", ").join(columns);
        }

        /**
         * Returns the properties the index was defined with as a comma-separated string of
         * key-value pairs.
         *
         * E.g. used in information_schema.indices to display the index' properties
         *
         * @return
         */
        public String getPropertiesString() {
            return Joiner.on(", ").withKeyValueSeparator("=").join(properties);
        }

        /**
         * Create an Index from a columnDefinition and further informations.
         *
         * @param tableName the name of the table this index is defined in
         * @param indexName the name of the index
         * @param columnName the name of the column this index indexes (there can be more than
         *                   one index for a column)
         * @param columnProperties the properties of the column this index indexes -
         *                         index attributes are extracted from here
         * @param indexColumns an empty list that holds state and gets filled while calling
         *                         this function. Use one map for iterating over all columns of
         *                         a table.
         * @return and Index instance or null if this column is not indexed
         */
        public static Index create(String tableName, String indexName,
                                   String columnName, Map<String, Object> columnProperties,
                                   Map<String, List<String>> indexColumns) {
            Map<String, String> properties = new HashMap<>();
            List<String> columns = new ArrayList<>();

            Tuple<String, String> analyzerTuple = getAnalyzer(columnProperties);
            if (analyzerTuple.v1() == null && analyzerTuple.v2() == null) {
                return null;
            }
            String analyzer = analyzerTuple.v1();
            String method = analyzerTuple.v2();

            if (analyzer != null) {
                properties.put("analyzer", analyzer);
            }
            if (indexColumns.containsKey(indexName)) {
                columns = indexColumns.get(indexName);
            } else {
                indexColumns.put(indexName, columns);
            }
            columns.add(columnName);
            return new Index(tableName, indexName, columnName, method, columns, properties);
        }

        /**
         * returns a unique identifier for this index
         */
        public String getUid() {
            return String.format("%s.%s", tableName, indexName);
        }
    }

    private final IndexMetaData metaData;
    private final MappingMetaData defaultMappingMetaData;
    private Map<String, Object> defaultMappingMap = new HashMap<>();

    /**
     * TODO: cache built information
     * @param metaData
     */
    public IndexMetaDataExtractor(IndexMetaData metaData) {
        this.metaData = metaData;
        this.defaultMappingMetaData = this.metaData.mappingOrDefault(Constants.DEFAULT_MAPPING_TYPE);
        if (this.defaultMappingMetaData != null) {
            try {
                this.defaultMappingMap = this.defaultMappingMetaData.sourceAsMap();
            } catch (IOException e) {
                this.defaultMappingMap = new HashMap<>();
            }
        }
    }

    public String getIndexName() {
        return this.metaData.getIndex();
    }

    public int getNumberOfShards() {
        return this.metaData.getNumberOfShards();
    }

    public int getNumberOfReplicas() {
        return this.metaData.getNumberOfReplicas();
    }

    public boolean hasDefaultMapping() {
        return this.defaultMappingMetaData != null;
    }

    public MappingMetaData getDefaultMappingMetaData() {
        return this.defaultMappingMetaData;
    }

    public Map<String, Object> getDefaultMappingMap() {
        return this.defaultMappingMap;
    }

    public List<String> getPrimaryKeys() {
        List<String> primaryKeys = new ArrayList<>(1);
        if (hasDefaultMapping()) {
            Map<String, Object> mappingsMap = getDefaultMappingMap();
            if (mappingsMap != null) {
                // if primary key has been set, use this
                @SuppressWarnings("unchecked")
                Map<String, Object> metaMap = (Map<String, Object>)mappingsMap.get("_meta");
                if (metaMap != null) {
                    primaryKeys.add((String)metaMap.get("primary_keys"));
                }
            }
        }
        return primaryKeys;
    }

    public String getRoutingColumn() {
        String routingColumnName = null;
        if (hasDefaultMapping()) {
            MappingMetaData mappingMetaData = getDefaultMappingMetaData();
            if (mappingMetaData != null) {
                if (mappingMetaData.routing().hasPath()) {
                    routingColumnName = mappingMetaData.routing().path();
                }
                if (routingColumnName == null) {
                    try {
                        routingColumnName = getPrimaryKeys().get(0);
                    } catch(IndexOutOfBoundsException e) {
                        // ignore
                    }
                }
            }
        }
        return routingColumnName == null ? "_id" : routingColumnName;
    }

    /**
     * extract dataType from given columnProperties
     *
     *
     * @param columnProperties map of String to Object containing column properties
     * @return dataType of the column with columnProperties
     */
    private DataType getColumnDataType(Map<String, Object> columnProperties) {
        String typeName = (String)columnProperties.get("type");
        if (typeName == null) {
            if (columnProperties.get("properties") != null) {
                return DataType.CRATY;
            }
        } else {
            switch(typeName.toLowerCase()) {
                case "date":
                    return DataType.TIMESTAMP;
                case "string":
                    return DataType.STRING;
                case "boolean":
                    return DataType.BOOLEAN;
                case "byte":
                    return DataType.BYTE;
                case "short":
                    return DataType.SHORT;
                case "integer":
                    return DataType.INTEGER;
                case "long":
                    return DataType.LONG;
                case "float":
                    return DataType.FLOAT;
                case "double":
                    return DataType.DOUBLE;
                case "ip":
                    return DataType.IP;
                case "object":
                case "nested":
                    return DataType.CRATY;
                }
        }
        return null;
    }

    private static Tuple<String, String> getAnalyzer(Map<String, Object> columnProperties) {
        String index = (String)columnProperties.get("index");
        String analyzer = (String)columnProperties.get("analyzer");
        String method;

        if (index != null && index.equals("no")) {
            return new Tuple<String, String>(null, null);
        } else if ((index == null && analyzer == null)
            || (index != null && index.equals("not_analyzed"))) {
            method = "plain";
        } else {
            method = "fulltext";
        }
        return new Tuple<>(analyzer, method);
    }

    private String getColumnName(String prefix, String columnName) {
        if (prefix == null) { return columnName; }
        return String.format("%s.%s", prefix, columnName);
    }

    /**
     * return a list of ColumnDefinitions for the table defined as "default"-mapping in this index
     *
     * @return a list of ColumnDefinitions
     */

    public List<ColumnDefinition> getColumnDefinitions() {
        String indexName = getIndexName();

        List<ColumnDefinition> columns = new ArrayList<>();
        if (hasDefaultMapping()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> propertiesMap = (Map<String, Object>)getDefaultMappingMap()
                    .get("properties");

            if (propertiesMap != null) {
                internalExtractColumnDefinitions(
                        indexName,
                        null,
                        columns,
                        propertiesMap,
                        1
                );
            }

        }
        return columns;
    }

    /**
     * Extract ColumnDefinitions from the properties of a root-object or object mapping.
     * Collect the results into the columns list parameter and proceed recursively if necessary.
     *
     * @param indexName the name of the index to which the mapping-properties belong
     * @param prefix the prefix to prefix the extracted column, if null,
     *               use the column names from the mapping.
     * @param columns the result parameter, collecting the nested ColumnDefinitions
     * @param propertiesMap the properties to extract the columnDefinitions from
     * @param startPos the position of the next field in the current table/index
     * @return the position of the next field to come
     */
    @SuppressWarnings("unchecked")
    private int internalExtractColumnDefinitions( String indexName,
                                                  String prefix,
                                                  List<ColumnDefinition> columns,
                                                  Map<String, Object> propertiesMap,
                                                  int startPos) {
        for (Map.Entry<String, Object> columnEntry: propertiesMap.entrySet()) {
            Map<String, Object> columnProperties = (Map)columnEntry.getValue();
            if (columnProperties.get("type") != null
                    && columnProperties.get("type").equals("multi_field")) {
                for (Map.Entry<String, Object> multiColumnEntry:
                        ((Map<String, Object>)columnProperties.get("fields")).entrySet()) {
                    Map<String, Object> multiColumnProperties = (Map)multiColumnEntry.getValue();

                    if (multiColumnEntry.getKey().equals(columnEntry.getKey())) {

                        String columnName = getColumnName(prefix, columnEntry.getKey());
                        columns.add(
                            new ColumnDefinition(
                                indexName,
                                columnName,
                                getColumnDataType(multiColumnProperties),
                                getAnalyzer(multiColumnProperties).v2(),
                                startPos++,
                                false,
                                true
                            )
                        );
                    }
                }
            } else if ((columnProperties.get("type") == null
                    &&
                    columnProperties.containsKey("properties"))
                    ||
                    columnProperties.get("type").equals("object")
                    ||
                    columnProperties.get("type").equals("nested")) {
                boolean strict = columnProperties.get("dynamic") != null
                        && columnProperties.get("dynamic").equals("strict");
                boolean dynamic = columnProperties.get("dynamic") == null ||
                        (!strict &&
                        !columnProperties.get("dynamic").equals(false) &&
                        !Booleans.isExplicitFalse((String)columnProperties.get("dynamic")));

                String objectColumnName = getColumnName(prefix, columnEntry.getKey());
                // add object column before child columns
                ObjectColumnDefinition objectColumnDefinition = new ObjectColumnDefinition(
                    indexName,
                    objectColumnName,
                    getColumnDataType(columnProperties),
                    getAnalyzer(columnProperties).v2(),
                    startPos++,
                    dynamic,
                    strict
                );
                if (columnProperties.get("properties") != null) {

                    // extract nested columns from object into flat list with dotted notation
                    startPos = internalExtractColumnDefinitions(
                            indexName,
                            objectColumnName,
                            objectColumnDefinition.nestedColumns,
                            (Map<String, Object>)columnProperties.get("properties"),
                            startPos
                    );
                }
                columns.add(objectColumnDefinition);
                columns.addAll(objectColumnDefinition.nestedColumns);

            } else {
                String columnName = getColumnName(prefix, columnEntry.getKey());
                columns.add(
                    new ColumnDefinition(
                        indexName,
                        columnName,
                        getColumnDataType(columnProperties),
                        getAnalyzer(columnProperties).v2(),
                        startPos++,
                        false,
                        true
                    )
                );
            }
        }
        return startPos;
    }

    public Map<String, ColumnDefinition> getColumnDefinitionsMap() {
        Map<String, ColumnDefinition> map = new HashMap<>();
        for (ColumnDefinition columnDefinition : this.getColumnDefinitions()) {
            map.put(columnDefinition.columnName, columnDefinition);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    public List<Index> getIndices() {
        List<Index> indices = new ArrayList<>();

        if (hasDefaultMapping()) {
            String tableName = getIndexName();
            Map<String, List<String>> indicesColumns = new HashMap<>();

            Map<String, Object> propertiesMap = (Map<String, Object>)getDefaultMappingMap().get
                    ("properties");
            if (propertiesMap != null) {
                for (Map.Entry<String, Object> columnEntry: propertiesMap.entrySet()) {
                    Map<String, Object> columnProperties = (Map)columnEntry.getValue();

                    if (columnProperties.get("type") != null
                            && columnProperties.get("type").equals("multi_field")) {
                        for (Map.Entry<String, Object> multiColumnEntry:
                                ((Map<String, Object>)columnProperties.get("fields")).entrySet()) {
                            Map<String, Object> multiColumnProperties = (Map)multiColumnEntry.getValue();

                            Index idx = Index.create(tableName,
                                    multiColumnEntry.getKey(),
                                    columnEntry.getKey(),
                                    multiColumnProperties,
                                    indicesColumns);
                            if (idx != null) {
                                indices.add(idx);
                            }

                        }
                    } else {
                        Index idx = Index.create(
                                tableName,
                                columnEntry.getKey(),
                                columnEntry.getKey(),
                                columnProperties,
                                indicesColumns);
                        if (idx != null) {
                            indices.add(idx);
                        }
                    }
                }
            }
        }
        return indices;
    }

    /**
     * Returns true if this table is dynamic, so new columns can be added.
     * if false, only
     */
    public boolean isDynamic() {
        boolean dynamic = metaData.settings().getAsBoolean("index.mapper.dynamic", true);
        if (dynamic && hasDefaultMapping()) {
            Object dynamicProperty = getDefaultMappingMap().get("dynamic");
            if (dynamicProperty != null
                    &&
                ((dynamicProperty instanceof String) && (dynamicProperty.equals("strict") || Booleans.isExplicitFalse((String)dynamicProperty))
                    ||
                ((dynamicProperty instanceof Boolean) && dynamicProperty == Boolean.FALSE))) {
                dynamic = false;
            }

        }
        return dynamic;
    }
}
