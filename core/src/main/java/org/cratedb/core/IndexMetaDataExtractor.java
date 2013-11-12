package org.cratedb.core;

import com.google.common.base.Joiner;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class to extract frequently needed attributes from IndexMetaData
 */
public class IndexMetaDataExtractor {

    public static class ColumnDefinition {
        public final String tableName;
        public final String columnName;
        public final String dataType;
        public final int ordinalPosition;

        public ColumnDefinition(String tableName, String columnName, String dataType,
                                int ordinalPosition) {
            this.tableName = tableName;
            this.columnName = columnName;
            this.dataType = dataType;
            this.ordinalPosition = ordinalPosition;
        }
    }

    public static class Index {
        public final String tableName;
        public final String indexName;
        public final String columnName;
        public final String method;
        public final List<String> expressions;
        public final Map<String, String> properties;

        protected Index(String tableName, String indexName, String columnName, String method,
                     List<String> expressions, Map<String, String> properties) {
            this.tableName = tableName;
            this.indexName = indexName;
            this.columnName = columnName;
            this.method = method;
            this.expressions = expressions;
            this.properties = properties;
        }

        /**
         * returns the expressions over which the index is defined as a comma-separated string
         * @return the index-expressions as a comma-separated string
         */
        public String getExpressionsString() {
            return Joiner.on(", ").join(expressions);
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
         * @param indexExpressions an empty list that holds state and gets filled while calling
         *                         this function. Use one map for iterating over all columns of
         *                         a table.
         * @return and Index instance or null if this column is not indexed
         */
        public static Index create(String tableName, String indexName,
                                   String columnName, Map<String, Object> columnProperties,
                                   Map<String, List<String>> indexExpressions) {
            String method;
            Map<String, String> properties = new HashMap<>();
            List<String> expressions = new ArrayList<>();

            String index = (String)columnProperties.get("index");
            String analyzer = (String)columnProperties.get("analyzer");
            if (index != null && index.equals("no")) {
                return null;
            } else if ((index == null && analyzer == null)
                    || (index != null && index.equals("not_analyzed"))) {
                method = "plain";
            } else {
                method = "fulltext";
            }

            if (analyzer != null) {
                properties.put("analyzer", analyzer);
            }
            if (indexExpressions.containsKey(indexName)) {
                expressions = indexExpressions.get(indexName);
            } else {
                indexExpressions.put(indexName, expressions);
            }
            expressions.add(columnName);
            return new Index(tableName, indexName, columnName, method, expressions, properties);
        }

        /**
         * returns a unique identifier for this index
         */
        public String getUid() {
            return String.format("%s.%s", tableName, indexName);
        }
    }

    private final IndexMetaData metaData;
    private MappingMetaData defaultMappingMetaData = null;

    public IndexMetaDataExtractor(IndexMetaData metaData) {
        this.metaData = metaData;
        this.defaultMappingMetaData = this.metaData.getMappings()
                .get(Constants.DEFAULT_MAPPING_TYPE);
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

    public MappingMetaData getDefaultMappingMetaData() throws IOException {
        return this.defaultMappingMetaData;
    }

    public String getPrimaryKey() throws IOException {
        String primaryKeyColumn = null;
        if (hasDefaultMapping()) {
            Map<String, Object> mappingsMap = getDefaultMappingMetaData().sourceAsMap();
            if (mappingsMap != null) {
                // if primary key has been set, use this
                @SuppressWarnings("unchecked")
                Map<String, Object> metaMap = (Map<String, Object>)mappingsMap.get("_meta");
                if (metaMap != null) {
                    primaryKeyColumn = (String)metaMap.get("primary_keys");
                }
            }
        }
        return primaryKeyColumn;
    }

    public String getRoutingColumn() throws IOException {
        String routingColumnName = null;
        if (hasDefaultMapping()) {
            MappingMetaData mappingMetaData = getDefaultMappingMetaData();
            if (mappingMetaData != null) {
                if (mappingMetaData.routing().hasPath()) {
                    routingColumnName = mappingMetaData.routing().path();
                }
                if (routingColumnName == null) {
                    routingColumnName = getPrimaryKey();
                }
            }
        }
        return routingColumnName == null ? "_id" : routingColumnName;
    }

    /**
     * extract dataType from given columnProperties
     *
     * @param columnProperties map of String to Object containing column properties
     * @return dataType of the column with columnProperties as String
     */
    private String getColumnDataType(Map<String, Object> columnProperties) {
        String dataType = (String)columnProperties.get("type");
        if (dataType == null) {
            // TODO: whats about nested object schema?
            if (columnProperties.get("properties") != null) {
                // ``object`` type detected, but we call it ``craty``
                dataType = "craty";
            }

        } else if (dataType.equals("date")) {
            dataType = "timestamp";
        }
        return dataType;
    }

    /**
     * return a list of ColumnDefinitions for the table defined as "default"-mapping in this index
     *
     * @return a list of ColumnDefinitions
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public List<ColumnDefinition> getColumnDefinitions() throws IOException {
        String indexName = getIndexName();

        List<ColumnDefinition> columns = new ArrayList<>();
        if (hasDefaultMapping()) {
            Map<String, Object> propertiesMap = (Map<String, Object>)getDefaultMappingMetaData()
                    .sourceAsMap().get("properties");

            int pos = 1;
            if (propertiesMap != null) {
                for (Map.Entry<String, Object> columnEntry: propertiesMap.entrySet()) {
                    Map<String, Object> columnProperties = (Map)columnEntry.getValue();
                    if (columnProperties.get("type") != null
                            && columnProperties.get("type").equals("multi_field")) {
                        for (Map.Entry<String, Object> multiColumnEntry:
                                ((Map<String, Object>)columnProperties.get("fields")).entrySet()) {
                            Map<String, Object> multiColumnProperties = (Map)multiColumnEntry.getValue();

                            if (multiColumnEntry.getKey().equals(columnEntry.getKey())) {

                                columns.add(
                                        new ColumnDefinition(
                                                indexName,
                                                multiColumnEntry.getKey(),
                                                getColumnDataType(multiColumnProperties),
                                                pos++
                                        )
                                );
                            }
                        }
                    } else {
                        columns.add(
                                new ColumnDefinition(
                                        indexName,
                                        columnEntry.getKey(),
                                        getColumnDataType(columnProperties),
                                        pos++
                                )
                        );
                    }
                }
            }

        }
        return columns;
    }

    @SuppressWarnings("unchecked")
    public List<Index> getIndices() throws IOException {
        List<Index> indices = new ArrayList<>();

        if (hasDefaultMapping()) {
            String tableName = getIndexName();
            Map<String, List<String>> indicesExpressions = new HashMap<>();

            Map<String, Object> propertiesMap = (Map<String, Object>)getDefaultMappingMetaData()
                    .sourceAsMap().get("properties");
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
                                    indicesExpressions);
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
                                indicesExpressions);
                        if (idx != null) {
                            indices.add(idx);
                        }
                    }
                }
            }
        }
        return indices;
    }
}
