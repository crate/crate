package org.cratedb.information_schema;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.cratedb.action.sql.NodeExecutionContext;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;

import java.io.IOException;
import java.util.*;

/**
 * virtual information_schema table listing table index definitions
 */
public class IndicesTable extends AbstractInformationSchemaTable {

    private Map<String, InformationSchemaColumn> fieldMapper = new LinkedHashMap<>();

    public static final String NAME = "indices";

    private Map<String, List> indicesExpressions;

    public class Columns {
        public static final String TABLE_NAME = "table_name";
        public static final String INDEX_NAME = "index_name";
        public static final String METHOD = "method";
        public static final String EXPRESSIONS = "expressions";
        public static final String PROPERTIES = "properties";
    }

    StringField tableNameField = new StringField(Columns.TABLE_NAME, "", Field.Store.YES);
    StringField indexNameField = new StringField(Columns.INDEX_NAME, "", Field.Store.YES);
    StringField methodField = new StringField(Columns.METHOD, "", Field.Store.YES);
    StringField expressionsField = new StringField(Columns.EXPRESSIONS, "", Field.Store.YES);
    StringField propertiesField = new StringField(Columns.PROPERTIES, "", Field.Store.YES);
    // only internal used
    StringField uidField = new StringField("uid", "", Field.Store.YES);

    public IndicesTable() {
        fieldMapper.put(
                Columns.TABLE_NAME,
                new InformationSchemaStringColumn(Columns.TABLE_NAME)
        );
        fieldMapper.put(
                Columns.INDEX_NAME,
                new InformationSchemaStringColumn(Columns.INDEX_NAME)
        );
        fieldMapper.put(
                Columns.METHOD,
                new InformationSchemaStringColumn(Columns.METHOD)
        );
        fieldMapper.put(
                Columns.EXPRESSIONS,
                new InformationSchemaStringColumn(Columns.EXPRESSIONS)
        );
        fieldMapper.put(
                Columns.PROPERTIES,
                new InformationSchemaStringColumn(Columns.PROPERTIES)
        );
    }

    @Override
    public Iterable<String> cols() {
        return fieldMapper.keySet();
    }

    @Override
    public ImmutableMap<String, InformationSchemaColumn> fieldMapper() {
        return ImmutableMap.copyOf(fieldMapper);
    }

    @Override
    public void doIndex(ClusterState clusterState) throws IOException {

        for (IndexMetaData indexMetaData : clusterState.metaData().indices().values()) {
            indicesExpressions = new HashMap<>();

            MappingMetaData mappingMetaData = indexMetaData.getMappings().get(NodeExecutionContext.DEFAULT_TYPE);
            if (mappingMetaData != null) {
                Map<String, Object> mappingProperties = (Map)mappingMetaData.sourceAsMap()
                                                                        .get("properties");
                for (ImmutableMap.Entry<String, Object> columnEntry: mappingProperties.entrySet()) {
                    Map<String, Object> columnProperties = (Map)columnEntry.getValue();

                    if (columnProperties.get("type") != null
                            && columnProperties.get("type").equals("multi_field")) {
                        for (ImmutableMap.Entry<String, Object> multiColumnEntry:
                                ((Map<String, Object>)columnProperties.get("fields")).entrySet()) {
                            Map<String, Object> multiColumnProperties = (Map)multiColumnEntry.getValue();

                            addIndexDocument(
                                    indexMetaData.getIndex(),
                                    multiColumnEntry.getKey(),
                                    columnEntry.getKey(),
                                    multiColumnProperties
                            );
                        }
                    } else {
                        addIndexDocument(
                                indexMetaData.getIndex(),
                                columnEntry.getKey(),
                                columnEntry.getKey(),
                                columnProperties
                        );
                    }
                }
            }

        }
    }

    private void addIndexDocument(String tableName, String indexName,
                                  String columnName,
                                  Map<String, Object> columnProperties) throws IOException {
        String method;
        Map<String, String> properties = new HashMap<>();
        List<String> expressions = new ArrayList<>();

        String index = (String)columnProperties.get("index");
        String analyzer = (String)columnProperties.get("analyzer");
        if (index != null && index.equals("no")) {
            return;
        } else if ((index == null && analyzer == null)
                || (index != null && index.equals("not_analyzed"))) {
            method = "plain";
        } else {
            method = "fulltext";
        }

        if (analyzer != null) {
            properties.put("analyzer", analyzer);
        }

        if (indicesExpressions.containsKey(indexName)) {
            expressions = indicesExpressions.get(indexName);
        } else {
            indicesExpressions.put(indexName, expressions);
        }
        expressions.add(columnName);

        Document doc = new Document();

        tableNameField.setStringValue(tableName);
        doc.add(tableNameField);

        indexNameField.setStringValue(indexName);
        doc.add(indexNameField);

        methodField.setStringValue(method);
        doc.add(methodField);

        expressionsField.setStringValue(Joiner.on(", ").join(expressions));
        doc.add(expressionsField);

        Joiner.MapJoiner joiner = Joiner.on(", ").withKeyValueSeparator("=");
        propertiesField.setStringValue(joiner.join(properties));
        doc.add(propertiesField);

        String uid = tableName + "." + indexName;
        uidField.setStringValue(uid);
        doc.add(uidField);

        indexWriter.updateDocument(new Term("uid", uid), doc);
    }
}
