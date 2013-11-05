package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.cratedb.action.sql.NodeExecutionContext;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * virtual information_schema table listing table columns definition
 */
public class TableColumnsTable extends AbstractInformationSchemaTable {

    private Map<String, InformationSchemaColumn> fieldMapper = new LinkedHashMap<>();

    public static final String NAME = "columns";

    public class Columns {
        public static final String TABLE_NAME = "table_name";
        public static final String COLUMN_NAME = "column_name";
        public static final String ORDINAL_POSITION = "ordinal_position";
        public static final String DATA_TYPE = "data_type";
    }

    StringField tableNameField = new StringField(Columns.TABLE_NAME, "", Field.Store.YES);
    StringField columnNameField = new StringField(Columns.COLUMN_NAME, "", Field.Store.YES);
    IntField ordinalPositionField = new IntField(Columns.ORDINAL_POSITION, 0, Field.Store.YES);
    StringField dataTypeField = new StringField(Columns.DATA_TYPE, "", Field.Store.YES);

    public TableColumnsTable() {
        fieldMapper.put(
                Columns.TABLE_NAME,
                new InformationSchemaStringColumn(Columns.TABLE_NAME)
        );
        fieldMapper.put(
                Columns.COLUMN_NAME,
                new InformationSchemaStringColumn(Columns.COLUMN_NAME)
        );
        fieldMapper.put(
                Columns.ORDINAL_POSITION,
                new InformationSchemaIntegerColumn(Columns.ORDINAL_POSITION)
        );
        fieldMapper.put(
                Columns.DATA_TYPE,
                new InformationSchemaStringColumn(Columns.DATA_TYPE)
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

            MappingMetaData mappingMetaData = indexMetaData.getMappings().get(NodeExecutionContext.DEFAULT_TYPE);
            if (mappingMetaData != null) {
                Map<String, Object> mappingProperties = (Map)mappingMetaData.sourceAsMap()
                                                                        .get("properties");
                int pos = 1;
                for (ImmutableMap.Entry<String, Object> columnEntry: mappingProperties.entrySet()) {
                    Map<String, Object> columnProperties = (Map)columnEntry.getValue();

                    if (columnProperties.get("type") != null
                            && columnProperties.get("type").equals("multi_field")) {
                        for (ImmutableMap.Entry<String, Object> multiColumnEntry:
                                ((Map<String, Object>)columnProperties.get("fields")).entrySet()) {
                            Map<String, Object> multiColumnProperties = (Map)multiColumnEntry.getValue();

                            if (multiColumnEntry.getKey().equals(columnEntry.getKey())) {
                                addColumnDocument(
                                        indexMetaData.getIndex(),
                                        multiColumnEntry.getKey(),
                                        pos,
                                        multiColumnProperties
                                        );
                                pos++;
                            }
                        }
                    } else {
                        addColumnDocument(
                                indexMetaData.getIndex(),
                                columnEntry.getKey(),
                                pos,
                                columnProperties
                                );
                        pos++;
                    }
                }
            }

        }
    }

    private void addColumnDocument(String tableName, String columnName, int ordinalPosition,
                                   Map<String, Object>columnProperties) throws IOException {
        String dataType = (String)columnProperties.get("type");
        if (dataType == null && columnProperties.get("properties") != null) {
            // TODO: whats about nested object schema?
            dataType = "craty"; // ``object`` type detected, but we call it ``craty``
        }
        if (dataType.equals("date")) {
            dataType = "timestamp";
        }

        Document doc = new Document();

        tableNameField.setStringValue(tableName);
        doc.add(tableNameField);

        columnNameField.setStringValue(columnName);
        doc.add(columnNameField);

        ordinalPositionField.setIntValue(ordinalPosition);
        doc.add(ordinalPositionField);

        dataTypeField.setStringValue(dataType);
        doc.add(dataTypeField);

        indexWriter.addDocument(doc);
    }
}
