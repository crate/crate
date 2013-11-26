package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.index.ColumnDefinition;
import org.cratedb.index.IndexMetaDataExtractor;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * virtual information_schema table listing table columns definition
 */
public class ColumnsTable extends AbstractInformationSchemaTable {

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

    @Inject
    public ColumnsTable(Map<String, AggFunction> aggFunctionMap) {
        super(aggFunctionMap);
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
            IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(indexMetaData);

            for (ColumnDefinition columnDefinition : extractor.getColumnDefinitions()) {
                addColumnDocument(columnDefinition);
            }
        }
    }

    private void addColumnDocument(ColumnDefinition columnDefinition) throws IOException {
        Document doc = new Document();
        tableNameField.setStringValue(columnDefinition.tableName);
        doc.add(tableNameField);

        columnNameField.setStringValue(columnDefinition.columnName);
        doc.add(columnNameField);

        ordinalPositionField.setIntValue(columnDefinition.ordinalPosition);
        doc.add(ordinalPositionField);

        dataTypeField.setStringValue(columnDefinition.dataType.getName());
        doc.add(dataTypeField);

        indexWriter.addDocument(doc);
    }
}
