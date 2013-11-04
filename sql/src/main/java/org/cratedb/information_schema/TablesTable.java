package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class TablesTable extends AbstractClusterStateBackedInformationSchemaTable {

    private Map<String, InformationSchemaColumn> fieldMapper = new LinkedHashMap<>();

    public static final String NAME = "tables";

    public class Columns {
        public static final String TABLE_NAME = "table_name";
        public static final String NUMBER_OF_SHARDS = "number_of_shards";
        public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
    }

    public TablesTable() {
        super();
        fieldMapper.put(
            Columns.TABLE_NAME,
            new InformationSchemaStringColumn(Columns.TABLE_NAME)
        );

        fieldMapper.put(
            Columns.NUMBER_OF_SHARDS,
            new InformationSchemaIntegerColumn(Columns.NUMBER_OF_SHARDS)
        );

        fieldMapper.put(
            Columns.NUMBER_OF_REPLICAS,
            new InformationSchemaIntegerColumn(Columns.NUMBER_OF_REPLICAS)
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

        // according to http://wiki.apache.org/lucene-java/ImproveIndexingSpeed
        // re-using fields is faster than re-creating the field inside the loop
        StringField tableName = new StringField(TablesTable.Columns.TABLE_NAME, "", Field.Store.YES);
        IntField numberOfShards = new IntField(TablesTable.Columns.NUMBER_OF_SHARDS, 0, Field.Store.YES);
        IntField numberOfReplicas = new IntField(TablesTable.Columns.NUMBER_OF_REPLICAS, 0, Field.Store.YES);

        for (IndexMetaData metaData : clusterState.metaData().indices().values()) {
            Document doc = new Document();
            tableName.setStringValue(metaData.getIndex());
            doc.add(tableName);

            numberOfShards.setIntValue(metaData.getNumberOfShards());
            doc.add(numberOfShards);

            numberOfReplicas.setIntValue(metaData.getNumberOfReplicas());
            doc.add(numberOfReplicas);

            indexWriter.addDocument(doc);
        }

    }
}
