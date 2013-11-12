package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.cratedb.core.IndexMetaDataExtractor;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.io.IOException;

public class TablesTable extends AbstractInformationSchemaTable {

    private ImmutableMap<String, InformationSchemaColumn> fieldMapper = new ImmutableMap
            .Builder<String, InformationSchemaColumn>()
            .put(Columns.TABLE_NAME, new InformationSchemaStringColumn(Columns.TABLE_NAME))
            .put(Columns.NUMBER_OF_SHARDS, new InformationSchemaIntegerColumn(Columns.NUMBER_OF_SHARDS))
            .put(Columns.NUMBER_OF_REPLICAS, new InformationSchemaIntegerColumn(Columns.NUMBER_OF_REPLICAS))
            .put(Columns.ROUTING_COLUMN, new InformationSchemaStringColumn(Columns.ROUTING_COLUMN))
            .build();

    public static final String NAME = "tables";

    public class Columns {
        public static final String TABLE_NAME = "table_name";
        public static final String NUMBER_OF_SHARDS = "number_of_shards";
        public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
        public static final String ROUTING_COLUMN = "routing_column";
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
        StringField routingColumn = new StringField(Columns.ROUTING_COLUMN, "", Field.Store.YES);

        for (IndexMetaData metaData : clusterState.metaData().indices().values()) {
            IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
            Document doc = new Document();
            tableName.setStringValue(extractor.getIndexName());
            doc.add(tableName);

            numberOfShards.setIntValue(extractor.getNumberOfShards());
            doc.add(numberOfShards);

            numberOfReplicas.setIntValue(extractor.getNumberOfReplicas());
            doc.add(numberOfReplicas);

            // routing column

            String routingColumnName = extractor.getRoutingColumn();
            routingColumn.setStringValue(routingColumnName);
            doc.add(routingColumn);

            indexWriter.addDocument(doc);
        }

    }
}
