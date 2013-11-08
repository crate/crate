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
import java.util.Map;

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
            Document doc = new Document();
            tableName.setStringValue(metaData.getIndex());
            doc.add(tableName);

            numberOfShards.setIntValue(metaData.getNumberOfShards());
            doc.add(numberOfShards);

            numberOfReplicas.setIntValue(metaData.getNumberOfReplicas());
            doc.add(numberOfReplicas);

            // routing column

            String routingColumnName = null;
            MappingMetaData mappingMetaData= metaData.getMappings()
                    .get(NodeExecutionContext.DEFAULT_TYPE);
            if (mappingMetaData != null) {
                if (mappingMetaData.routing().hasPath()) {
                    routingColumnName = mappingMetaData.routing().path();
                }
                if (routingColumnName == null) {

                    Map<String, Object> mappingsMap = mappingMetaData.sourceAsMap();
                    if (mappingsMap != null) {
                        // if primary key has been set, use this
                        @SuppressWarnings("unchecked")
                        Map<String, Object> metaMap = (Map<String, Object>)mappingsMap.get("_meta");
                        if (metaMap != null) {
                            String primaryKeyColumn = (String)metaMap.get("primary_keys");
                            if (primaryKeyColumn != null) {
                                routingColumnName = primaryKeyColumn;
                            }
                        }
                    }
                }
            }
            // default routing key is _id if none has been set
            routingColumn.setStringValue(routingColumnName == null ? "_id" : routingColumnName);
            doc.add(routingColumn);

            indexWriter.addDocument(doc);
        }

    }
}
