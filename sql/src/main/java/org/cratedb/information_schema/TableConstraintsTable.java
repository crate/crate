package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.cratedb.action.sql.NodeExecutionContext;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * virtual information_schema table listing table constraints like primary_key constraints
 */
public class TableConstraintsTable extends AbstractClusterStateBackedInformationSchemaTable {

    private Map<String, InformationSchemaColumn> fieldMapper = new LinkedHashMap<>();

    public static final String NAME = "table_constraints";

    public class Columns {
        public static final String TABLE_NAME = "table_name";
        public static final String CONSTRAINT_NAME = "constraint_name";
        public static final String CONSTRAINT_TYPE = "constraint_type";
    }

    public class ConstraintType {
        public static final String PRIMARY_KEY = "PRIMARY_KEY";
        // UNIQUE, CHECK, FOREIGN KEY etc.
    }

    public TableConstraintsTable() {
        fieldMapper.put(
                Columns.TABLE_NAME,
                new InformationSchemaStringColumn(Columns.TABLE_NAME)
        );
        fieldMapper.put(
                Columns.CONSTRAINT_NAME,
                new InformationSchemaStringColumn(Columns.CONSTRAINT_NAME)
        );
        fieldMapper.put(
                Columns.CONSTRAINT_TYPE,
                new InformationSchemaStringColumn(Columns.CONSTRAINT_TYPE)
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
        StringField tableName = new StringField(Columns.TABLE_NAME, "", Field.Store.YES);
        StringField constraintName = new StringField(Columns.CONSTRAINT_NAME, "", Field.Store.YES);
        StringField constraintType = new StringField(Columns.CONSTRAINT_TYPE, "", Field.Store.YES);

        for (IndexMetaData indexMetaData : clusterState.metaData().indices().values()) {


            MappingMetaData mappingMetaData = indexMetaData.getMappings()
                    .get(NodeExecutionContext.DEFAULT_TYPE);
            if (mappingMetaData != null) {
                Map<String, Object> metaMap = (Map<String, Object>)mappingMetaData.sourceAsMap()
                        .get("_meta");
                if (metaMap != null) {
                    String primaryKeyColumn = (String)metaMap.get("primary_keys");
                    if (primaryKeyColumn != null ) {
                        Document doc = new Document();

                        tableName.setStringValue(indexMetaData.getIndex());
                        doc.add(tableName);

                        constraintName.setStringValue(primaryKeyColumn);
                        doc.add(constraintName);

                        constraintType.setStringValue(ConstraintType.PRIMARY_KEY);
                        doc.add(constraintType);
                        indexWriter.addDocument(doc);
                    }
                }
            }
        }
    }
}
