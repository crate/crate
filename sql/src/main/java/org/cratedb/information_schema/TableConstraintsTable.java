package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.index.IndexMetaDataExtractor;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * virtual information_schema table listing table constraints like primary_key constraints
 */
public class TableConstraintsTable extends AbstractInformationSchemaTable {

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

    @Inject
    public TableConstraintsTable(Map<String, AggFunction> aggFunctionMap) {
        super(aggFunctionMap);

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
            IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(indexMetaData);
            List<String> primaryKeyColumns = extractor.getPrimaryKeys();
            if (primaryKeyColumns.size() > 0) {
                Document doc = new Document();

                tableName.setStringValue(extractor.getIndexName());
                doc.add(tableName);

                // TODO: support multiple primary keys
                constraintName.setStringValue(primaryKeyColumns.get(0));
                doc.add(constraintName);

                constraintType.setStringValue(ConstraintType.PRIMARY_KEY);
                doc.add(constraintType);
                indexWriter.addDocument(doc);
            }
        }
    }
}
