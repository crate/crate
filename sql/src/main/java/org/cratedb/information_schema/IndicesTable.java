package org.cratedb.information_schema;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.index.IndexMetaDataExtractor;
import org.cratedb.lucene.fields.StringLuceneField;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * virtual information_schema table listing table index definitions
 */
public class IndicesTable extends AbstractInformationSchemaTable {

    public static final String NAME = "indices";

    private Map<String, List> indicesExpressions;

    public class Columns {
        public static final String TABLE_NAME = "table_name";
        public static final String INDEX_NAME = "index_name";
        public static final String METHOD = "method";
        public static final String COLUMNS = "columns";
        public static final String PROPERTIES = "properties";
    }

    StringField tableNameField = new StringField(Columns.TABLE_NAME, "", Field.Store.YES);
    StringField indexNameField = new StringField(Columns.INDEX_NAME, "", Field.Store.YES);
    StringField methodField = new StringField(Columns.METHOD, "", Field.Store.YES);
    StringField propertiesField = new StringField(Columns.PROPERTIES, "", Field.Store.YES);
    // only internal used
    StringField uidField = new StringField("uid", "", Field.Store.YES);


    @Inject
    public IndicesTable(Map<String, AggFunction> aggFunctionMap, CacheRecycler cacheRecycler) {
        super(aggFunctionMap, cacheRecycler);
        fieldMapper.put(
                Columns.TABLE_NAME,
                new StringLuceneField(Columns.TABLE_NAME)
        );
        fieldMapper.put(
                Columns.INDEX_NAME,
                new StringLuceneField(Columns.INDEX_NAME)
        );
        fieldMapper.put(
                Columns.METHOD,
                new StringLuceneField(Columns.METHOD)
        );
        fieldMapper.put(
                Columns.COLUMNS,
                new StringLuceneField(Columns.COLUMNS, true)
        );
        fieldMapper.put(
                Columns.PROPERTIES,
                new StringLuceneField(Columns.PROPERTIES)
        );
    }

    @Override
    public void doIndex(ClusterState clusterState) throws IOException {

        for (ObjectCursor<IndexMetaData> cursor: clusterState.metaData().indices().values()) {
            IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(cursor.value);
            // ignore closed indices
            if (extractor.isIndexClosed()) {
                continue;
            }

            for (IndexMetaDataExtractor.Index index: extractor
                    .getIndices()) {
                addIndexDocument(index);
            }

        }
    }

    private void addIndexDocument(IndexMetaDataExtractor.Index index) throws IOException {
        Document doc = new Document();

        tableNameField.setStringValue(index.tableName);
        doc.add(tableNameField);

        indexNameField.setStringValue(index.indexName);
        doc.add(indexNameField);

        methodField.setStringValue(index.method);
        doc.add(methodField);

        for (String column : index.columns) {
            StringField columnsField = new StringField(Columns.COLUMNS, column, Field.Store.YES);
            doc.add(columnsField);
        }

        propertiesField.setStringValue(index.getPropertiesString());
        doc.add(propertiesField);

        uidField.setStringValue(index.getUid());
        doc.add(uidField);

        indexWriter.updateDocument(new Term("uid", index.getUid()), doc);
    }
}
