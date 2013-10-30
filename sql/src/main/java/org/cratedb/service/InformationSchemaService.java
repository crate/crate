package org.cratedb.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.cratedb.action.sql.*;
import org.cratedb.information_schema.InformationSchemaColumn;
import org.cratedb.information_schema.TablesTable;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.TableUnknownException;
import org.cratedb.sql.parser.parser.NodeTypes;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;

/**
 * the informationSchemaService creates and manages lucene in-memory indices which can be used
 * to query the clusterState (tables, columns... etc) using SQL.
 *
 * Each node holds and manages its own index.
 * They are created on first access and once created are
 * updated according to various events (like index created...)
 */
public class InformationSchemaService extends AbstractLifecycleComponent<InformationSchemaService> {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final ClusterService clusterService;
    private final SearcherFactory searcherFactory;
    private final Object readLock = new Object();
    private final IndexWriterConfig indexWriterConfig;
    private final ImmutableMap<String, InformationSchemaColumn> tablesFieldMapper;
    private boolean dirty;
    private IndexWriter indexWriter = null;

    private SearcherManager tablesSearcherManager;
    private IndexSearcher indexSearcher;
    private IndexSearcher newIndexSearcher;
    private AtomicInteger activeSearches = new AtomicInteger(0);
    private Directory tablesDirectory;

    @Inject
    public InformationSchemaService(Settings settings,
                                    ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
        this.searcherFactory = new SearcherFactory();
        this.indexWriterConfig = new IndexWriterConfig(Version.LUCENE_44, null);
        this.indexWriterConfig.setCodec(new Lucene42Codec());
        this.dirty = false;
        this.tablesFieldMapper = new TablesTable().fieldMapper();
    }

    @Override
    protected void doStart() throws ElasticSearchException {
    }

    @Override
    protected void doStop() throws ElasticSearchException {
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        try {
            if (tablesSearcherManager != null) {
                tablesSearcherManager.close();
            }

            if (indexWriter != null) {
                indexWriter.close();
            }

            if (tablesDirectory != null) {
                tablesDirectory.close();
            }

        } catch (IOException e) {
            throw new CrateException(e);
        }
    }

    private void initSearcher() throws IOException {
        initTablesIndexSearcher();
        indexSearcher = tablesSearcherManager.acquire();
    }

    private void initTablesIndexSearcher() throws IOException {
        // TODO:
        // ramDirectory seems to have trouble with large indices because internally it always
        // uses fixed size byte buffers that are quite small and therefore cause many GC
        // maybe this should be changed to MMapDirectory and recommend to mount /tmp with tmpfs
        //tablesDirectory = new RAMDirectory();
        tablesDirectory = new MMapDirectory(Files.createTempDir());
        indexWriter = new IndexWriter(tablesDirectory, indexWriterConfig);
        tablesSearcherManager = new SearcherManager(indexWriter, true, searcherFactory);

        clusterService.add(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.metaDataChanged()) {
                    synchronized (readLock) {
                        dirty = true;
                    }
                }
            }
        });

        indexMetaData(clusterService.state().getMetaData().indices());
    }

    private void indexMetaData(Map<String, IndexMetaData> indices) throws IOException {
        indexWriter.deleteAll();

        // according to http://wiki.apache.org/lucene-java/ImproveIndexingSpeed
        // re-using fields is faster than re-creating the field inside the loop
        StringField tableName = new StringField(TablesTable.Columns.TABLE_NAME, "", Field.Store.YES);
        IntField numberOfShards = new IntField(TablesTable.Columns.NUMBER_OF_SHARDS, 0, Field.Store.YES);
        IntField numberOfReplicas = new IntField(TablesTable.Columns.NUMBER_OF_REPLICAS, 0, Field.Store.YES);

        for (IndexMetaData metaData : indices.values()) {
            Document doc = new Document();
            tableName.setStringValue(metaData.getIndex());
            doc.add(tableName);

            numberOfShards.setIntValue(metaData.getNumberOfShards());
            doc.add(numberOfShards);

            numberOfReplicas.setIntValue(metaData.getNumberOfReplicas());
            doc.add(numberOfReplicas);

            indexWriter.addDocument(doc);
        }

        tablesSearcherManager.maybeRefresh();
    }

    public void execute(ParsedStatement stmt, final ActionListener<SQLResponse> listener) throws IOException {
        if (!stmt.schemaName().equalsIgnoreCase("information_schema")) {
            listener.onFailure(new IllegalStateException("Trying to query information schema with invalid ParsedStatement"));
            return;
        }

        if (stmt.nodeType() != NodeTypes.CURSOR_NODE) {
            throw new SQLParseException(
                "INFORMATION_SCHEMA tables are virtual and read-only. Only SELECT statements are supported");
        }

        switch (stmt.tableName().toLowerCase()) {
            case TablesTable.NAME:
                queryTables(stmt, listener);
                break;

            default:
                listener.onFailure(new TableUnknownException(stmt.tableName()));
        }
    }

    public ActionFuture<SQLResponse> execute(ParsedStatement stmt) throws IOException {
        PlainActionFuture<SQLResponse> future = newFuture();
        execute(stmt, future);
        return future;
    }

    private void queryTables(ParsedStatement stmt, ActionListener<SQLResponse> listener) throws IOException {
        synchronized (readLock) {
            if (indexSearcher == null) {
                initSearcher();
            } else if (dirty) {
                indexMetaData(clusterService.state().getMetaData().indices());
                // re-open the indexSearcher to get new results
                newIndexSearcher = tablesSearcherManager.acquire();
                dirty = false;
            }

            // can only replace the indexSearcher with the new one if no search is active
            // until the searcher can be replaced all searches will get old results.
            if (activeSearches.getAndIncrement() == 0 && newIndexSearcher != null) {
                tablesSearcherManager.release(indexSearcher);
                indexSearcher = newIndexSearcher;
                newIndexSearcher = null;
            }
        }

        Sort sort = null;
        if (stmt.hasOrderBy()) {
            sort = getSort(stmt);
        }

        Integer limit = initLimit(stmt);
        Integer offset = initOffset(stmt);
        limit += offset;

        TopDocs docs;
        if (stmt.hasOrderBy()) {
            docs = indexSearcher.search(stmt.query, null, limit, sort);
        } else {
            docs = indexSearcher.search(stmt.query, null, limit);
        }

        SQLResponse response = docsToSQLResponse(indexSearcher, stmt, docs, offset);
        activeSearches.decrementAndGet();
        listener.onResponse(response);
    }

    private Integer initOffset(ParsedStatement stmt) {
        Integer offset = 0;
        if (stmt.offset != null) {
            offset = stmt.offset;
        }
        return offset;
    }

    private Integer initLimit(ParsedStatement stmt) {
        Integer limit = stmt.limit;
        if (limit == null) {
            limit = SQLParseService.DEFAULT_SELECT_LIMIT;
        }
        return limit;
    }

    private Sort getSort(ParsedStatement stmt) {
        SortField[] sortFields = new SortField[stmt.orderByColumns.size()];

        for (int i = 0; i < stmt.orderByColumns.size(); i++) {
            OrderByColumnName column = stmt.orderByColumns.get(i);
            boolean reverse = !column.isAsc;
            sortFields[i] = new SortField(
                column.name, tablesFieldMapper.get(column.name).type, reverse);
        }

        return new Sort(sortFields);
    }

    private SQLResponse docsToSQLResponse(IndexSearcher searcher, ParsedStatement stmt,
                                          TopDocs docs, Integer offset) throws IOException {
        // TODO: move into SQLResponseBuilder ?

        String[] cols = stmt.cols();
        Set<String> fieldsToLoad = new HashSet<>();
        Collections.addAll(fieldsToLoad, cols);

        Object[][] rows = new Object[Math.max(docs.scoreDocs.length - offset, 0)][stmt.cols().length];

        int r = 0;
        for (ScoreDoc scoreDoc : docs.scoreDocs) {
            if (offset > 0) {
                offset--;
                continue;
            }
            Document doc = searcher.doc(scoreDoc.doc, fieldsToLoad);
            for (int c = 0; c < cols.length; c++) {
                IndexableField field = doc.getField(cols[c]);
                if (field == null) {
                    rows[r][c] = null;
                } else {
                    rows[r][c] = tablesFieldMapper.get(cols[c]).getValue(field);
                }
            }
            r++;
        }
        return new SQLResponse(cols, rows, rows.length);
    }
}
