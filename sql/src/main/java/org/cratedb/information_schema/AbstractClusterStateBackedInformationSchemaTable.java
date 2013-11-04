package org.cratedb.information_schema;

import com.google.common.io.Files;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.cratedb.action.sql.OrderByColumnName;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.CrateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractClusterStateBackedInformationSchemaTable implements InformationSchemaTable {

    protected final IndexWriterConfig indexWriterConfig;
    protected IndexWriter indexWriter = null;

    protected final Object indexLock = new Object();
    protected SearcherManager tablesSearcherManager;
    protected IndexSearcher indexSearcher;
    protected final SearcherFactory searcherFactory;
    protected AtomicInteger activeSearches = new AtomicInteger(0);
    protected Directory tablesDirectory;

    public AbstractClusterStateBackedInformationSchemaTable() {
        this.searcherFactory = new SearcherFactory();
        this.indexWriterConfig = new IndexWriterConfig(Version.LUCENE_44, null);
        this.indexWriterConfig.setCodec(new Lucene42Codec());
    }

    @Override
    public void init() throws CrateException {
        try {
            tablesDirectory = new MMapDirectory(Files.createTempDir());
            indexWriter = new IndexWriter(tablesDirectory, indexWriterConfig);
            tablesSearcherManager = new SearcherManager(indexWriter, true, searcherFactory);
            indexSearcher = tablesSearcherManager.acquire();
        } catch(IOException ioe) {
            throw new CrateException(ioe);
        }
    }

    @Override
    public boolean initialized() {
        return indexSearcher != null;
    }

    @Override
    public void query(ParsedStatement stmt, ActionListener<SQLResponse> listener) {
        try {
            doQuery(stmt, listener);
        } catch(IOException e) {
            throw new CrateException(e);
        }
    }

    /**
     * override this method if you need some special query logic
     *
     * this one here should do the heavy-lifting for you if you're fine with the default
     *
     * @param stmt
     * @param listener
     */
    public void doQuery(ParsedStatement stmt, ActionListener<SQLResponse> listener) throws IOException {
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

    @Override
    public void index(ClusterState clusterState) {
        synchronized (indexLock) {
            try {
                if (!initialized()) {
                    init();
                }
                indexWriter.deleteAll();

                doIndex(clusterState);

                tablesSearcherManager.maybeRefresh();
                // refresh searcher after index

                IndexSearcher newIndexSearcher = tablesSearcherManager.acquire();
                // can only replace the indexSearcher with the new one if no search is active
                // until the searcher can be replaced all searches will get old results.
                if (activeSearches.getAndIncrement() == 0) {
                    tablesSearcherManager.release(indexSearcher);
                    indexSearcher = newIndexSearcher;
                }


            } catch (IOException e) {
                throw new CrateException(e);
            }
        }
    }

    /**
     * override this method to do the actual index work
     *
     * all documents were deleted before
     * and the searchManager will be refreshed afterwards, so you don't have to do it here
     * @param clusterState
     */
    public abstract void doIndex(ClusterState clusterState) throws IOException;

    @Override
    public void close() throws CrateException {
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


    // Mothers little Helpers

    protected Integer initOffset(ParsedStatement stmt) {
        Integer offset = 0;
        if (stmt.offset != null) {
            offset = stmt.offset;
        }
        return offset;
    }

    protected Integer initLimit(ParsedStatement stmt) {
        Integer limit = stmt.limit;
        if (limit == null) {
            limit = SQLParseService.DEFAULT_SELECT_LIMIT;
        }
        return limit;
    }

    protected Sort getSort(ParsedStatement stmt) {
        SortField[] sortFields = new SortField[stmt.orderByColumns.size()];

        for (int i = 0; i < stmt.orderByColumns.size(); i++) {
            OrderByColumnName column = stmt.orderByColumns.get(i);
            boolean reverse = !column.isAsc;
            sortFields[i] = new SortField(
                    column.name, this.fieldMapper().get(column.name).type, reverse);
        }

        return new Sort(sortFields);
    }

    protected SQLResponse docsToSQLResponse(IndexSearcher searcher, ParsedStatement stmt,
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
                rows[r][c] = fieldMapper().get(cols[c]).getValue(field);
            }
            r++;
        }
        return new SQLResponse(cols, rows, rows.length);
    }
}
