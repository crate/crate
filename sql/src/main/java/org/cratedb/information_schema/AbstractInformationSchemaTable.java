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
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * abstract implementation of InformationSchemaTable that does the heavy lifting
 * of starting and handling an MMap-lucene-index as backend.
 *
 * All you have to do is define columns, a fieldmapper, an index-method and optionally a
 * quer-method
 */
public abstract class AbstractInformationSchemaTable implements InformationSchemaTable {

    protected final IndexWriterConfig indexWriterConfig;
    protected IndexWriter indexWriter = null;

    protected final Object indexLock = new Object();
    protected SearcherManager searcherManager;
    protected IndexSearcher indexSearcher;
    protected final SearcherFactory searcherFactory;
    protected AtomicInteger activeSearches = new AtomicInteger(0);
    protected Directory indexDirectory;

    public AbstractInformationSchemaTable() {
        this.searcherFactory = new SearcherFactory();
        this.indexWriterConfig = new IndexWriterConfig(Version.LUCENE_44, null);
        this.indexWriterConfig.setCodec(new Lucene42Codec());
    }

    @Override
    public void init() throws CrateException {
        try {
            indexDirectory = new MMapDirectory(Files.createTempDir());
            indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);
            searcherManager = new SearcherManager(indexWriter, true, searcherFactory);
            indexSearcher = searcherManager.acquire();
        } catch(IOException ioe) {
            throw new CrateException(ioe);
        }
    }

    @Override
    public boolean initialized() {
        return indexSearcher != null && searcherManager != null && indexWriter != null;
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
        activeSearches.incrementAndGet();
        Sort sort = null;
        if (stmt.hasOrderBy()) {
            sort = getSort(stmt);
        }

        Integer limit = initLimit(stmt);
        Integer offset = initOffset(stmt);
        limit += offset;

        TopDocs docs;
        if (sort != null) {
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

                searcherManager.maybeRefresh();
                // refresh searcher after index

                // can only replace the indexSearcher with the new one if no search is active
                // until the searcher can be replaced all searches will get old results.
                if (activeSearches.get() == 0) {
                    IndexSearcher newIndexSearcher = searcherManager.acquire();
                    searcherManager.release(indexSearcher);
                    indexSearcher = newIndexSearcher;
                }

            } catch (IOException e) {
                throw new CrateException(e);
            }
        }
    }

    /**
     *
     * @return the number of indexed documents
     * @throws CrateException
     */
    public long count() throws CrateException {
        activeSearches.getAndIncrement();
        try {
            return Lucene.count(indexSearcher, new MatchAllDocsQuery());
        } catch (IOException e) {
            throw new CrateException(e);
        } finally {
            activeSearches.decrementAndGet();
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
            if (searcherManager != null) {
                searcherManager.close();
                searcherManager = null;
            }
            if (indexWriter != null) {
                indexWriter.close();
                indexWriter = null;
            }
            if (indexDirectory != null) {
                indexDirectory.close();
                indexDirectory = null;
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
        List<SortField> sortFields = new ArrayList<>();

        for (int i = 0; i < stmt.orderByColumns.size(); i++) {
            OrderByColumnName column = stmt.orderByColumns.get(i);
            boolean reverse = !column.isAsc();
            InformationSchemaColumn tableColumn = this.fieldMapper().get(column.name);
            if (tableColumn != null) {
                sortFields.add(new SortField(column.name, tableColumn.type, reverse));
            }
        }
        if (sortFields.size() == 0) {
            return null;
        } else {
            return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
        }
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
                IndexableField[] fields = doc.getFields(cols[c]);
                InformationSchemaColumn tableColumn = fieldMapper().get(cols[c]);
                Object rowValue = null;
                if (fields.length > 0) {
                    if (tableColumn.allowMultipleValues) {
                        List<Object> rowValues = new ArrayList<>(fields.length);
                        for (int i=0; i < fields.length; i++) {
                            rowValues.add(tableColumn.getValue(fields[i]));
                        }
                        rowValue = rowValues;
                    } else {
                        rowValue = tableColumn.getValue(fields[0]);
                    }
                }
                rows[r][c] = rowValue;
            }
            r++;
        }
        return new SQLResponse(cols, rows, rows.length);
    }
}
