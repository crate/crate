/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.information_schema;

import com.google.common.io.Files;
import org.apache.lucene.codecs.lucene45.Lucene45Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.cratedb.DataType;
import org.cratedb.action.collect.CollectorContext;
import org.cratedb.action.collect.scope.GlobalExpressionDescription;
import org.cratedb.action.groupby.*;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.OrderByColumnName;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.lucene.LuceneFieldMapper;
import org.cratedb.lucene.fields.LuceneField;
import org.cratedb.sql.CrateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cache.recycler.CacheRecycler;
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

    private final CacheRecycler cacheRecycler;
    protected LuceneFieldMapper fieldMapper = new LuceneFieldMapper();

    protected final IndexWriterConfig indexWriterConfig;
    private final Map<String, AggFunction> aggFunctionMap;
    protected IndexWriter indexWriter = null;

    protected final Object indexLock = new Object();
    protected SearcherManager searcherManager;
    protected IndexSearcher indexSearcher;
    protected final SearcherFactory searcherFactory;
    protected AtomicInteger activeSearches = new AtomicInteger(0);
    protected Directory indexDirectory;

    public AbstractInformationSchemaTable(Map<String, AggFunction> aggFunctionMap,
                                          CacheRecycler cacheRecycler) {
        this.searcherFactory = new SearcherFactory();
        this.indexWriterConfig = new IndexWriterConfig(Version.LUCENE_45, null);
        this.indexWriterConfig.setCodec(new Lucene45Codec());
        this.aggFunctionMap = aggFunctionMap;
        this.cacheRecycler = cacheRecycler;
    }

    @Override
    public Iterable<String> cols() {
        return fieldMapper.keySet();
    }

    @Override
    public LuceneFieldMapper fieldMapper() {
        return fieldMapper;
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
    public void query(ParsedStatement stmt, ActionListener<SQLResponse> listener,
                      long requestStartedTime) {
        try {
            doQuery(stmt, listener, requestStartedTime);
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
    public void doQuery(ParsedStatement stmt, ActionListener<SQLResponse> listener,
                        long requestStartedTime) throws IOException {
        activeSearches.incrementAndGet();
        Sort sort = null;
        if (stmt.hasOrderBy()) {
            sort = getSort(stmt);
        }

        TopDocs docs;
        if (stmt.hasGroupBy() || stmt.isGlobalAggregate()) {
            SQLResponse response = doGroupByQuery(stmt, requestStartedTime);
            activeSearches.decrementAndGet();
            listener.onResponse(response);
            return;
        }

        if (sort != null) {
            docs = indexSearcher.search(stmt.query, null, stmt.totalLimit(), sort);
        } else {
            docs = indexSearcher.search(stmt.query, null, stmt.totalLimit());
        }

        SQLResponse response = docsToSQLResponse(indexSearcher, stmt, docs, stmt.offset(),
                requestStartedTime);
        activeSearches.decrementAndGet();
        listener.onResponse(response);
    }

    protected SQLResponse doGroupByQuery(ParsedStatement stmt, long requestStartedTime) throws IOException {
        assert stmt.hasGroupBy() || stmt.isGlobalAggregate();

        CollectorContext collectorContext = new CollectorContext();
        collectorContext.fieldLookup(new InformationSchemaFieldLookup(fieldMapper()));
        collectorContext.cacheRecycler(cacheRecycler);
        SQLGroupingCollector collector;
        if (stmt.isGlobalAggregate()) {
            collector = new GlobalSQLGroupingCollector(
                    stmt,
                    collectorContext,
                    aggFunctionMap,
                    1
            );
        } else {
            collector = new SQLGroupingCollector(
                    stmt,
                    collectorContext,
                    aggFunctionMap,
                    1
            );
        }

        indexSearcher.search(stmt.query, collector);

        final List<GroupByRow> rowList = new ArrayList<>();
        Rows rows = collector.rows();
        rows.walk(new Rows.RowVisitor() {
            @Override
            public void visit(GroupByRow row) {
                row.terminatePartial();
                rowList.add(row);
            }
        });
        return groupByRowsToSQLResponse(stmt, rowList, requestStartedTime);
    }

    private SQLResponse groupByRowsToSQLResponse(ParsedStatement stmt,
                                                 List<GroupByRow> rows,
                                                 long requestStartedTime) {
        GroupByFieldExtractor[] extractors = GroupByHelper.buildFieldExtractor(stmt, null);
        GroupByRowComparator comparator = new GroupByRowComparator(extractors, stmt.orderByIndices());

        Object[][] sortedRows = GroupByHelper.sortedRowsToObjectArray(
                GroupByHelper.sortAndTrimRows(rows, comparator, stmt.totalLimit()),
                stmt,
                extractors
        );
        return new SQLResponse(
            stmt.cols(),
            sortedRows,
            sortedRows.length,
            requestStartedTime
        );
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

    protected Sort getSort(ParsedStatement stmt) {
        List<SortField> sortFields = new ArrayList<>();

        for (int i = 0; i < stmt.orderByColumns.size(); i++) {
            OrderByColumnName column = stmt.orderByColumns.get(i);
            boolean reverse = !column.isAsc();
            LuceneField tableColumn = this.fieldMapper().get(column.name);
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
                                          TopDocs docs, Integer offset,
                                          long requestStartedTime) throws IOException {
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
            int globalExprIdx = 0;
            for (int c = 0; c < cols.length; c++) {
                Object rowValue = null;
                if (stmt.resultColumnList().get(c) instanceof GlobalExpressionDescription) {
                    rowValue = stmt.globalExpressions().get(globalExprIdx++).evaluate();
                } else if (stmt.resultColumnList().get(c).returnType() == DataType.NOT_SUPPORTED){
                    rowValue = null;
                } else {
                    IndexableField[] fields = doc.getFields(cols[c]);
                    LuceneField tableColumn = fieldMapper().get(cols[c]);

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
                }
                rows[r][c] = rowValue;
            }
            r++;
        }
        return new SQLResponse(cols, rows, rows.length, requestStartedTime);
    }
}
