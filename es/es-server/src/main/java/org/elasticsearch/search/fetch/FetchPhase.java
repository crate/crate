/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsFetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Fetch phase of a search request, used to fetch the actual top matching documents to be returned to the client, identified
 * after reducing all of the matches returned by the query phase
 */
public class FetchPhase implements SearchPhase {

    private final FetchSubPhase[] fetchSubPhases;

    public FetchPhase(List<FetchSubPhase> fetchSubPhases) {
        this.fetchSubPhases = fetchSubPhases.toArray(new FetchSubPhase[fetchSubPhases.size() + 1]);
        this.fetchSubPhases[fetchSubPhases.size()] = new InnerHitsFetchSubPhase(this);
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    @Override
    public void execute(SearchContext context) {
        final FieldsVisitor fieldsVisitor;
        Map<String, Set<String>> storedToRequestedFields = new HashMap<>();
        StoredFieldsContext storedFieldsContext = context.storedFieldsContext();

        if (storedFieldsContext == null) {
            // no fields specified, default to return source if no explicit indication
            if (!context.hasScriptFields() && !context.hasFetchSourceContext()) {
                context.fetchSourceContext(new FetchSourceContext(true));
            }
            fieldsVisitor = new FieldsVisitor(context.sourceRequested());
        } else if (storedFieldsContext.fetchFields() == false) {
            // disable stored fields entirely
            fieldsVisitor = null;
        } else {
            for (String fieldNameOrPattern : context.storedFieldsContext().fieldNames()) {
                if (fieldNameOrPattern.equals(SourceFieldMapper.NAME)) {
                    FetchSourceContext fetchSourceContext = context.hasFetchSourceContext() ? context.fetchSourceContext()
                        : FetchSourceContext.FETCH_SOURCE;
                    context.fetchSourceContext(new FetchSourceContext(true, fetchSourceContext.includes(), fetchSourceContext.excludes()));
                    continue;
                }

                Collection<String> fieldNames = context.mapperService().simpleMatchToFullName(fieldNameOrPattern);
                for (String fieldName : fieldNames) {
                    MappedFieldType fieldType = context.smartNameFieldType(fieldName);
                    if (fieldType == null) {
                        // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                        if (context.getObjectMapper(fieldName) != null) {
                            throw new IllegalArgumentException("field [" + fieldName + "] isn't a leaf field");
                        }
                    } else {
                        String storedField = fieldType.name();
                        Set<String> requestedFields = storedToRequestedFields.computeIfAbsent(
                            storedField, key -> new HashSet<>());
                        requestedFields.add(fieldName);
                    }
                }
            }
            boolean loadSource = context.sourceRequested();
            if (storedToRequestedFields.isEmpty()) {
                // empty list specified, default to disable _source if no explicit indication
                fieldsVisitor = new FieldsVisitor(loadSource);
            } else {
                fieldsVisitor = new CustomFieldsVisitor(storedToRequestedFields.keySet(), loadSource);
            }
        }

        try {
            SearchHit[] hits = new SearchHit[context.docIdsToLoadSize()];
            FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
            for (int index = 0; index < context.docIdsToLoadSize(); index++) {
                if (context.isCancelled()) {
                    throw new TaskCancelledException("cancelled");
                }
                int docId = context.docIdsToLoad()[context.docIdsToLoadFrom() + index];
                int readerIndex = ReaderUtil.subIndex(docId, context.searcher().getIndexReader().leaves());
                LeafReaderContext subReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
                int subDocId = docId - subReaderContext.docBase;

                final SearchHit searchHit;
                int rootDocId = findRootDocumentIfNested(context, subReaderContext, subDocId);
                if (rootDocId != -1) {
                    searchHit = createNestedSearchHit(context, docId, subDocId, rootDocId,
                        storedToRequestedFields, subReaderContext);
                } else {
                    searchHit = createSearchHit(context, fieldsVisitor, docId, subDocId,
                        storedToRequestedFields, subReaderContext);
                }

                hits[index] = searchHit;
                hitContext.reset(searchHit, subReaderContext, subDocId, context.searcher());
                for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
                    fetchSubPhase.hitExecute(context, hitContext);
                }
            }
            if (context.isCancelled()) {
                throw new TaskCancelledException("cancelled");
            }

            for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
                fetchSubPhase.hitsExecute(context, hits);
                if (context.isCancelled()) {
                    throw new TaskCancelledException("cancelled");
                }
            }

            context.fetchResult().hits(new SearchHits(hits, context.queryResult().getTotalHits(), context.queryResult().getMaxScore()));
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    private int findRootDocumentIfNested(SearchContext context, LeafReaderContext subReaderContext, int subDocId) throws IOException {
        if (context.mapperService().hasNested()) {
            BitSet bits = context.bitsetFilterCache()
                .getBitSetProducer(Queries.newNonNestedFilter(context.indexShard().indexSettings().getIndexVersionCreated()))
                .getBitSet(subReaderContext);
            if (!bits.get(subDocId)) {
                return bits.nextSetBit(subDocId);
            }
        }
        return -1;
    }

    private SearchHit createSearchHit(SearchContext context,
                                      FieldsVisitor fieldsVisitor,
                                      int docId,
                                      int subDocId,
                                      Map<String, Set<String>> storedToRequestedFields,
                                      LeafReaderContext subReaderContext) {
        if (fieldsVisitor == null) {
            return new SearchHit(docId);
        }

        Map<String, DocumentField> searchFields = getSearchFields(context, fieldsVisitor, subDocId,
            storedToRequestedFields, subReaderContext);

        DocumentMapper documentMapper = context.mapperService().documentMapper(fieldsVisitor.uid().type());
        Text typeText;
        if (documentMapper == null) {
            typeText = new Text(fieldsVisitor.uid().type());
        } else {
            typeText = documentMapper.typeText();
        }
        SearchHit searchHit = new SearchHit(docId, fieldsVisitor.uid().id(), typeText, searchFields);
        // Set _source if requested.
        SourceLookup sourceLookup = context.lookup().source();
        sourceLookup.setSegmentAndDocument(subReaderContext, subDocId);
        if (fieldsVisitor.source() != null) {
            sourceLookup.setSource(fieldsVisitor.source());
        }
        return searchHit;
    }

    private Map<String, DocumentField> getSearchFields(SearchContext context,
                                                       FieldsVisitor fieldsVisitor,
                                                       int subDocId,
                                                       Map<String, Set<String>> storedToRequestedFields,
                                                       LeafReaderContext subReaderContext) {
        loadStoredFields(context, subReaderContext, fieldsVisitor, subDocId);
        fieldsVisitor.postProcess(context.mapperService());

        if (fieldsVisitor.fields().isEmpty()) {
            return null;
        }

        Map<String, DocumentField> searchFields = new HashMap<>(fieldsVisitor.fields().size());
        for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
            String storedField = entry.getKey();
            List<Object> storedValues = entry.getValue();

            if (storedToRequestedFields.containsKey(storedField)) {
                for (String requestedField : storedToRequestedFields.get(storedField)) {
                    searchFields.put(requestedField, new DocumentField(requestedField, storedValues));
                }
            } else {
                searchFields.put(storedField, new DocumentField(storedField, storedValues));
            }
        }
        return searchFields;
    }

    private SearchHit createNestedSearchHit(SearchContext context,
                                            int nestedTopDocId,
                                            int nestedSubDocId,
                                            int rootSubDocId,
                                            Map<String, Set<String>> storedToRequestedFields,
                                            LeafReaderContext subReaderContext) throws IOException {
        // Also if highlighting is requested on nested documents we need to fetch the _source from the root document,
        // otherwise highlighting will attempt to fetch the _source from the nested doc, which will fail,
        // because the entire _source is only stored with the root document.
        final Uid uid;
        final BytesReference source;
        final boolean needSource = context.sourceRequested() || context.highlight() != null;
        if (needSource || (context instanceof InnerHitsContext.InnerHitSubContext == false)) {
            FieldsVisitor rootFieldsVisitor = new FieldsVisitor(needSource);
            loadStoredFields(context, subReaderContext, rootFieldsVisitor, rootSubDocId);
            rootFieldsVisitor.postProcess(context.mapperService());
            uid = rootFieldsVisitor.uid();
            source = rootFieldsVisitor.source();
        } else {
            // In case of nested inner hits we already know the uid, so no need to fetch it from stored fields again!
            uid = ((InnerHitsContext.InnerHitSubContext) context).getUid();
            source = null;
        }

        Map<String, DocumentField> searchFields = null;
        if (context.hasStoredFields() && !context.storedFieldsContext().fieldNames().isEmpty()) {
            FieldsVisitor nestedFieldsVisitor = new CustomFieldsVisitor(storedToRequestedFields.keySet(), false);
            searchFields = getSearchFields(context, nestedFieldsVisitor, nestedSubDocId,
                storedToRequestedFields, subReaderContext);
        }

        final String typeText;
        if (uid != null && uid.type() != null) {
            typeText = uid.type();
        } else {
            // stored fields are disabled but it is not allowed to disable them on inner hits
            // if the index has multiple types so we can assume that the index has a single type.
            assert context.mapperService().types().size() == 1;
            typeText = context.mapperService().types().iterator().next();
        }
        DocumentMapper documentMapper = context.mapperService().documentMapper(typeText);
        SourceLookup sourceLookup = context.lookup().source();
        sourceLookup.setSegmentAndDocument(subReaderContext, nestedSubDocId);

        ObjectMapper nestedObjectMapper = documentMapper.findNestedObjectMapper(nestedSubDocId, context, subReaderContext);
        assert nestedObjectMapper != null;
        SearchHit.NestedIdentity nestedIdentity =
                getInternalNestedIdentity(context, nestedSubDocId, subReaderContext, context.mapperService(), nestedObjectMapper);

        if (source != null) {
            Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(source, true);
            Map<String, Object> sourceAsMap = tuple.v2();

            // Isolate the nested json array object that matches with nested hit and wrap it back into the same json
            // structure with the nested json array object being the actual content. The latter is important, so that
            // features like source filtering and highlighting work consistent regardless of whether the field points
            // to a json object array for consistency reasons on how we refer to fields
            Map<String, Object> nestedSourceAsMap = new HashMap<>();
            Map<String, Object> current = nestedSourceAsMap;
            for (SearchHit.NestedIdentity nested = nestedIdentity; nested != null; nested = nested.getChild()) {
                String nestedPath = nested.getField().string();
                current.put(nestedPath, new HashMap<>());
                Object extractedValue = XContentMapValues.extractValue(nestedPath, sourceAsMap);
                List<?> nestedParsedSource;
                if (extractedValue instanceof List) {
                    // nested field has an array value in the _source
                    nestedParsedSource = (List<?>) extractedValue;
                } else if (extractedValue instanceof Map) {
                    // nested field has an object value in the _source. This just means the nested field has just one inner object,
                    // which is valid, but uncommon.
                    nestedParsedSource = Collections.singletonList(extractedValue);
                } else {
                    throw new IllegalStateException("extracted source isn't an object or an array");
                }
                if ((nestedParsedSource.get(0) instanceof Map) == false &&
                    nestedObjectMapper.parentObjectMapperAreNested(context.mapperService()) == false) {
                    // When one of the parent objects are not nested then XContentMapValues.extractValue(...) extracts the values
                    // from two or more layers resulting in a list of list being returned. This is because nestedPath
                    // encapsulates two or more object layers in the _source.
                    //
                    // This is why only the first element of nestedParsedSource needs to be checked.
                    throw new IllegalArgumentException("Cannot execute inner hits. One or more parent object fields of nested field [" +
                        nestedObjectMapper.name() + "] are not nested. All parent fields need to be nested fields too");
                }
                sourceAsMap = (Map<String, Object>) nestedParsedSource.get(nested.getOffset());
                if (nested.getChild() == null) {
                    current.put(nestedPath, sourceAsMap);
                } else {
                    Map<String, Object> next = new HashMap<>();
                    current.put(nestedPath, next);
                    current = next;
                }
            }
            context.lookup().source().setSource(nestedSourceAsMap);
            XContentType contentType = tuple.v1();
            context.lookup().source().setSourceContentType(contentType);
        }
        return new SearchHit(nestedTopDocId, uid.id(), documentMapper.typeText(), nestedIdentity, searchFields);
    }

    private SearchHit.NestedIdentity getInternalNestedIdentity(SearchContext context, int nestedSubDocId,
                                                               LeafReaderContext subReaderContext,
                                                               MapperService mapperService,
                                                               ObjectMapper nestedObjectMapper) throws IOException {
        int currentParent = nestedSubDocId;
        ObjectMapper nestedParentObjectMapper;
        ObjectMapper current = nestedObjectMapper;
        String originalName = nestedObjectMapper.name();
        SearchHit.NestedIdentity nestedIdentity = null;
        final IndexSettings indexSettings = context.getQueryShardContext().getIndexSettings();
        do {
            Query parentFilter;
            nestedParentObjectMapper = current.getParentObjectMapper(mapperService);
            if (nestedParentObjectMapper != null) {
                if (nestedParentObjectMapper.nested().isNested() == false) {
                    current = nestedParentObjectMapper;
                    continue;
                }
                parentFilter = nestedParentObjectMapper.nestedTypeFilter();
            } else {
                parentFilter = Queries.newNonNestedFilter(context.indexShard().indexSettings().getIndexVersionCreated());
            }

            Query childFilter = nestedObjectMapper.nestedTypeFilter();
            if (childFilter == null) {
                current = nestedParentObjectMapper;
                continue;
            }
            final Weight childWeight = context.searcher().createNormalizedWeight(childFilter, false);
            Scorer childScorer = childWeight.scorer(subReaderContext);
            if (childScorer == null) {
                current = nestedParentObjectMapper;
                continue;
            }
            DocIdSetIterator childIter = childScorer.iterator();

            BitSet parentBits = context.bitsetFilterCache().getBitSetProducer(parentFilter).getBitSet(subReaderContext);

            int offset = 0;
            if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_6_5_0)) {
                /**
                 * Starts from the previous parent and finds the offset of the
                 * <code>nestedSubDocID</code> within the nested children. Nested documents
                 * are indexed in the same order than in the source array so the offset
                 * of the nested child is the number of nested document with the same parent
                 * that appear before him.
                 */
                int previousParent = parentBits.prevSetBit(currentParent);
                for (int docId = childIter.advance(previousParent + 1); docId < nestedSubDocId && docId != DocIdSetIterator.NO_MORE_DOCS;
                        docId = childIter.nextDoc()) {
                    offset++;
                }
                currentParent = nestedSubDocId;
            } else {
                /**
                 * Nested documents are in reverse order in this version so we start from the current nested document
                 * and find the number of documents with the same parent that appear after it.
                 */
                int nextParent = parentBits.nextSetBit(currentParent);
                for (int docId = childIter.advance(currentParent + 1); docId < nextParent && docId != DocIdSetIterator.NO_MORE_DOCS;
                        docId = childIter.nextDoc()) {
                    offset++;
                }
                currentParent = nextParent;
            }
            current = nestedObjectMapper = nestedParentObjectMapper;
            int currentPrefix = current == null ? 0 : current.name().length() + 1;
            nestedIdentity = new SearchHit.NestedIdentity(originalName.substring(currentPrefix), offset, nestedIdentity);
            if (current != null) {
                originalName = current.name();
            }
        } while (current != null);
        return nestedIdentity;
    }

    private void loadStoredFields(SearchContext searchContext, LeafReaderContext readerContext, FieldsVisitor fieldVisitor, int docId) {
        fieldVisitor.reset();
        try {
            readerContext.reader().document(docId, fieldVisitor);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(searchContext, "Failed to fetch doc id [" + docId + "]", e);
        }
    }
}
