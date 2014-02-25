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

package org.cratedb.export;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.cratedb.action.export.ExportContext;
import org.elasticsearch.common.text.StringAndBytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.fieldvisitor.*;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;


public class ExportCollector extends Collector {

    private IndexReader currentReader;
    private long numExported = 0;
    private final FieldsVisitor fieldsVisitor;
    private final ExportContext context;

    private List<String> extractFieldNames;
    boolean sourceRequested;
    private ExportFields exportFields;
    private final OutputStream out;
    private AtomicReaderContext arc;
    private final FetchSubPhase[] fetchSubPhases;

    public ExportCollector(ExportContext context,
                           OutputStream os, FetchSubPhase[] fetchSubPhases) {
        this.out = os;
        this.context = context;
        this.fetchSubPhases = fetchSubPhases;
        this.exportFields = new ExportFields(context.fieldNames());
        sourceRequested = false;

        if (!context.hasFieldNames()) {
            if (context.hasPartialFields()) {
                // partial fields need the source, so fetch it, but don't return it
                fieldsVisitor = new UidAndSourceFieldsVisitor();
            } else if (context.hasScriptFields()) {
                // we ask for script fields, and no field names, don't load the source
                fieldsVisitor = new JustUidFieldsVisitor();
            } else {
                sourceRequested = true;
                fieldsVisitor = new UidAndSourceFieldsVisitor();
            }
        } else if (context.fieldNames().isEmpty()) {
            fieldsVisitor = new JustUidFieldsVisitor();
        } else {
            boolean loadAllStored = false;
            Set<String> fieldNames = null;
            for (String fieldName : context.fieldNames()) {
                if (fieldName.equals("*")) {
                    loadAllStored = true;
                    continue;
                }
                if (fieldName.equals(SourceFieldMapper.NAME)) {
                    sourceRequested = true;
                    continue;
                }
                FieldMappers x = context.smartNameFieldMappers
                        (fieldName);
                if (x != null && x.mapper().fieldType().stored()) {
                    if (fieldNames == null) {
                        fieldNames = new HashSet<String>();
                    }
                    fieldNames.add(x.mapper().names().indexName());
                } else {
                    if (extractFieldNames == null) {
                        extractFieldNames = newArrayList();
                    }
                    extractFieldNames.add(fieldName);
                }
            }
            if (loadAllStored) {
                fieldsVisitor = new AllFieldsVisitor(); // load everything, including _source
            } else if (fieldNames != null) {
                boolean loadSource = extractFieldNames != null || sourceRequested;
                fieldsVisitor = new CustomFieldsVisitor(fieldNames, loadSource);
            } else if (extractFieldNames != null || sourceRequested) {
                fieldsVisitor = new UidAndSourceFieldsVisitor();
            } else {
                fieldsVisitor = new JustUidFieldsVisitor();
            }
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.arc = context;
        this.currentReader = context.reader();
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    public long numExported(){
        return numExported;
    }

    @Override
    public void collect(int doc) throws IOException {
        fieldsVisitor.reset();
        exportFields = new ExportFields(context.fieldNames());
        currentReader.document(doc, fieldsVisitor);

        Map<String, SearchHitField> searchFields = null;
        if (fieldsVisitor.fields() != null) {
            searchFields = new HashMap<String, SearchHitField>(fieldsVisitor.fields().size());
            for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
                searchFields.put(entry.getKey(), new InternalSearchHitField(entry.getKey(), entry.getValue()));
            }
        }

        DocumentMapper documentMapper = context.mapperService()
                .documentMapper(fieldsVisitor.uid().type());
        Text typeText;
        if (documentMapper == null) {
            typeText = new StringAndBytesText(fieldsVisitor.uid().type());
        } else {
            typeText = documentMapper.typeText();
        }

        InternalSearchHit searchHit = new InternalSearchHit(doc,
                fieldsVisitor.uid().id(), typeText,
                searchFields);
        if(sourceRequested) {
            searchHit.sourceRef(fieldsVisitor.source());
        }

        for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
            FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
            if (fetchSubPhase.hitExecutionNeeded(context)) {
                hitContext.reset(searchHit, arc, doc, context.searcher().getIndexReader(), doc, fieldsVisitor);
                fetchSubPhase.hitExecute(context, hitContext);
            }
        }

        searchHit.shardTarget(context.shardTarget());
        exportFields.hit(searchHit);
        XContentBuilder builder = new XContentBuilder(XContentFactory.xContent(XContentType.JSON), out);
        exportFields.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.flush();
        out.write('\n');
        out.flush();
        numExported++;
    }

}
