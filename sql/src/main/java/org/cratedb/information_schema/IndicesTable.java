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

        for (IndexMetaData indexMetaData : clusterState.metaData().indices().values()) {
            IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(indexMetaData);
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
