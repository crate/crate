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
 * virtual information_schema table listing table constraints like primary_key constraints
 */
public class TableConstraintsTable extends AbstractInformationSchemaTable {

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
    public TableConstraintsTable(Map<String, AggFunction> aggFunctionMap,
            CacheRecycler cacheRecycler) {
        super(aggFunctionMap, cacheRecycler);

        fieldMapper.put(
                Columns.TABLE_NAME,
                new StringLuceneField(Columns.TABLE_NAME)
        );
        fieldMapper.put(
                Columns.CONSTRAINT_NAME,
                new StringLuceneField(Columns.CONSTRAINT_NAME)
        );
        fieldMapper.put(
                Columns.CONSTRAINT_TYPE,
                new StringLuceneField(Columns.CONSTRAINT_TYPE)
        );
    }

    @Override
    public void doIndex(ClusterState clusterState) throws IOException {
        StringField tableName = new StringField(Columns.TABLE_NAME, "", Field.Store.YES);
        StringField constraintName = new StringField(Columns.CONSTRAINT_NAME, "", Field.Store.YES);
        StringField constraintType = new StringField(Columns.CONSTRAINT_TYPE, "", Field.Store.YES);

        for (IndexMetaData indexMetaData : clusterState.metaData().indices().values()) {
            IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(indexMetaData);
            // ignore closed indices
            if (extractor.isIndexClosed()) {
                continue;
            }

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
