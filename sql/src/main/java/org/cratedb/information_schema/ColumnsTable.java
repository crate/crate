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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.index.ColumnDefinition;
import org.cratedb.index.IndexMetaDataExtractor;
import org.cratedb.lucene.fields.IntegerLuceneField;
import org.cratedb.lucene.fields.StringLuceneField;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;

import java.io.IOException;
import java.util.Map;

/**
 * virtual information_schema table listing table columns definition
 */
public class ColumnsTable extends AbstractInformationSchemaTable {

    public static final String NAME = "columns";

    public class Columns {
        public static final String TABLE_NAME = "table_name";
        public static final String COLUMN_NAME = "column_name";
        public static final String ORDINAL_POSITION = "ordinal_position";
        public static final String DATA_TYPE = "data_type";
    }

    StringField tableNameField = new StringField(Columns.TABLE_NAME, "", Field.Store.YES);
    StringField columnNameField = new StringField(Columns.COLUMN_NAME, "", Field.Store.YES);
    IntField ordinalPositionField = new IntField(Columns.ORDINAL_POSITION, 0, Field.Store.YES);
    StringField dataTypeField = new StringField(Columns.DATA_TYPE, "", Field.Store.YES);

    @Inject
    public ColumnsTable(Map<String, AggFunction> aggFunctionMap, CacheRecycler cacheRecycler) {
        super(aggFunctionMap, cacheRecycler);
        fieldMapper.put(
                Columns.TABLE_NAME,
                new StringLuceneField(Columns.TABLE_NAME)
        );
        fieldMapper.put(
                Columns.COLUMN_NAME,
                new StringLuceneField(Columns.COLUMN_NAME)
        );
        fieldMapper.put(
                Columns.ORDINAL_POSITION,
                new IntegerLuceneField(Columns.ORDINAL_POSITION)
        );
        fieldMapper.put(
                Columns.DATA_TYPE,
                new StringLuceneField(Columns.DATA_TYPE)
        );
    }

    @Override
    public void doIndex(ClusterState clusterState) throws IOException {

        for (ObjectCursor<IndexMetaData> cursor : clusterState.metaData().indices().values()) {
            IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(cursor.value);
            // ignore closed indices
            if (extractor.isIndexClosed()) {
                continue;
            }

            for (ColumnDefinition columnDefinition : extractor.getColumnDefinitions()) {
                if (columnDefinition.isSupported()) {
                    addColumnDocument(columnDefinition);
                }
            }
        }
    }

    private void addColumnDocument(ColumnDefinition columnDefinition) throws IOException {
        Document doc = new Document();
        tableNameField.setStringValue(columnDefinition.tableName);
        doc.add(tableNameField);

        columnNameField.setStringValue(columnDefinition.columnName);
        doc.add(columnNameField);

        ordinalPositionField.setIntValue(columnDefinition.ordinalPosition);
        doc.add(ordinalPositionField);

        dataTypeField.setStringValue(columnDefinition.dataType.getName());
        doc.add(dataTypeField);

        indexWriter.addDocument(doc);
    }
}
