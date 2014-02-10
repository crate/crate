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
import org.cratedb.index.IndexMetaDataExtractor;
import org.cratedb.lucene.fields.IntegerLuceneField;
import org.cratedb.lucene.fields.StringLuceneField;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;

import java.io.IOException;
import java.util.Map;

public class TablesTable extends AbstractInformationSchemaTable {

    public static final String NAME = "tables";

    @Inject
    public TablesTable(Map<String, AggFunction> aggFunctionMap, CacheRecycler cacheRecycler) {
        super(aggFunctionMap, cacheRecycler);
        fieldMapper.put(Columns.TABLE_NAME,
                new StringLuceneField(Columns.TABLE_NAME));
        fieldMapper.put(Columns.NUMBER_OF_SHARDS,
                new IntegerLuceneField(Columns.NUMBER_OF_SHARDS));
        fieldMapper.put(Columns.NUMBER_OF_REPLICAS,
                new IntegerLuceneField(Columns.NUMBER_OF_REPLICAS));
        fieldMapper.put(Columns.ROUTING_COLUMN,
                new StringLuceneField(Columns.ROUTING_COLUMN));

    }

    public class Columns {
        public static final String TABLE_NAME = "table_name";
        public static final String NUMBER_OF_SHARDS = "number_of_shards";
        public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
        public static final String ROUTING_COLUMN = "routing_column";
    }

    @Override
    public void doIndex(ClusterState clusterState) throws IOException {

        // according to http://wiki.apache.org/lucene-java/ImproveIndexingSpeed
        // re-using fields is faster than re-creating the field inside the loop
        StringField tableName = new StringField(TablesTable.Columns.TABLE_NAME, "", Field.Store.YES);
        IntField numberOfShards = new IntField(TablesTable.Columns.NUMBER_OF_SHARDS, 0, Field.Store.YES);
        IntField numberOfReplicas = new IntField(TablesTable.Columns.NUMBER_OF_REPLICAS, 0, Field.Store.YES);
        StringField routingColumn = new StringField(Columns.ROUTING_COLUMN, "", Field.Store.YES);

        for (ObjectCursor<IndexMetaData> cursor : clusterState.metaData().indices().values()) {
            IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(cursor.value);

            // ignore closed indices
            if (extractor.isIndexClosed()) {
                continue;
            }
            Document doc = new Document();
            tableName.setStringValue(extractor.getIndexName());
            doc.add(tableName);

            numberOfShards.setIntValue(extractor.getNumberOfShards());
            doc.add(numberOfShards);

            numberOfReplicas.setIntValue(extractor.getNumberOfReplicas());
            doc.add(numberOfReplicas);

            // routing column

            String routingColumnName = extractor.getRoutingColumn();
            routingColumn.setStringValue(routingColumnName);
            doc.add(routingColumn);

            indexWriter.addDocument(doc);
        }

    }
}
