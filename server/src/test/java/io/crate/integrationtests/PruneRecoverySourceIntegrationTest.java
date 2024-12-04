/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.integrationtests;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;
import java.util.function.IntFunction;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

@IntegTestCase.ClusterScope(minNumDataNodes = 3)
public class PruneRecoverySourceIntegrationTest extends IntegTestCase {

    private void indexWith(String statement, int docCount, IntFunction<Object[]> supplier) {
        for (int i = 0; i < docCount; i++) {
            execute(statement, supplier.apply(i));
        }
    }

    @Test
    public void testPruneRecoverySource() throws Exception {

        int docCount = 500;

        execute("create table tbl (x int) with (number_of_replicas=2)");
        indexWith("insert into tbl (x) values (?)", docCount, i -> new Object[]{ i });
        execute("refresh table tbl");
        execute("select count(*) from tbl");
        assertThat(response.rows()[0][0]).isEqualTo((long) docCount);

        execute("optimize table tbl with (max_num_segments = 1)");
        execute("refresh table tbl");
        assertThat(storedFieldCount("_recovery_source")).isLessThan(docCount);
        // generated ids don't get pruned
        assertThat(storedFieldCount("_id")).isEqualTo(docCount);
    }

    @Test
    public void testPruneId() throws Exception {

        int docCount = 500;

        execute("create table tbl (x int primary key) with (number_of_replicas=2)");
        indexWith("insert into tbl (x) values (?)", docCount, i -> new Object[]{ i });
        execute("refresh table tbl");
        execute("select count(*) from tbl");
        assertThat(response.rows()[0][0]).isEqualTo((long) docCount);

        execute("optimize table tbl with (max_num_segments = 1)");
        execute("refresh table tbl");
        assertThat(storedFieldCount("_recovery_source")).isLessThan(docCount);
        // ids that map to a single field stored elsewhere can be pruned
        assertThat(storedFieldCount("_id")).isLessThan(docCount);
    }

    @Test
    public void testMultiplePrimaryKeys() throws Exception {

        int docCount = 500;

        execute("create table tbl (x int primary key, y int primary key) with (number_of_replicas=2)");
        indexWith("insert into tbl (x, y) values (?, ?)", docCount, i -> new Object[]{ i, i + 1 });
        execute("refresh table tbl");
        execute("select count(*) from tbl");
        assertThat(response.rows()[0][0]).isEqualTo((long) docCount);

        execute("optimize table tbl with (max_num_segments = 1)");
        execute("refresh table tbl");
        assertThat(storedFieldCount("_recovery_source")).isLessThan(docCount);
        // We don't prune IDs if they don't directly map to a single other source
        assertThat(storedFieldCount("_id")).isEqualTo(docCount);

    }

    private int storedFieldCount(String field) throws IOException {
        ensureGreen();
        int[] fieldCount = new int[1];
        var indexesService = cluster().getDataNodeInstance(IndicesService.class);
        for (IndexService service : indexesService) {
            for (IndexShard shard : service) {
                try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                    var reader = searcher.getIndexReader();
                    var storedFields = reader.storedFields();
                    for (int doc = 0; doc < reader.maxDoc(); doc++) {
                        storedFields.document(doc, new StoredFieldVisitor() {
                            @Override
                            public Status needsField(FieldInfo fieldInfo) {
                                return Status.YES;
                            }

                            @Override
                            public void binaryField(FieldInfo fieldInfo, byte[] value) {
                                if (fieldInfo.name.equals(field)) {
                                    fieldCount[0]++;
                                }
                            }
                        });
                    }
                }
            }
        }
        return fieldCount[0];
    }

}
