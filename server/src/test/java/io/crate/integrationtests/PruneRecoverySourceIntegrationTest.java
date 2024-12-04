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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

@IntegTestCase.ClusterScope(minNumDataNodes = 3)
public class PruneRecoverySourceIntegrationTest extends IntegTestCase {

    @Test
    public void testPruneRecoverySource() throws IOException {
        execute("create table tbl (x int) with (number_of_replicas=2)");
        for (int i = 0; i < 500; i++) {
            execute("insert into tbl (x) values (" + i + ")");
        }
        execute("optimize table tbl with (max_num_segments = 1)");
        execute("refresh table tbl");

        int docCount = 0;
        int[] recoverySourceCount = new int[1];

        var indexesService = cluster().getDataNodeInstance(IndicesService.class);
        for (IndexService service : indexesService) {
            try (Engine.Searcher searcher = service.getShard(0).acquireSearcher("test")) {
                var reader = searcher.getIndexReader();
                var storedFields = reader.storedFields();
                for (int doc = 0; doc < reader.maxDoc(); doc++) {
                    docCount++;
                    storedFields.document(doc, new StoredFieldVisitor() {
                        @Override
                        public Status needsField(FieldInfo fieldInfo) throws IOException {
                            return Status.YES;
                        }

                        @Override
                        public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
                            if (fieldInfo.name.equals("_recovery_source")) {
                                recoverySourceCount[0]++;
                            }
                        }
                    });
                }
            }
        }

        assertThat(docCount).isGreaterThan(recoverySourceCount[0]);
    }

}
