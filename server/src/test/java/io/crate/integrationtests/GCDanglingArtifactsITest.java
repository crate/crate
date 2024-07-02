/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public class GCDanglingArtifactsITest extends IntegTestCase {


    @Test
    public void testAlterClusterGCDanglingIndicesRemovesDanglingIndices() {
        execute("create table doc.t1 (x int)");
        createIndex(".resized.foobar");

        ClusterService clusterService = cluster().getInstance(ClusterService.class);
        assertThat(clusterService.state().metadata().hasIndex(".resized.foobar")).isTrue();

        execute("alter cluster gc dangling artifacts");

        assertThat(clusterService.state().metadata().hasIndex(".resized.foobar")).isFalse();
        assertThat(clusterService.state().metadata().hasIndex("t1")).isTrue();
    }
}
