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

package io.crate.replication.logical.action;

import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.Publication;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.apache.logging.log4j.Level;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.MockLogAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.contains;

public class PublicationsStateActionTest extends CrateDummyClusterServiceUnitTest {

    private MockLogAppender appender;

    @Before
    public void appendLogger() throws Exception {
        appender = new MockLogAppender();
        appender.start();
        Loggers.addAppender(Loggers.getLogger(PublicationsStateAction.class), appender);

    }

    @After
    public void removeLogger() {
        Loggers.removeAppender(Loggers.getLogger(PublicationsStateAction.class), appender);
        appender.stop();
    }

    @Test
    public void test_resolve_relation_names_for_all_tables_ignores_table_with_soft_delete_disabled() throws Exception {
        var s = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t2 (id int) with (\"soft_deletes.enabled\" = false)")
            .build();
        var publication = new Publication("some_user", true, List.of());

        var expectedLogMessage = "Table 'doc.t2' won't be replicated as the required table setting " +
                                 "'soft_deletes.enabled' is set to: false";
        appender.addExpectation(new MockLogAppender.SeenEventExpectation(
            expectedLogMessage,
            Loggers.getLogger(PublicationsStateAction.class).getName(),
            Level.WARN,
            expectedLogMessage
        ));

        var resolvedRelations = PublicationsStateAction.TransportAction.resolveRelationsNames(
            publication,
            s.schemas()
        );
        assertThat(resolvedRelations, contains(new RelationName("doc", "t1")));
        appender.assertAllExpectationsMatched();
    }
}
