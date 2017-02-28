/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.reference.sys.check.cluster;

import io.crate.action.sql.Option;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.data.Row;
import io.crate.metadata.Schemas;
import io.crate.operation.reference.sys.check.AbstractSysCheck;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Singleton
public class TablesNeedUpgradeSysCheck extends AbstractSysCheck {

    public static final int ID = 4;
    public static final String DESCRIPTION =
        "The following tables must be upgraded for compatibility with future versions of CrateDB: ";

    private static final ESLogger LOGGER = Loggers.getLogger(TablesNeedUpgradeSysCheck.class);
    private static final String STMT = "select schema_name || '.' || table_name, min_lucene_version " +
                                       "from sys.shards where min_lucene_version <> '" +
                                       org.apache.lucene.util.Version.LATEST.toString() + "' " +
                                       "order by 1";
    private static final int LIMIT = 50_000;

    private final SQLOperations.Session session;
    private final ClusterService clusterService;
    private final Schemas schemas;
    private volatile Collection<String> tablesNeedUpgrade;

    @Inject
    public TablesNeedUpgradeSysCheck(ClusterService clusterService, Schemas schemas, SQLOperations sqlOperations) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.clusterService = clusterService;
        this.schemas = schemas;
        this.session = sqlOperations.createSession("sys", Option.NONE, LIMIT);
        this.session.parse(SQLOperations.Session.UNNAMED, STMT, Collections.emptyList());
            sqlOperations.createSession(null, Option.NONE, 1);
    }

    @Override
    public BytesRef description() {
        String linkedDescriptionBuilder = DESCRIPTION + tablesNeedUpgrade + ' ' + LINK_PATTERN + ID;
        return new BytesRef(linkedDescriptionBuilder);
    }

    @Override
    public boolean validate() {
        Collection<String> tablesNeedReindexing = new ArrayList<>(LuceneVersionChecks.tablesNeedReindexing(
            schemas,
            clusterService.state().metaData()));

        final CompletableFuture<Collection<String>> result = new CompletableFuture<>();
        try {
            session.parse(SQLOperations.Session.UNNAMED, STMT, Collections.emptyList());
            session.bind(SQLOperations.Session.UNNAMED, SQLOperations.Session.UNNAMED, Collections.emptyList(), null);
            session.execute(
                SQLOperations.Session.UNNAMED,
                0,
                new SycCheckResultReceiver(result, tablesNeedReindexing));
            session.sync();
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
        try {
            tablesNeedUpgrade = result.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOGGER.error("error occurred when checking for tables that need to be upgraded", e);
        }
        return tablesNeedUpgrade.isEmpty();
    }

    private class SycCheckResultReceiver implements ResultReceiver {

        private final CompletableFuture<Collection<String>> result;
        private final Collection<String> tablesNeedReindexing;
        private final Collection<String> tables;

        private SycCheckResultReceiver(CompletableFuture<Collection<String>> result,
                                       Collection<String> tablesNeedReindexing) {
            this.result = result;
            this.tables = new HashSet<>();
            this.tablesNeedReindexing = tablesNeedReindexing;
        }

        @Override
        public void setNextRow(Row row) {
            String versionStr = ((BytesRef)row.get(1)).utf8ToString();
            String tableName = ((BytesRef)row.get(0)).utf8ToString();
            if (!tablesNeedReindexing.contains(tableName) && LuceneVersionChecks.checkUpgradeRequired(versionStr)) {
                tables.add(tableName);
            }
        }

        @Override
        public void batchFinished() {
        }

        @Override
        public void allFinished(boolean interrupted) {
            result.complete(tables);
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            result.completeExceptionally(t);
        }

        @Override
        public CompletableFuture<Collection<String>> completionFuture() {
            return result;
        }
    }
}
