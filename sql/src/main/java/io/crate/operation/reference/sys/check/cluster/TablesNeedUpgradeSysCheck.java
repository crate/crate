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
import io.crate.operation.reference.sys.check.AbstractSysCheck;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
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
                                       org.apache.lucene.util.Version.LATEST.toString() + "'";
    private static final String STMT_NOT_IN = " AND schema_name || '.' || table_name NOT IN (";
    private static final String STMT_SUFFIX = " order by 1";
    private static final int LIMIT = 50_000;

    private final SQLOperations.Session session;
    private final ClusterService clusterService;
    private volatile Collection<String> tablesNeedUpgrade;

    @Inject
    public TablesNeedUpgradeSysCheck(ClusterService clusterService, SQLOperations sqlOperations) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.clusterService = clusterService;
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
        Collection<String> tablesNeedReindexing =
            LuceneVersionChecks.tablesNeedReindexing(clusterService.state().metaData());
        String statement = createSQLStatement(tablesNeedReindexing);

        final CompletableFuture<Collection<String>> result = new CompletableFuture<>();
        try {
            session.parse(SQLOperations.Session.UNNAMED, statement, Collections.emptyList());
            session.bind(
                SQLOperations.Session.UNNAMED,
                SQLOperations.Session.UNNAMED,
                Collections.emptyList(),
                null);
            session.execute(
                SQLOperations.Session.UNNAMED,
                0,
                new SycCheckResultReceiver(result));
            session.sync();
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
        try {
            tablesNeedUpgrade = result.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOGGER.error("error occurred when checking for tables that need to be upgraded", e);
        }
        return tablesNeedUpgrade == null || tablesNeedUpgrade.isEmpty();
    }

    private static String createSQLStatement(Collection<String> tablesNeedReindexing) {
        String statement;
        if (tablesNeedReindexing.isEmpty()) {
            statement = STMT + STMT_SUFFIX;
        } else {
            StringBuilder sb = new StringBuilder(STMT + STMT_NOT_IN);
            int i = 0;
            for (String tableName : tablesNeedReindexing) {
                sb.append("'").append(tableName).append("'");
                if (i < tablesNeedReindexing.size() - 1) {
                    sb.append(",");
                }
                i++;
            }
            statement = sb.toString();
        }
        return statement;
    }

    private class SycCheckResultReceiver implements ResultReceiver {

        private final CompletableFuture<Collection<String>> result;
        private final Collection<String> tables;

        private SycCheckResultReceiver(CompletableFuture<Collection<String>> result) {
            this.result = result;
            this.tables = new HashSet<>();
        }

        @Override
        public void setNextRow(Row row) {
            // Row[0] = table_name, Row[1] = min_lucene_version
            if (LuceneVersionChecks.isUpgradeRequired(((BytesRef)row.get(1)).utf8ToString())) {
                tables.add(((BytesRef)row.get(0)).utf8ToString());
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
