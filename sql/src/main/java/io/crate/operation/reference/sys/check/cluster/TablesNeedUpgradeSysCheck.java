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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

@Singleton
public class TablesNeedUpgradeSysCheck extends AbstractSysCheck {

    public static final int ID = 4;
    public static final String DESCRIPTION =
        "The following tables must be upgraded for compatibility with future versions of CrateDB: ";

    private static final ESLogger LOGGER = Loggers.getLogger(TablesNeedUpgradeSysCheck.class);
    private static final String STMT = "select schema_name || '.' || table_name, min_lucene_version " +
                                       "from sys.shards where min_lucene_version <> '" +
                                       org.apache.lucene.util.Version.LATEST.toString() + "' " +
                                       "AND (? = [] OR schema_name || '.' || table_name != ANY (?)) " +
                                       "order by 1";
    private static final int LIMIT = 50_000;
    private static final String PREP_STMT_NAME = "tables_need_recrate_syscheck";

    private final SQLOperations.Session session;
    private final ClusterService clusterService;
    private volatile Collection<String> tablesNeedUpgrade;

    @Inject
    public TablesNeedUpgradeSysCheck(ClusterService clusterService, SQLOperations sqlOperations) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.clusterService = clusterService;
        this.session = sqlOperations.createSession("sys", Option.NONE, LIMIT);
        this.session.parse(PREP_STMT_NAME, STMT, Collections.emptyList());
    }

    @Override
    public BytesRef description() {
        String linkedDescriptionBuilder = DESCRIPTION + tablesNeedUpgrade + ' ' + LINK_PATTERN + ID;
        return new BytesRef(linkedDescriptionBuilder);
    }

    @Override
    public CompletableFuture<?> computeResult() {
        Collection<String> tablesNeedRecreation =
            LuceneVersionChecks.tablesNeedRecreation(clusterService.state().metaData());

        final CompletableFuture<Collection<String>> result = new CompletableFuture<>();
        try {
            Object[] sqlParam = tablesNeedRecreation.toArray(new String[]{});
            session.bind(PREP_STMT_NAME, PREP_STMT_NAME, Arrays.asList(sqlParam, sqlParam),null);
            session.execute(PREP_STMT_NAME, 0, new SycCheckResultReceiver(result));
            session.sync().whenComplete((o, throwable) -> session.close((byte) 'P', PREP_STMT_NAME));
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
        return result.whenComplete((tableNames, throwable) -> {
            if (throwable == null) {
                tablesNeedUpgrade = tableNames;
            } else {
                LOGGER.error("error while checking for tables that need upgrade", throwable);
            }
        });
    }

    @Override
    public boolean validate() {
        return tablesNeedUpgrade == null || tablesNeedUpgrade.isEmpty();
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
            if (LuceneVersionChecks.isUpgradeRequired(((BytesRef) row.get(1)).utf8ToString())) {
                tables.add(((BytesRef) row.get(0)).utf8ToString());
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
