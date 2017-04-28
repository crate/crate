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

import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.data.Row;
import io.crate.operation.reference.sys.check.AbstractSysCheck;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

@Singleton
public class TablesNeedUpgradeSysCheck extends AbstractSysCheck {

    public static final int ID = 3;
    public static final String DESCRIPTION =
        "The following tables need to be upgraded for compatibility with future versions of CrateDB: ";

    private static final String STMT = "select schema_name || '.' || table_name, min_lucene_version " +
                                       "from sys.shards where min_lucene_version not like '" +
                                       Version.LATEST.major+ ".%.%' " +
                                       "order by 1";
    private static final int LIMIT = 50_000;
    private static final String PREP_STMT_NAME = "tables_need_upgrade_syscheck";

    private final Logger logger;
    private final Provider<SQLOperations> sqlOperationsProvider;
    private SQLOperations.SQLDirectExecutor sqlDirectExecutor;
    private volatile Collection<String> tablesNeedUpgrade;

    @Inject
    public TablesNeedUpgradeSysCheck(Provider<SQLOperations> sqlOperationsProvider, Settings settings) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.sqlOperationsProvider = sqlOperationsProvider;
        this.logger = Loggers.getLogger(TablesNeedUpgradeSysCheck.class, settings);
    }

    @Override
    public BytesRef description() {
        String linkedDescriptionBuilder = DESCRIPTION + tablesNeedUpgrade + ' ' + LINK_PATTERN + ID;
        return new BytesRef(linkedDescriptionBuilder);
    }

    @Override
    public CompletableFuture<?> computeResult() {
        final CompletableFuture<Collection<String>> result = new CompletableFuture<>();
        try {
            directExecutor().execute(new SycCheckResultReceiver(result), Collections.emptyList());
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
        return result.whenComplete((tableNames, throwable) -> {
            if (throwable == null) {
                tablesNeedUpgrade = tableNames;
            } else {
                logger.error("error while checking for tables that need upgrade", throwable);
            }
        });
    }

    private SQLOperations.SQLDirectExecutor directExecutor() {
        if (sqlDirectExecutor == null) {
            sqlDirectExecutor = sqlOperationsProvider.get().createSQLDirectExecutor(
                "sys", PREP_STMT_NAME, STMT, LIMIT);
        }
        return sqlDirectExecutor;
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
