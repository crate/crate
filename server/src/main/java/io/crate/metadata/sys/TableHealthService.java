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

package io.crate.metadata.sys;

import com.google.common.annotations.VisibleForTesting;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.data.Row;
import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ShardedTable;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.rest.RestStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.concurrent.CompletableFuture.completedFuture;

@Singleton
public class TableHealthService {

    private static final Logger LOGGER = LogManager.getLogger(TableHealthService.class);

    private static final String STMT = "select table_name, schema_name, partition_ident, routing_state," +
                                       " \"primary\", relocating_node, count(*) from sys.shards " +
                                       " group by 1, 2, 3, 4, 5, 6";
    private static final Statement PARSED_STMT = SqlParser.createStatement(STMT);

    private final ClusterService clusterService;
    private final Schemas schemas;
    private final Provider<SQLOperations> sqlOperationsProvider;
    private Session session;

    @Inject
    public TableHealthService(ClusterService clusterService,
                              Schemas schemas,
                              Provider<SQLOperations> sqlOperationsProvider) {
        this.clusterService = clusterService;
        this.schemas = schemas;
        this.sqlOperationsProvider = sqlOperationsProvider;
    }

    public CompletableFuture<Iterable<TableHealth>> computeResults() {
        if (clusterService.localNode() == null) {
            /*
              During a long startup (e.g. during an upgrade process) the localNode() may be null
              and this would lead to NullPointerException in the TransportExecutor.
             */
            LOGGER.debug("Could not retrieve tables health information. localNode is not fully available yet.");
            return completedFuture(Collections.emptyList());
        }
        if (clusterService.state().getBlocks().hasGlobalBlockWithStatus(RestStatus.SERVICE_UNAVAILABLE)) {
            return completedFuture(allAsUnavailable());
        }
        try {
            CompletableFuture<Map<TablePartitionIdent, ShardsInfo>> future = new CompletableFuture<>();
            HealthResultReceiver resultReceiver = new HealthResultReceiver(future);
            session().quickExec(STMT, stmt -> PARSED_STMT, resultReceiver, Row.EMPTY);
            return future.thenApply(this::buildTablesHealth);
        } catch (Throwable t) {
            LOGGER.error("error retrieving tables health information", t);
            return completedFuture(allAsUnavailable());
        }
    }

    private Iterable<TableHealth> allAsUnavailable() {
        return StreamSupport.stream(schemas.spliterator(), false)
            .flatMap(schemaInfo -> StreamSupport.stream(schemaInfo.getTables().spliterator(), false))
            .flatMap(tableInfo -> {
                if (tableInfo instanceof DocTableInfo) {
                    return healthFromPartitions(tableInfo.ident(), ((DocTableInfo) tableInfo).partitions().stream());
                }
                RelationName ident = tableInfo.ident();
                TableHealth tableHealth = new TableHealth(
                    ident.name(), ident.schema(), null, TableHealth.Health.RED, -1, -1);
                return Stream.of(tableHealth);
            })::iterator;
    }

    private static Stream<TableHealth> healthFromPartitions(RelationName table, Stream<PartitionName> partitions) {
        String tableName = table.name();
        String tableSchema = table.schema();
        return partitions
            .map(pn -> new TableHealth(tableName, tableSchema, pn.ident(), TableHealth.Health.RED, -1, -1));
    }

    private Session session() {
        if (session == null) {
            session = sqlOperationsProvider.get().newSystemSession();
        }
        return session;
    }

    @VisibleForTesting
    Iterable<TableHealth> buildTablesHealth(Map<TablePartitionIdent, ShardsInfo> tables) {
        return () -> tables.entrySet().stream()
            .map(this::tableHealthFromEntry)
            .filter(Objects::nonNull)
            .iterator();
    }


    private TableHealth tableHealthFromEntry(Map.Entry<TablePartitionIdent, ShardsInfo> entry) {
        TablePartitionIdent ident = entry.getKey();
        ShardsInfo shardsInfo = entry.getValue();
        RelationName relationName = new RelationName(ident.tableSchema, ident.tableName);
        ShardedTable tableInfo;
        try {
            tableInfo = schemas.getTableInfo(relationName);
        } catch (RelationUnknown e) {
            return null;
        }
        return calculateHealth(ident, shardsInfo, tableInfo.numberOfShards());
    }

    @VisibleForTesting
    static TableHealth calculateHealth(TablePartitionIdent ident, ShardsInfo shardsInfo, int configuredShards) {
        long missing = Math.max(0, configuredShards - shardsInfo.activePrimaries);
        long underreplicated = Math.max(0, shardsInfo.unassigned + shardsInfo.replicating - missing);
        TableHealth.Health health = TableHealth.Health.GREEN;
        if (missing > 0) {
            health = TableHealth.Health.RED;
        } else if (underreplicated > 0) {
            health = TableHealth.Health.YELLOW;
        }
        return new TableHealth(
            ident.tableName,
            ident.tableSchema,
            ident.partitionIdent,
            health,
            missing,
            underreplicated
        );
    }

    private static boolean isActiveShard(String routingState) {
        return routingState.equalsIgnoreCase("STARTED") || routingState.equalsIgnoreCase("RELOCATING");
    }

    @VisibleForTesting
    static void collectShardInfo(ShardsInfo shardsInfo,
                                 String routingState,
                                 boolean primary,
                                 long shardCount,
                                 @Nullable String relocatingNode) {
        if (isActiveShard(routingState) && primary) {
            shardsInfo.activePrimaries += shardCount;
        } else if (routingState.equalsIgnoreCase("UNASSIGNED")) {
            shardsInfo.unassigned += shardCount;
        } else if (routingState.equalsIgnoreCase("INITIALIZING") && relocatingNode == null) {
            shardsInfo.replicating += shardCount;
        }
    }

    private static class HealthResultReceiver implements ResultReceiver {

        private static final Logger LOGGER = LogManager.getLogger(TableHealthService.HealthResultReceiver.class);

        private final CompletableFuture<Map<TablePartitionIdent, ShardsInfo>> result;
        private final Map<TablePartitionIdent, ShardsInfo> tables = new HashMap<>();

        HealthResultReceiver(CompletableFuture<Map<TablePartitionIdent, ShardsInfo>> result) {
            this.result = result;
        }

        @Override
        public void setNextRow(Row row) {
            TablePartitionIdent ident = new TablePartitionIdent(
                (String) row.get(0), (String) row.get(1), (String) row.get(2));
            ShardsInfo shardsInfo = tables.getOrDefault(ident, new ShardsInfo());
            String routingState = (String) row.get(3);
            boolean primary = (boolean) row.get(4);
            String relocatingNode = (String) row.get(5);
            long cnt = (long) row.get(6);

            collectShardInfo(shardsInfo, routingState, primary, cnt, relocatingNode);
            tables.put(ident, shardsInfo);
        }

        @Override
        public void allFinished(boolean interrupted) {
            result.complete(tables);
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            LOGGER.error("error retrieving tables health", t);
            result.completeExceptionally(t);
        }

        @Override
        public void batchFinished() {
        }

        @Override
        public CompletableFuture<?> completionFuture() {
            return result;
        }
    }

    @VisibleForTesting
    static class TablePartitionIdent {
        private final String tableName;
        private final String tableSchema;
        @Nullable
        private final String partitionIdent;

        TablePartitionIdent(String tableName, String tableSchema, @Nullable String partitionIdent) {
            this.tableName = tableName;
            this.tableSchema = tableSchema;
            this.partitionIdent = partitionIdent;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TablePartitionIdent that = (TablePartitionIdent) o;
            return Objects.equals(tableName, that.tableName) &&
                   Objects.equals(tableSchema, that.tableSchema) &&
                   Objects.equals(partitionIdent, that.partitionIdent);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableName, tableSchema, partitionIdent);
        }
    }

    @VisibleForTesting
    static class ShardsInfo {
        long activePrimaries = 0;
        long unassigned = 0;
        long replicating = 0;
    }
}
