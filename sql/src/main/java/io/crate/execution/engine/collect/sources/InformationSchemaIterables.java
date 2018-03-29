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

package io.crate.execution.engine.collect.sources;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import io.crate.execution.engine.collect.files.SqlFeatureContext;
import io.crate.execution.engine.collect.files.SqlFeaturesIterable;
import io.crate.expression.reference.information.ColumnContext;
import io.crate.expression.udf.UserDefinedFunctionsMetaData;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.IndexParts;
import io.crate.metadata.IngestionRuleInfo;
import io.crate.metadata.IngestionRuleInfos;
import io.crate.metadata.PartitionInfo;
import io.crate.metadata.PartitionInfos;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.RoutineInfo;
import io.crate.metadata.RoutineInfos;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.rule.ingest.IngestRulesMetaData;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.ConstraintInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.view.ViewInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyList;

public class InformationSchemaIterables implements ClusterStateListener {

    public static final String PK_SUFFIX = "_pk";

    private static final Set<String> IGNORED_SCHEMAS =
        ImmutableSet.of(InformationSchemaInfo.NAME, SysSchemaInfo.NAME, BlobSchemaInfo.NAME, PgCatalogSchemaInfo.NAME);

    private final Schemas schemas;
    private final FluentIterable<RelationInfo> tables;
    private final FluentIterable<ViewInfo> views;
    private final PartitionInfos partitionInfos;
    private final FluentIterable<ColumnContext> columns;
    private final FluentIterable<RelationInfo> primaryKeys;
    private final FluentIterable<ConstraintInfo> constraints;
    private final SqlFeaturesIterable sqlFeatures;
    private final Iterable<Void> referentialConstraints;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

    private Iterable<RoutineInfo> routines;
    private Iterable<IngestionRuleInfo> ingestionRules;
    private boolean initialClusterStateReceived = false;

    @Inject
    public InformationSchemaIterables(final Schemas schemas,
                                      FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                      ClusterService clusterService) throws IOException {
        this.schemas = schemas;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        tables = FluentIterable.from(schemas)
            .transformAndConcat(schema -> FluentIterable.from(schema.getTables())
                .filter(i -> !IndexParts.isPartitioned(i.ident().indexName())));

        views = FluentIterable.from(schemas)
            .transformAndConcat(schema -> FluentIterable.from(schema.getViews())
                .filter(i -> !IndexParts.isPartitioned(i.ident().indexName())));

        partitionInfos = new PartitionInfos(clusterService);
        columns = tables.append(views).transformAndConcat(ColumnsIterable::new);

        primaryKeys = tables
            .filter(i -> i != null && (i.primaryKey().size() > 1 ||
                                       (i.primaryKey().size() == 1 && !i.primaryKey().get(0).name().equals("_id"))));
        FluentIterable<ConstraintInfo> primaryKeyConstraints = primaryKeys
            .transform(t -> new ConstraintInfo(
                t.ident(),
                t.ident().name() + PK_SUFFIX,
                ConstraintInfo.Constraint.PRIMARY_KEY));
        FluentIterable<ConstraintInfo> notnullConstraints = tables.transformAndConcat(NotNullConstraintIterable::new);
        constraints = FluentIterable.concat(primaryKeyConstraints, notnullConstraints);

        sqlFeatures = new SqlFeaturesIterable();

        referentialConstraints = emptyList();
        // these are initialized on a clusterState change
        routines = emptyList();
        ingestionRules = emptyList();
        clusterService.addListener(this);
    }

    public Iterable<SchemaInfo> schemas() {
        return schemas;
    }

    public Iterable<RelationInfo> relations() {
        return FluentIterable.concat(tables, views);
    }

    public Iterable<ViewInfo> views() {
        return views;
    }

    public Iterable<PartitionInfo> partitions() {
        return partitionInfos;
    }

    public Iterable<ColumnContext> columns() {
        return columns;
    }

    public Iterable<ConstraintInfo> constraints() {
        return constraints;
    }

    public Iterable<RoutineInfo> routines() {
        return routines;
    }

    public Iterable<SqlFeatureContext> features() {
        return sqlFeatures;
    }

    public Iterable<IngestionRuleInfo> ingestionRules() {
        return ingestionRules;
    }

    public Iterable<KeyColumnUsage> keyColumnUsage() {
        return primaryKeys.stream()
            .filter(tableInfo -> !IGNORED_SCHEMAS.contains(tableInfo.ident().schema()))
            .flatMap(tableInfo -> {
                List<ColumnIdent> pks = tableInfo.primaryKey();
                PrimitiveIterator.OfInt ids = IntStream.range(1, pks.size() + 1).iterator();
                RelationName ident = tableInfo.ident();
                return pks.stream().map(
                    pk -> new KeyColumnUsage(ident, pk, ids.next()));
            })::iterator;
    }


    public Iterable<Void> referentialConstraintsInfos() {
        return referentialConstraints;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (initialClusterStateReceived) {
            Set<String> changedCustomMetaDataSet = event.changedCustomMetaDataSet();
            if (changedCustomMetaDataSet.contains(UserDefinedFunctionsMetaData.TYPE) == false &&
                changedCustomMetaDataSet.contains(IngestRulesMetaData.TYPE) == false) {
                return;
            }
            createMetaDataBasedIterables(event.state().getMetaData());
        } else {
            initialClusterStateReceived = true;
            createMetaDataBasedIterables(event.state().getMetaData());
        }
    }

    private void createMetaDataBasedIterables(MetaData metaData) {
        RoutineInfos routineInfos = new RoutineInfos(fulltextAnalyzerResolver,
            metaData.custom(UserDefinedFunctionsMetaData.TYPE));
        routines = FluentIterable.from(routineInfos).filter(Objects::nonNull);
        ingestionRules = new IngestionRuleInfos(metaData.custom(IngestRulesMetaData.TYPE));
    }

    /**
     * Iterable for extracting not null constraints from table info.
     */
    static class NotNullConstraintIterable implements Iterable<ConstraintInfo> {

        private final RelationInfo info;

        NotNullConstraintIterable(RelationInfo info) {
            this.info = info;
        }

        @Override
        public Iterator<ConstraintInfo> iterator() {
            return new NotNullConstraintIterator(info);
        }
    }

    /**
     * Iterator that returns ConstraintInfo for each TableInfo with not null constraints.
     */
    static class NotNullConstraintIterator implements Iterator<ConstraintInfo> {
        private final RelationInfo relationInfo;
        private final Iterator<Reference> nullableColumns;

        NotNullConstraintIterator(RelationInfo relationInfo) {
            this.relationInfo = relationInfo;
            nullableColumns = StreamSupport.stream(relationInfo.spliterator(), false)
                .filter(reference -> reference.column().isSystemColumn() == false &&
                                     reference.valueType() != DataTypes.NOT_SUPPORTED &&
                                     reference.isNullable() == false)
                .iterator();
        }

        @Override
        public boolean hasNext() {
            return nullableColumns.hasNext();
        }

        @Override
        public ConstraintInfo next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Not null constraint iterator exhausted");
            }

            // Building not null constraint name with following pattern:
            // {table_schema}_{table_name}_{column_name}_not_null
            // Currently the longest not null constraint of information_schema
            // is 56 characters long, that's why default string length is set to
            // 60.
            String constraintName = new StringBuilder(60)
                .append(this.relationInfo.ident().schema())
                .append("_")
                .append(this.relationInfo.ident().name())
                .append("_")
                .append(this.nullableColumns.next().column().name())
                .append("_not_null")
                .toString();

            // Return nullable columns instead.
            return new ConstraintInfo(
                this.relationInfo.ident(),
                constraintName,
                ConstraintInfo.Constraint.CHECK
            );
        }
    }

    static class ColumnsIterable implements Iterable<ColumnContext> {

        private final RelationInfo relationInfo;

        ColumnsIterable(RelationInfo relationInfo) {
            this.relationInfo = relationInfo;
        }

        @Override
        public Iterator<ColumnContext> iterator() {
            return new ColumnsIterator(relationInfo);
        }
    }

    static class ColumnsIterator implements Iterator<ColumnContext> {

        private final Iterator<Reference> columns;
        private final RelationInfo tableInfo;
        private Short ordinal = 1;

        ColumnsIterator(RelationInfo tableInfo) {
            columns = StreamSupport.stream(tableInfo.spliterator(), false)
                .filter(reference -> !reference.column().isSystemColumn()
                                     && reference.valueType() != DataTypes.NOT_SUPPORTED).iterator();
            this.tableInfo = tableInfo;
        }

        @Override
        public boolean hasNext() {
            return columns.hasNext();
        }

        @Override
        public ColumnContext next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Columns iterator exhausted");
            }
            Reference colRef = columns.next();
            if (colRef.column().isTopLevel() == false) {
                return new ColumnContext(tableInfo, colRef, null);
            }
            return new ColumnContext(tableInfo, colRef, ordinal++);
        }
    }

    public static class KeyColumnUsage {

        private final RelationName relationName;
        private final ColumnIdent pkColumnIdent;
        private final int ordinal;

        KeyColumnUsage(RelationName relationName, ColumnIdent pkColumnIdent, int ordinal) {
            this.relationName = relationName;
            this.pkColumnIdent = pkColumnIdent;
            this.ordinal = ordinal;
        }

        public String getSchema() {
            return relationName.schema();
        }

        public String getPkColumnName() {
            return pkColumnIdent.name();
        }

        public int getOrdinal() {
            return ordinal;
        }

        public String getTableName() {
            return relationName.name();
        }

        public String getFQN() {
            return relationName.fqn();
        }
    }
}
