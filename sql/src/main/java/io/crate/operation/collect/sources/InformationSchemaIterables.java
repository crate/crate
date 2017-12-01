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

package io.crate.operation.collect.sources;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.IndexParts;
import io.crate.metadata.IngestionRuleInfo;
import io.crate.metadata.IngestionRuleInfos;
import io.crate.metadata.PartitionInfo;
import io.crate.metadata.PartitionInfos;
import io.crate.metadata.Reference;
import io.crate.metadata.RoutineInfo;
import io.crate.metadata.RoutineInfos;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.rule.ingest.IngestRulesMetaData;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.ConstraintInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.collect.files.SqlFeatureContext;
import io.crate.operation.collect.files.SqlFeaturesIterable;
import io.crate.operation.reference.information.ColumnContext;
import io.crate.operation.udf.UserDefinedFunctionsMetaData;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class InformationSchemaIterables implements ClusterStateListener {

    public static final String PK_SUFFIX = "_pk";

    private static final Set<String> IGNORED_SCHEMAS =
        ImmutableSet.of(InformationSchemaInfo.NAME, SysSchemaInfo.NAME, BlobSchemaInfo.NAME, PgCatalogSchemaInfo.NAME);

    private final Schemas schemas;
    private final FluentIterable<TableInfo> tablesIterable;
    private final PartitionInfos partitionInfos;
    private final FluentIterable<ColumnContext> columnsIterable;
    private final FluentIterable<TableInfo> primaryKeyTableInfos;
    private final FluentIterable<ConstraintInfo> constraints;
    private final SqlFeaturesIterable sqlFeatures;
    private final FluentIterable<Void> referentialConstraints;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

    private FluentIterable<RoutineInfo> routines;
    private Iterable<IngestionRuleInfo> ingestionRules;

    @Inject
    public InformationSchemaIterables(final Schemas schemas,
                                      FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                      ClusterService clusterService) throws IOException {
        this.schemas = schemas;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        tablesIterable = FluentIterable.from(schemas)
            .transformAndConcat(schema -> FluentIterable.from(schema)
                .filter(i -> !IndexParts.isPartitioned(i.ident().indexName())));
        partitionInfos = new PartitionInfos(clusterService);
        columnsIterable = tablesIterable.transformAndConcat(ColumnsIterable::new);

        primaryKeyTableInfos = tablesIterable
            .filter(i -> i != null && (i.primaryKey().size() > 1 ||
                                       (i.primaryKey().size() == 1 && !i.primaryKey().get(0).name().equals("_id"))));
        FluentIterable<ConstraintInfo> primaryKeyConstraints = primaryKeyTableInfos
            .transform(t -> new ConstraintInfo(
                t.ident(),
                t.ident().name() + PK_SUFFIX,
                ConstraintInfo.Constraint.PRIMARY_KEY));
        FluentIterable<ConstraintInfo> notnullConstraints = tablesIterable.transformAndConcat(NotNullConstraintIterable::new);
        constraints = FluentIterable.concat(primaryKeyConstraints, notnullConstraints);

        sqlFeatures = new SqlFeaturesIterable();

        referentialConstraints = FluentIterable.from(Collections.emptyList());

        createMetaDataBasedIterables(clusterService.state().getMetaData());
        clusterService.addListener(this);
    }

    public Iterable<SchemaInfo> schemas() {
        return schemas;
    }

    public Iterable<TableInfo> tables() {
        return tablesIterable;
    }

    public Iterable<PartitionInfo> partitions() {
        return partitionInfos;
    }

    public Iterable<ColumnContext> columns() {
        return columnsIterable;
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
        return primaryKeyTableInfos.stream()
            .filter(tableInfo -> !IGNORED_SCHEMAS.contains(tableInfo.ident().schema()))
            .flatMap(tableInfo -> {
                List<ColumnIdent> pks = tableInfo.primaryKey();
                PrimitiveIterator.OfInt ids = IntStream.range(1, pks.size() + 1).iterator();
                TableIdent ident = tableInfo.ident();
                return pks.stream().map(
                    pk -> new KeyColumnUsage(ident, pk, ids.next()));
            })::iterator;
    }


    public Iterable<Void> referentialConstraintsInfos() {
        return referentialConstraints;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        Set<String> changedCustomMetaDataSet = event.changedCustomMetaDataSet();
        if (changedCustomMetaDataSet.contains(UserDefinedFunctionsMetaData.TYPE) == false &&
            changedCustomMetaDataSet.contains(IngestRulesMetaData.TYPE) == false) {
            return;
        }
        createMetaDataBasedIterables(event.state().getMetaData());
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

        private final TableInfo ti;

        NotNullConstraintIterable(TableInfo ti) {
            this.ti = ti;
        }

        @Override
        public Iterator<ConstraintInfo> iterator() {
            return new NotNullConstraintIterator(ti);
        }
    }

    /**
     * Iterator that returns ConstraintInfo for each TableInfo with not null constraints.
     */
    static class NotNullConstraintIterator implements Iterator<ConstraintInfo> {
        private final TableInfo tableInfo;
        private final Iterator<Reference> nullableColumns;

        NotNullConstraintIterator(TableInfo tableInfo) {
            this.tableInfo = tableInfo;
            nullableColumns = StreamSupport.stream(tableInfo.spliterator(), false)
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
                .append(this.tableInfo.ident().schema())
                .append("_")
                .append(this.tableInfo.ident().name())
                .append("_")
                .append(this.nullableColumns.next().column().name())
                .append("_not_null")
                .toString();

            // Return nullable columns instead.
            return new ConstraintInfo(
                this.tableInfo.ident(),
                constraintName,
                ConstraintInfo.Constraint.CHECK
            );
        }
    }

    static class ColumnsIterable implements Iterable<ColumnContext> {

        private final TableInfo ti;

        ColumnsIterable(TableInfo ti) {
            this.ti = ti;
        }

        @Override
        public Iterator<ColumnContext> iterator() {
            return new ColumnsIterator(ti);
        }
    }

    static class ColumnsIterator implements Iterator<ColumnContext> {

        private final Iterator<Reference> columns;
        private final TableInfo tableInfo;
        private Short ordinal = 1;

        ColumnsIterator(TableInfo tableInfo) {
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

        private final TableIdent tableIdent;
        private final ColumnIdent pkColumnIdent;
        private final int ordinal;

        KeyColumnUsage(TableIdent tableIdent, ColumnIdent pkColumnIdent, int ordinal) {
            this.tableIdent = tableIdent;
            this.pkColumnIdent = pkColumnIdent;
            this.ordinal = ordinal;
        }

        public String getSchema() {
            return tableIdent.schema();
        }

        public String getPkColumnName() {
            return pkColumnIdent.name();
        }

        public int getOrdinal() {
            return ordinal;
        }

        public String getTableName() {
            return tableIdent.name();
        }

        public String getFQN() {
            return tableIdent.fqn();
        }
    }
}
