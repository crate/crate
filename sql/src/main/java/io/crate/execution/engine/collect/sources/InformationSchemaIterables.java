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

import com.google.common.collect.ImmutableSet;
import io.crate.execution.engine.collect.files.SqlFeatureContext;
import io.crate.execution.engine.collect.files.SqlFeaturesIterable;
import io.crate.expression.reference.information.ColumnContext;
import io.crate.expression.udf.UserDefinedFunctionsMetaData;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexParts;
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
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.pgcatalog.PgClassTable;
import io.crate.metadata.pgcatalog.PgIndexTable;
import io.crate.metadata.pgcatalog.PgProcTable;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.ConstraintInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.stream.Stream.concat;
import static java.util.stream.StreamSupport.stream;

public class InformationSchemaIterables implements ClusterStateListener {

    public static final String PK_SUFFIX = "_pk";

    private static final Set<String> IGNORED_SCHEMAS =
        ImmutableSet.of(InformationSchemaInfo.NAME, SysSchemaInfo.NAME, BlobSchemaInfo.NAME, PgCatalogSchemaInfo.NAME);

    private final Schemas schemas;
    private final Iterable<RelationInfo> relations;
    private final Iterable<ViewInfo> views;
    private final PartitionInfos partitionInfos;
    private final Iterable<ColumnContext> columns;
    private final Iterable<RelationInfo> primaryKeys;
    private final Iterable<ConstraintInfo> constraints;
    private final SqlFeaturesIterable sqlFeatures;
    private final Iterable<Void> referentialConstraints;
    private final Iterable<PgIndexTable.Entry> pgIndices;
    private final Iterable<PgClassTable.Entry> pgClasses;
    private final Iterable<PgProcTable.Entry> pgBuiltInFunc;
    private final Functions functions;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

    private Iterable<RoutineInfo> routines;
    private boolean initialClusterStateReceived = false;

    @Inject
    public InformationSchemaIterables(final Schemas schemas,
                                      final Functions functions,
                                      FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                      ClusterService clusterService) throws IOException {
        this.schemas = schemas;
        this.functions = functions;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        views = () -> viewsStream(schemas).iterator();
        relations = () -> concat(tablesStream(schemas), viewsStream(schemas)).iterator();
        primaryKeys = () -> sequentialStream(relations)
            .filter(this::isPrimaryKey)
            .iterator();

        columns = () -> sequentialStream(relations)
            .flatMap(r -> sequentialStream(new ColumnsIterable(r)))
            .iterator();

        Iterable<ConstraintInfo> primaryKeyConstraints = () -> sequentialStream(primaryKeys)
            .map(t -> new ConstraintInfo(
                t,
                t.ident().name() + PK_SUFFIX,
                ConstraintInfo.Type.PRIMARY_KEY))
            .iterator();

        Iterable<ConstraintInfo> notnullConstraints = () -> sequentialStream(relations)
            .flatMap(r -> sequentialStream(new NotNullConstraintIterable(r)))
            .iterator();

        Iterable<ConstraintInfo> checkConstraints = () ->
            sequentialStream(relations)
                .flatMap(r -> r.checkConstraints()
                    .stream()
                    .map(chk -> new ConstraintInfo(r, chk.name(), ConstraintInfo.Type.CHECK)))
                .iterator();

        constraints = () -> Stream.of(sequentialStream(primaryKeyConstraints),
                                      sequentialStream(notnullConstraints),
                                      sequentialStream(checkConstraints))
            .flatMap(Function.identity())
            .iterator();

        partitionInfos = new PartitionInfos(clusterService);
        sqlFeatures = new SqlFeaturesIterable();

        referentialConstraints = emptyList();
        // these are initialized on a clusterState change
        routines = emptyList();
        clusterService.addListener(this);

        pgIndices = () -> tablesStream(schemas).filter(this::isPrimaryKey).map(this::pgIndex).iterator();
        pgClasses = () -> concat(sequentialStream(relations).map(this::relationToPgClassEntry),
                                 sequentialStream(primaryKeys).map(this::primaryKeyToPgClassEntry)).iterator();
        pgBuiltInFunc = () -> sequentialStream(functions.functionResolvers().keySet())
            .map(this::pgProc)
            .iterator();
    }

    private boolean isPrimaryKey(RelationInfo relationInfo) {
        return (relationInfo.primaryKey().size() > 1 ||
            (relationInfo.primaryKey().size() == 1 &&
            !relationInfo.primaryKey().get(0).name().equals("_id")));
    }

    private PgClassTable.Entry relationToPgClassEntry(RelationInfo info) {
        return new PgClassTable.Entry(
            OidHash.relationOid(info),
            OidHash.schemaOid(info.ident().schema()),
            info.ident(),
            info.ident().name(),
            toEntryType(info.relationType()),
            info.columns().size(),
            info.primaryKey().size() > 0);
    }

    private PgClassTable.Entry primaryKeyToPgClassEntry(RelationInfo info) {
        return new PgClassTable.Entry(
            OidHash.primaryKeyOid(info),
            OidHash.schemaOid(info.ident().schema()),
            info.ident(),
            info.ident().name() + "_pkey",
            PgClassTable.Entry.Type.INDEX,
            info.columns().size(),
            info.primaryKey().size() > 0);
    }

    private PgClassTable.Entry.Type toEntryType(RelationInfo.RelationType type) {
        switch (type) {
            case BASE_TABLE:
                return PgClassTable.Entry.Type.RELATION;
            case VIEW:
                return PgClassTable.Entry.Type.VIEW;
            default:
                return null;
        }
    }

    private PgIndexTable.Entry pgIndex(TableInfo tableInfo) {
        var primaryKey = tableInfo.primaryKey();
        var positions = new ArrayList<Integer>();
        for (var columnIdent : primaryKey) {
            var pkRef = tableInfo.getReference(columnIdent);
            assert pkRef != null : "`getReference(..)` must not return null for columns retrieved from `primaryKey()`";
            var position = pkRef.position();
            positions.add(position);
        }
        return new PgIndexTable.Entry(OidHash.relationOid(tableInfo), OidHash.primaryKeyOid(tableInfo), positions);
    }

    private PgProcTable.Entry pgProc(FunctionName functionName) {
        return new PgProcTable.Entry(
            OidHash.functionOid(functionName),
            functionName,
            OidHash.schemaOid(functionName.schema())
        );
    }

    private static Stream<ViewInfo> viewsStream(Schemas schemas) {
        return sequentialStream(schemas)
            .flatMap(schema -> sequentialStream(schema.getViews()))
            .filter(i -> !IndexParts.isPartitioned(i.ident().indexNameOrAlias()));
    }

    private static Stream<TableInfo> tablesStream(Schemas schemas) {
        return sequentialStream(schemas)
            .flatMap(s -> sequentialStream(s.getTables()))
            .filter(i -> !(IndexParts.isPartitioned(i.ident().indexNameOrAlias()) ||
                           IndexParts.isDangling(i.ident().indexNameOrAlias())));
    }

    private static <T> Stream<T> sequentialStream(Iterable<T> iterable) {
        return stream(iterable.spliterator(), false);
    }

    public Iterable<SchemaInfo> schemas() {
        return schemas;
    }

    public Iterable<RelationInfo> relations() {
        return relations;
    }

    public Iterable<PgIndexTable.Entry> pgIndices() {
        return pgIndices;
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

    public Iterable<PgClassTable.Entry> pgClasses() {
        return pgClasses;
    }

    public Iterable<PgProcTable.Entry> pgProc() {
        return () -> concat(
            sequentialStream(pgBuiltInFunc),
            sequentialStream(functions.udfFunctionResolvers().keySet()).map(this::pgProc)
        ).iterator();
    }

    public Iterable<KeyColumnUsage> keyColumnUsage() {
        return sequentialStream(primaryKeys)
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
            if (changedCustomMetaDataSet.contains(UserDefinedFunctionsMetaData.TYPE) == false) {
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
        routines = () -> sequentialStream(routineInfos).filter(Objects::nonNull).iterator();
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
            nullableColumns = stream(relationInfo.spliterator(), false)
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
                this.relationInfo,
                constraintName,
                ConstraintInfo.Type.CHECK
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
        private int ordinal = 1;

        ColumnsIterator(RelationInfo tableInfo) {
            columns = stream(tableInfo.spliterator(), false)
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
            Reference ref = columns.next();
            if (ref.column().isTopLevel() == false) {
                return new ColumnContext(tableInfo, ref, null);
            }

            // Tables created with CrateDB < 4.0 don't have any positional information stored inside the meta data so
            // we must fallback to old behaviour which uses a simple increased iterating based var.
            Integer position = ref.position();
            if (position == null) {
                position = ordinal;
            }
            ordinal++;
            return new ColumnContext(tableInfo, ref, position);
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

        public String getPkColumnIdent() {
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
