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

package io.crate.analyze;

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.elasticsearch.common.UUIDs;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.ddl.GeoSettingsApplier;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.annotations.NotThreadSafe;
import io.crate.common.collections.Lists;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.GeoReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.planner.operators.EnsureNoMatchPredicate;
import io.crate.sql.tree.AddColumnDefinition;
import io.crate.sql.tree.AlterTableAddColumn;
import io.crate.sql.tree.CheckColumnConstraint;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnStorageDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.DefaultConstraint;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GeneratedExpressionConstraint;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.IndexColumnConstraint;
import io.crate.sql.tree.IndexDefinition;
import io.crate.sql.tree.NotNullColumnConstraint;
import io.crate.sql.tree.NullColumnConstraint;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.sql.tree.PartitionedBy;
import io.crate.sql.tree.PrimaryKeyColumnConstraint;
import io.crate.sql.tree.PrimaryKeyConstraint;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.GeoShapeType;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;

@NotThreadSafe
public class TableElementsAnalyzer implements FieldProvider<Reference> {

    public static final Set<Integer> UNSUPPORTED_INDEX_TYPE_IDS = Set.of(
        ObjectType.ID,
        DataTypes.GEO_POINT.id(),
        DataTypes.GEO_SHAPE.id()
    );
    public static final Set<Integer> UNSUPPORTED_PK_TYPE_IDS = Set.of(
        ObjectType.ID,
        DataTypes.GEO_POINT.id(),
        DataTypes.GEO_SHAPE.id()
    );

    public static final String COLUMN_STORE_PROPERTY = "columnstore";

    private final RelationName tableName;
    private final ExpressionAnalyzer expressionAnalyzer;
    private final ExpressionAnalysisContext expressionContext;
    /**
     * Columns are in the order as they appear in the statement
     */
    private final Map<ColumnIdent, RefBuilder> columns = new LinkedHashMap<>();
    private final PeekColumns peekColumns;
    private final ColumnAnalyzer columnAnalyzer;
    private final Function<Expression, Symbol> toSymbol;
    private final Set<ColumnIdent> primaryKeys = new HashSet<>();
    private final Map<String, AnalyzedCheck> checks = new LinkedHashMap<>();

    /**
     * Only set in ALTER TABLE
     */
    @Nullable
    private final DocTableInfo table;
    /**
     * Indicates if the expressionAnalyzer is allowed to return a Reference for
     * columns neither present in {@link #columns} (populated with peekColumns) nor
     * in {@link #table}
     */
    private boolean resolveMissing = false;
    private final EvaluatingNormalizer normalizer;
    private final CoordinatorTxnCtx txnCtx;

    public TableElementsAnalyzer(RelationName tableName,
                                 CoordinatorTxnCtx txnCtx,
                                 NodeContext nodeCtx,
                                 ParamTypeHints paramTypeHints) {
        this(null, tableName, txnCtx, nodeCtx, paramTypeHints);
    }

    public TableElementsAnalyzer(DocTableInfo table,
                                 CoordinatorTxnCtx txnCtx,
                                 NodeContext nodeCtx,
                                 ParamTypeHints paramTypeHints) {
        this(table, table.ident(), txnCtx, nodeCtx, paramTypeHints);
    }

    private TableElementsAnalyzer(@Nullable DocTableInfo table,
                                 RelationName tableName,
                                 CoordinatorTxnCtx txnCtx,
                                 NodeContext nodeCtx,
                                 ParamTypeHints paramTypeHints) {
        this.table = table;
        this.tableName = tableName;
        this.txnCtx = txnCtx;
        this.normalizer = EvaluatingNormalizer.functionOnlyNormalizer(nodeCtx);
        this.expressionAnalyzer = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            paramTypeHints,
            this,
            null
        );
        this.expressionContext = new ExpressionAnalysisContext(txnCtx.sessionSettings());
        this.columnAnalyzer = new ColumnAnalyzer();
        this.peekColumns = new PeekColumns();
        this.toSymbol = x -> expressionAnalyzer.convert(x, expressionContext);
    }

    static class RefBuilder {

        private final ColumnIdent name;
        private DataType<?> type;

        private ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC;
        private IndexType indexType = IndexType.PLAIN;
        private RowGranularity rowGranularity = RowGranularity.DOC;
        private String indexMethod;
        private boolean nullable = true;
        private GenericProperties<Symbol> indexProperties = GenericProperties.empty();
        private boolean primaryKey;
        @Nullable
        private String pkConstraintName;
        private boolean explicitNullable;
        private Symbol generated;
        private Symbol defaultExpression;
        private GenericProperties<Symbol> storageProperties = GenericProperties.empty();
        private List<Symbol> indexSources = List.of();


        /**
         * cached result
         **/
        private Reference builtReference;

        public RefBuilder(ColumnIdent name, DataType<?> type) {
            this.name = name;
            this.type = type;
        }

        @Nullable
        public String pkConstraintName() {
            return pkConstraintName;
        }

        public boolean isPrimaryKey() {
            return primaryKey;
        }

        public boolean isExplicitlyNull() {
            return explicitNullable;
        }

        public Reference build(Map<ColumnIdent, RefBuilder> columns,
                               RelationName tableName,
                               UnaryOperator<Symbol> bindParameter,
                               Function<Symbol, Object> toValue) {
            if (builtReference != null) {
                return builtReference;
            }

            StorageSupport<?> storageSupport = type.storageSupportSafe();
            Symbol columnStoreSymbol = storageProperties.get(COLUMN_STORE_PROPERTY);
            if (!storageSupport.supportsDocValuesOff() && columnStoreSymbol != null) {
                throw new IllegalArgumentException("Invalid storage option \"columnstore\" for data type \"" + type.getName() + "\" for column: " + name);
            }
            boolean hasDocValues = columnStoreSymbol == null
                ? storageSupport.getComputedDocValuesDefault(indexType)
                : DataTypes.BOOLEAN.implicitCast(toValue.apply(columnStoreSymbol));
            Reference ref;
            ReferenceIdent refIdent = new ReferenceIdent(tableName, name);
            int position = -1;


            if (defaultExpression != null) {
                defaultExpression = bindParameter.apply(defaultExpression);
            }

            if (!indexSources.isEmpty() || indexType == IndexType.FULLTEXT || indexProperties.properties().containsKey("analyzer")) {
                List<Reference> sources = new ArrayList<>(indexSources.size());
                for (Symbol indexSource : indexSources) {
                    if (!ArrayType.unnest(indexSource.valueType()).equals(DataTypes.STRING)) {
                        throw new IllegalArgumentException(String.format(
                            Locale.ENGLISH,
                            "INDEX source columns require `string` types. Cannot use `%s` (%s) as source for `%s`",
                            Symbols.pathFromSymbol(indexSource),
                            indexSource.valueType().getName(),
                            name
                            ));
                    }
                    Reference source = (Reference) RefReplacer.replaceRefs(bindParameter.apply(indexSource), x -> {
                        if (x instanceof DynamicReference) {
                            RefBuilder column = columns.get(x.column());
                            return column.build(columns, tableName, bindParameter, toValue);
                        }
                        return x;
                    });
                    if (Reference.indexOf(sources, source.column()) > -1) {
                        throw new IllegalArgumentException("Index " + name + " contains duplicate columns: " + sources);
                    }
                    sources.add(source);
                }
                String analyzer = DataTypes.STRING.sanitizeValue(indexProperties.map(toValue).get("analyzer"));
                ref = new IndexReference(
                    refIdent,
                    rowGranularity,
                    type,
                    columnPolicy,
                    indexType,
                    nullable,
                    hasDocValues,
                    position,
                    COLUMN_OID_UNASSIGNED,
                    false,
                    defaultExpression,
                    sources,
                    analyzer == null ? (indexType == IndexType.PLAIN ? "keyword" : "standard") : analyzer
                );
            } else if (ArrayType.unnest(type).id() == GeoShapeType.ID) {
                Map<String, Object> geoMap = new HashMap<>();
                GeoSettingsApplier.applySettings(geoMap, indexProperties.map(toValue), indexMethod);
                Float distError = (Float) geoMap.get("distance_error_pct");
                ref = new GeoReference(
                    refIdent,
                    type,
                    columnPolicy,
                    indexType,
                    nullable,
                    position,
                    COLUMN_OID_UNASSIGNED,
                    false,
                    defaultExpression,
                    indexMethod,
                    (String) geoMap.get("precision"),
                    (Integer) geoMap.get("tree_levels"),
                    distError == null ? null : distError.doubleValue()
                );
            } else {
                ref = new SimpleReference(
                    refIdent,
                    rowGranularity,
                    type,
                    columnPolicy,
                    indexType,
                    nullable,
                    hasDocValues,
                    position,
                    COLUMN_OID_UNASSIGNED,
                    false,
                    defaultExpression
                );
            }
            if (generated != null) {
                generated = RefReplacer.replaceRefs(bindParameter.apply(generated), x -> {
                    if (x instanceof DynamicReference dynamicRef) {
                        RefBuilder column = columns.get(dynamicRef.column());
                        x = column.build(columns, tableName, bindParameter, toValue);
                    }
                    if (x instanceof GeneratedReference) {
                        throw new ColumnValidationException(name.sqlFqn(), tableName, "Generated column cannot be based on generated column `" + x.column() + "`");
                    }
                    return x;
                });
                ref = new GeneratedReference(ref, generated);
            }

            builtReference = ref;
            return ref;
        }

        public void visitSymbols(Consumer<? super Symbol> consumer) {
            if (defaultExpression != null) {
                consumer.accept(defaultExpression);
            }
            if (generated != null) {
                consumer.accept(generated);
            }
            indexProperties.properties().values().forEach(consumer);
            storageProperties.properties().values().forEach(consumer);
        }
    }

    @Override
    public Reference resolveField(QualifiedName qualifiedName,
                                  @Nullable List<String> path,
                                  Operation operation,
                                  boolean errorOnUnknownObjectKey) {
        var columnIdent = ColumnIdent.fromNameSafe(qualifiedName, path);
        if (table != null) {
            Reference reference = table.getReference(columnIdent);
            if (reference != null) {
                return reference;
            }
        }
        var columnBuilder = columns.get(columnIdent);
        var dynamicReference = new DynamicReference(new ReferenceIdent(tableName, columnIdent), RowGranularity.DOC, -1);
        if (columnBuilder == null) {
            if (resolveMissing) {
                return dynamicReference;
            }
            throw new ColumnUnknownException(columnIdent, tableName);
        }
        dynamicReference.valueType(columnBuilder.type);
        return dynamicReference;
    }

    public AnalyzedCreateTable analyze(CreateTable<Expression> createTable) {
        for (var tableElement : createTable.tableElements()) {
            tableElement.accept(peekColumns, null);
        }
        for (var tableElement : createTable.tableElements()) {
            tableElement.accept(columnAnalyzer, null);
        }
        GenericProperties<Symbol> properties = createTable.properties().map(toSymbol);
        Optional<ClusteredBy<Symbol>> clusteredBy = createTable.clusteredBy().map(x -> x.map(toSymbol));

        Optional<PartitionedBy<Symbol>> partitionedBy = createTable.partitionedBy().map(x -> x.map(toSymbol));
        partitionedBy.ifPresent(p -> p.columns().forEach(partitionColumn -> {
            ColumnIdent partitionColumnIdent = Symbols.pathFromSymbol(partitionColumn);
            RefBuilder column = columns.get(partitionColumnIdent);
            if (column == null) {
                throw new ColumnUnknownException(partitionColumnIdent, tableName);
            }
            ensureValidPartitionColumn(clusteredBy, partitionColumnIdent, column);
            column.indexType = IndexType.NONE;
            column.rowGranularity = RowGranularity.PARTITION;
        }));

        return new AnalyzedCreateTable(
            tableName,
            createTable.ifNotExists(),
            columns,
            checks,
            properties,
            partitionedBy,
            clusteredBy
        );
    }

    public AnalyzedAlterTableAddColumn analyze(AlterTableAddColumn<Expression> alterTable) {
        assert table != null : "Must use CTOR that sets the DocTableInfo instance";
        List<AddColumnDefinition<Expression>> tableElements = alterTable.tableElements();
        for (var addColumnDefinition : tableElements) {
            addColumnDefinition.accept(peekColumns, null);
        }
        for (var addColumnDefinition : tableElements) {
            addColumnDefinition.accept(columnAnalyzer, null);
        }
        return new AnalyzedAlterTableAddColumn(table, columns, checks);
    }

    private void ensureValidPartitionColumn(Optional<ClusteredBy<Symbol>> clusteredBy, ColumnIdent partitionColumnIdent, RefBuilder column) {
        if (partitionColumnIdent.isSystemColumn()) {
            throw new IllegalArgumentException("Cannot use system column `" + partitionColumnIdent + "` in PARTITIONED BY clause");
        }
        if (!primaryKeys.isEmpty() && !primaryKeys.contains(partitionColumnIdent)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot use non primary key column '%s' in PARTITIONED BY clause if primary key is set on table",
                partitionColumnIdent.sqlFqn()));
        }
        if (!DataTypes.isPrimitive(column.type)) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Cannot use column %s of type %s in PARTITIONED BY clause",
                partitionColumnIdent.sqlFqn(),
                column.type
            ));
        }
        for (ColumnIdent parent : partitionColumnIdent.parents()) {
            RefBuilder parentColumn = columns.get(parent);
            if (parentColumn.type instanceof ArrayType<?>) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Cannot use array column %s in PARTITIONED BY clause", partitionColumnIdent.sqlFqn()));
            }
        }
        if (column.indexType == IndexType.FULLTEXT) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot use column %s with fulltext index in PARTITIONED BY clause",
                partitionColumnIdent.sqlFqn()
            ));
        }
        clusteredBy.flatMap(ClusteredBy::column).ifPresent(clusteredBySymbol -> {
            ColumnIdent clusteredByColumnIdent = Symbols.pathFromSymbol(clusteredBySymbol);
            if (partitionColumnIdent.equals(clusteredByColumnIdent)) {
                throw new IllegalArgumentException("Cannot use CLUSTERED BY column `" + clusteredByColumnIdent + "` in PARTITIONED BY clause");
            }
        });
    }

    class PeekColumns extends DefaultTraversalVisitor<Void, Void> {

        @Override
        @SuppressWarnings("unchecked")
        public Void visitColumnDefinition(ColumnDefinition<?> node, Void context) {
            ColumnDefinition<Expression> columnDefinition = (ColumnDefinition<Expression>) node;
            ColumnType<Expression> type = columnDefinition.type();
            ColumnIdent columnName = ColumnIdent.fromNameSafe(columnDefinition.ident(), List.of());
            DataType<?> dataType = type == null ? DataTypes.UNDEFINED : DataTypeAnalyzer.convert(type);

            addColumn(columnName, dataType);
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitAddColumnDefinition(AddColumnDefinition<?> node, Void context) {
            assert table != null : "Must use CTOR that sets the DocTableInfo instance for ALTER TABLE ADD COLUMN";
            AddColumnDefinition<Expression> columnDefinition = (AddColumnDefinition<Expression>) node;
            Expression name = columnDefinition.name();
            resolveMissing = true;
            Symbol columnSymbol = expressionAnalyzer.convert(name, expressionContext);
            resolveMissing = false;
            ColumnIdent columnName = Symbols.pathFromSymbol(columnSymbol);
            for (ColumnIdent parent : columnName.parents()) {
                Reference parentRef = table.getReference(parent);
                if (parentRef != null) {
                    RefBuilder parentBuilder = new RefBuilder(parent, parentRef.valueType());
                    parentBuilder.builtReference = parentRef;
                    columns.put(parent, parentBuilder);
                } else {
                    columns.put(parent, new RefBuilder(parent, ObjectType.UNTYPED));
                }
            }
            Reference reference = table.getReference(columnName);
            if (reference != null) {
                throw new IllegalArgumentException("The table " + tableName + " already has a column named " + columnName);
            }
            ColumnType<Expression> type = columnDefinition.type();
            DataType<?> dataType = type == null ? DataTypes.UNDEFINED : DataTypeAnalyzer.convert(type);
            addColumn(columnName, dataType);
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitIndexDefinition(IndexDefinition<?> node, Void context) {
            IndexDefinition<Expression> indexDefinition = (IndexDefinition<Expression>) node;
            String name = indexDefinition.ident();
            ColumnIdent columnName = ColumnIdent.fromNameSafe(name, List.of());
            addColumn(columnName, DataTypes.STRING);
            return null;
        }

        private void addColumn(ColumnIdent columnName, DataType<?> dataType) {
            RefBuilder builder = new RefBuilder(columnName, dataType);
            RefBuilder exists = columns.put(columnName, builder);
            if (exists != null) {
                throw new IllegalArgumentException("column \"" + columnName.sqlFqn() + "\" specified more than once");
            }
            if (table != null) {
                builder.builtReference = table.getReference(columnName);
            }
            while (dataType instanceof ArrayType<?> arrayType) {
                dataType = arrayType.innerType();
            }
            if (dataType instanceof ObjectType objectType) {
                for (var entry : objectType.innerTypes().entrySet()) {
                    String childName = entry.getKey();
                    ColumnIdent childColumn = ColumnIdent.getChildSafe(columnName, childName);
                    DataType<?> childType = entry.getValue();
                    addColumn(childColumn, childType);
                }
            }
        }
    }

    class ColumnAnalyzer extends DefaultTraversalVisitor<Void, ColumnIdent> {

        @Override
        @SuppressWarnings("unchecked")
        public Void visitColumnDefinition(ColumnDefinition<?> node, ColumnIdent parent) {
            ColumnDefinition<Expression> columnDefinition = (ColumnDefinition<Expression>) node;
            ColumnIdent columnName = parent == null
                ? new ColumnIdent(columnDefinition.ident())
                : ColumnIdent.getChildSafe(parent, columnDefinition.ident());
            RefBuilder builder = columns.get(columnName);

            for (var constraint : columnDefinition.constraints()) {
                processConstraint(builder, constraint);
            }

            ColumnType<Expression> type = columnDefinition.type();
            while (type instanceof CollectionColumnType collectionColumnType) {
                type = collectionColumnType.innerType();
            }
            if (type instanceof ObjectColumnType<Expression> objectColumnType) {
                builder.columnPolicy = objectColumnType.columnPolicy().orElse(ColumnPolicy.DYNAMIC);
                for (ColumnDefinition<Expression> nestedColumn : objectColumnType.nestedColumns()) {
                    nestedColumn.accept(this, columnName);
                }
            }

            return null;
        }

        private void processConstraint(RefBuilder builder, ColumnConstraint<Expression> constraint) {
            ColumnIdent columnName = builder.name;
            if (constraint instanceof CheckColumnConstraint<Expression> checkConstraint) {
                resolveMissing = true;
                Symbol checkSymbol = expressionAnalyzer.convert(checkConstraint.expression(), expressionContext);
                addCheck(checkConstraint.name(), checkConstraint.expressionStr(), checkSymbol, columnName);
                resolveMissing = false;
            } else if (constraint instanceof ColumnStorageDefinition<Expression> storageDefinition) {
                GenericProperties<Symbol> storageProperties = storageDefinition.properties().map(toSymbol);
                for (String storageProperty : storageProperties.keys()) {
                    if (!COLUMN_STORE_PROPERTY.equals(storageProperty)) {
                        throw new IllegalArgumentException("Invalid STORAGE WITH option `" + storageProperty + "` for column `" + columnName.sqlFqn() + "`");
                    }
                }
                builder.storageProperties = storageProperties;
            } else if (constraint instanceof IndexColumnConstraint<Expression> indexConstraint) {
                builder.indexMethod = indexConstraint.indexMethod();
                builder.indexProperties = indexConstraint.properties().map(toSymbol);
                builder.indexType = IndexType.of(builder.indexMethod);
                if (builder.indexType == IndexType.FULLTEXT && !DataTypes.STRING.equals(ArrayType.unnest(builder.type))) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "Can't use an Analyzer on column %s because analyzers are only allowed on " +
                        "columns of type \"%s\" of the unbound length limit.",
                        columnName.sqlFqn(),
                        DataTypes.STRING.getName()
                    ));
                }
                if (builder.indexType != IndexType.PLAIN && UNSUPPORTED_INDEX_TYPE_IDS.contains(builder.type.id())) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "INDEX constraint cannot be used on columns of type \"%s\": `%s`", builder.type, columnName));
                }
            } else if (constraint instanceof NotNullColumnConstraint<Expression>) {
                builder.nullable = false;
                if (builder.explicitNullable) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Column \"%s\" is declared NULL, therefore, cannot be declared NOT NULL", columnName));
                }
            } else if (constraint instanceof PrimaryKeyColumnConstraint<Expression> primaryKeyColumnConstraint) {
                markAsPrimaryKey(builder, primaryKeyColumnConstraint.constraintName());
            } else if (constraint instanceof NullColumnConstraint<Expression>) {
                builder.explicitNullable = true;
                if (builder.primaryKey) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Column \"%s\" is declared as PRIMARY KEY, therefore, cannot be declared NULL", columnName));
                }
                if (!builder.nullable) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Column \"%s\" is declared as NOT NULL, therefore, cannot be declared NULL", columnName));
                }
            } else if (constraint instanceof DefaultConstraint<Expression> defaultConstraint) {
                Expression defaultExpression = defaultConstraint.expression();
                if (defaultExpression != null) {
                    if (builder.type.id() == ObjectType.ID) {
                        throw new IllegalArgumentException("Default values are not allowed for object columns: " + columnName);
                    }
                    Symbol defaultSymbol = expressionAnalyzer.convert(defaultExpression, expressionContext);
                    builder.defaultExpression = defaultSymbol.cast(builder.type, CastMode.IMPLICIT);
                    // only used to validate; result is not used to preserve functions like `current_timestamp`
                    normalizer.normalize(builder.defaultExpression, txnCtx);
                    RefVisitor.visitRefs(builder.defaultExpression, x -> {
                        throw new UnsupportedOperationException(
                            "Cannot reference columns in DEFAULT expression of `" + columnName + "`. " +
                                "Maybe you wanted to use a string literal with single quotes instead: '" + x.column().name() + "'");
                    });
                    EnsureNoMatchPredicate.ensureNoMatchPredicate(defaultSymbol, "Cannot use MATCH in CREATE TABLE statements");
                }
            } else if (constraint instanceof GeneratedExpressionConstraint<Expression> generatedExpressionConstraint) {
                Expression generatedExpression = generatedExpressionConstraint.expression();
                if (generatedExpression != null) {
                    builder.generated = expressionAnalyzer.convert(generatedExpression, expressionContext);
                    EnsureNoMatchPredicate.ensureNoMatchPredicate(builder.generated, "Cannot use MATCH in CREATE TABLE statements");
                    if (builder.type == DataTypes.UNDEFINED) {
                        builder.type = builder.generated.valueType();
                    } else {
                        builder.generated = builder.generated.cast(builder.type, CastMode.IMPLICIT);
                    }
                    // only used to validate; result is not used to preserve functions like `current_timestamp`
                    normalizer.normalize(builder.generated, txnCtx);
                }
            } else {
                throw new UnsupportedOperationException("constraint not supported: " + constraint);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitAddColumnDefinition(AddColumnDefinition<?> node, ColumnIdent parent) {
            assert parent == null : "ADD COLUMN doesn't allow parents";
            AddColumnDefinition<Expression> columnDefinition = (AddColumnDefinition<Expression>) node;
            Expression name = columnDefinition.name();
            ColumnIdent columnName = Symbols.pathFromSymbol(expressionAnalyzer.convert(name, expressionContext));
            RefBuilder builder = columns.get(columnName);

            for (var constraint : columnDefinition.constraints()) {
                processConstraint(builder, constraint);
            }

            ColumnType<Expression> type = columnDefinition.type();
            while (type instanceof CollectionColumnType<Expression> collectionColumnType) {
                type = collectionColumnType.innerType();
            }
            if (type instanceof ObjectColumnType<Expression> objectColumnType) {
                builder.columnPolicy = objectColumnType.columnPolicy().orElse(ColumnPolicy.DYNAMIC);
                for (ColumnDefinition<Expression> nestedColumn : objectColumnType.nestedColumns()) {
                    nestedColumn.accept(this, columnName);
                }
            }
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitPrimaryKeyConstraint(PrimaryKeyConstraint<?> node, ColumnIdent parent) {
            PrimaryKeyConstraint<Expression> pkConstraint = (PrimaryKeyConstraint<Expression>) node;
            List<Expression> pkColumns = pkConstraint.columns();

            for (Expression pk : pkColumns) {
                Symbol pkColumn = toSymbol.apply(pk);
                ColumnIdent columnIdent = Symbols.pathFromSymbol(pkColumn);
                RefBuilder column = columns.get(columnIdent);
                if (column == null) {
                    throw new ColumnUnknownException(columnIdent, tableName);
                }
                markAsPrimaryKey(column, pkConstraint.constraintName());
            }

            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitIndexDefinition(IndexDefinition<?> node, ColumnIdent parent) {
            IndexDefinition<Expression> indexDefinition = (IndexDefinition<Expression>) node;
            String name = indexDefinition.ident();
            ColumnIdent columnIdent = parent == null ? new ColumnIdent(name) : ColumnIdent.getChildSafe(parent, name);
            RefBuilder builder = columns.get(columnIdent);
            builder.indexMethod = indexDefinition.method();
            builder.indexProperties = indexDefinition.properties().map(toSymbol);
            builder.indexSources = Lists.map(indexDefinition.columns(), toSymbol);
            builder.indexType = IndexType.of(builder.indexMethod);
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitCheckConstraint(CheckConstraint<?> node, ColumnIdent parent) {
            CheckConstraint<Expression> checkConstraint = (CheckConstraint<Expression>) node;
            Symbol checkSymbol = toSymbol.apply(checkConstraint.expression());
            addCheck(checkConstraint.name(), checkConstraint.expressionStr(), checkSymbol, null);
            return null;
        }
    }

    private void addCheck(@Nullable String constraintName, String expression, Symbol expressionSymbol, @Nullable ColumnIdent column) {
        if (constraintName == null) {
            do {
                constraintName = genUniqueConstraintName(tableName, column);
            } while (checks.containsKey(constraintName));
        }
        var analyzedCheck = new AnalyzedCheck(expression, expressionSymbol, null);
        if (column != null) {
            RefVisitor.visitRefs(expressionSymbol, ref -> {
                if (!ref.column().equals(column)) {
                    throw new UnsupportedOperationException(
                        "CHECK constraint on column `" + column + "` cannot refer to column `" + ref.column() +
                        "`. Use full path to refer to a sub-column or a table check constraint instead");
                }
            });
        }
        AnalyzedCheck exists = checks.put(constraintName, analyzedCheck);
        if (exists != null) {
            throw new IllegalArgumentException(
                "a check constraint of the same name is already declared [" + constraintName + "]");
        }
    }

    private static String genUniqueConstraintName(RelationName table, ColumnIdent column) {
        StringBuilder sb = new StringBuilder(table.fqn().replace(".", "_"));
        if (column != null) {
            sb.append("_").append(column.fqn().replace(".", "_"));
        }
        sb.append("_check_");
        String uuid = UUIDs.dirtyUUID().toString();
        int idx = uuid.lastIndexOf("-");
        sb.append(idx > 0 ? uuid.substring(idx + 1) : uuid);
        return sb.toString();
    }

    private void markAsPrimaryKey(RefBuilder column, @Nullable String pkConstraintName) {
        if (column.explicitNullable) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Column \"%s\" is declared NULL, therefore, cannot be declared as a PRIMARY KEY", column.name));
        }
        column.pkConstraintName = pkConstraintName;
        column.primaryKey = true;
        ColumnIdent columnName = column.name;
        DataType<?> type = column.type;
        if (type instanceof ArrayType) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Cannot use column \"%s\" with type \"%s\" as primary key", columnName.sqlFqn(), type));
        }
        if (UNSUPPORTED_PK_TYPE_IDS.contains(type.id())) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Cannot use columns of type \"%s\" as primary key", type));
        }
        for (ColumnIdent parent : columnName.parents()) {
            RefBuilder parentColumn = columns.get(parent);
            if (parentColumn.type instanceof ArrayType<?>) {
                throw new UnsupportedOperationException(
                    String.format(Locale.ENGLISH, "Cannot use column \"%s\" as primary key within an array object", columnName.leafName()));
            }
        }
        boolean wasNew = primaryKeys.add(column.name);
        if (!wasNew) {
            throw new IllegalArgumentException("Columns `" + column.name + "` appears twice in primary key constraint");
        }
    }
}
