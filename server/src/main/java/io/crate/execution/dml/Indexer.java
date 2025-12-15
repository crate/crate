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


package io.crate.execution.dml;

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.collections.Maps;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.InputFactory.Context;
import io.crate.expression.reference.Doc;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.ScopedRef;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.UnscopedRef;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

/**
 * <p>
 *  Component to create a {@link ParsedDocument} from a {@link IndexItem}
 * </p>
 *
 * <p>Takes care of:</p>
 * <ul>
 *  <li>Validation of constraints (NOT NULL, CHECK)</li>
 *  <li>Adds DEFAULT values for _missing_ columns</li>
 *  <li>Adds GENERATED values</li>
 *  <li>Source generation</li>
 *  <li>Lucene {@link Document} and index field creation</li>
 * </ul>
 * <p>
 * This process is split into multiple steps:
 * <ol>
 * <li>Look for unknown columns via {@link #collectSchemaUpdates(IndexItem)}</li>
 * <li>Update cluster state with new columns (The user of the Indexer is
 * responsible for this)</li>
 * <li>Update reference information based on cluster state update</li>
 * <li>Create {@link ParsedDocument} via {@link #index(IndexItem)}</li>
 * </ol>
 *
 * Schema update and creation of ParsedDocument is split into two steps to be
 * able to use information from the persisted {@link ScopedRef} from the cluster
 * state. (Mostly for the OID information)
 **/
public class Indexer {

    private final List<ValueIndexer<?>> valueIndexers;
    /**
     * target columns for INSERT and target columns for UPDATE converted to INSERT
     */
    private final List<ScopedRef> columns;
    private final Map<ColumnIdent, Synthetic> synthetics;
    private final List<CollectExpression<IndexItem, Object>> expressions;
    private final Map<ColumnIdent, ColumnConstraint> columnConstraints = new HashMap<>();
    private final List<TableConstraint> tableConstraints;
    private final List<IndexColumn<Input<?>>> indexColumns;
    private final List<Input<?>> returnValueInputs;
    private final List<Synthetic> undeterministic = new ArrayList<>();
    private final Function<ColumnIdent, ScopedRef> getRef;
    private final boolean writeOids;
    private final Version tableVersionCreated;
    @Nullable
    private final UpdateToInsert updateToInsert;
    private final Indexer onConflictIndexer;

    /// Indexing order for the columns;
    /// Starts out as [rootColumns][DocTableInfo#rootColumns()] but is updated
    /// after dynamic column additions;
    private List<ScopedRef> indexOrder;


    public record IndexColumn<I>(ScopedRef reference, List<? extends I> inputs) {
    }

    static class RefResolver implements ReferenceResolver<CollectExpression<IndexItem, Object>> {

        private final List<String> partitionValues;
        private final List<ScopedRef> targetColumns;
        private final DocTableInfo table;
        private final SymbolEvaluator symbolEval;

        private RefResolver(SymbolEvaluator symbolEval,
                            List<String> partitionValues,
                            List<ScopedRef> targetColumns,
                            DocTableInfo table) {
            this.symbolEval = symbolEval;
            this.partitionValues = partitionValues;
            this.targetColumns = targetColumns;
            this.table = table;
        }

        @Override
        public CollectExpression<IndexItem, Object> getImplementation(ScopedRef ref) {

            // Here be dragons: A reference can refer to:
            //
            //  - To seqNo or primaryTerm of the IndexItem
            //
            //  - To a primary key that was already generated when resolving the routing.
            //    It must be part of `pkValues()` of the IndexItem
            //
            //  - value in the IndexItem insertValues.
            //    Example `INSERT INTO (x, y) VALUES (1, 2)
            //                          │             ▲
            //                          └─────────────┘
            //
            //  - Can be used in PARITITONED BY, in which case it's part of the partition name
            //
            //  - Can be missing from targetColumns / insertValues but have a DEFAULT clause
            //
            //  - Can be a child column, where the root is part of the targetColumns / insertValues

            ColumnIdent column = ref.column();
            if (column.equals(SysColumns.ID.COLUMN)) {
                return NestableCollectExpression.forFunction(IndexItem::id);
            } else if (column.equals(SysColumns.SEQ_NO)) {
                return NestableCollectExpression.forFunction(IndexItem::seqNo);
            } else if (column.equals(SysColumns.PRIMARY_TERM)) {
                return NestableCollectExpression.forFunction(IndexItem::primaryTerm);
            }
            int pkIndex = table.primaryKey().indexOf(column);
            if (pkIndex > -1) {
                return NestableCollectExpression.forFunction(item -> {
                    // create table t (a int generated always as c+1, b int, c int primary key);
                    // insert into t(c) values (0);
                    // update t set b=-1;
                    // when replicating the UPDATE query, 'a' needs to resolve 'c' which is PK and is not available in
                    // item.pkValues(), fall back to search targetColumns.
                    if (pkIndex < item.pkValues().size()) {
                        return ref.valueType().implicitCast(item.pkValues().get(pkIndex));
                    } else {
                        return item.insertValues()[targetColumns.indexOf(ref)];
                    }
                });
            }
            int index = targetColumns.indexOf(ref);
            if (index > -1) {
                return NestableCollectExpression.forFunction(item -> item.insertValues()[index]);
            }
            if (ref.granularity() == RowGranularity.PARTITION) {
                int pIndex = table.partitionedByColumns().indexOf(ref);
                if (pIndex > -1) {
                    String val = partitionValues.get(pIndex);
                    return NestableCollectExpression.constant(ref.valueType().implicitCast(val));
                } else {
                    return NestableCollectExpression.constant(null);
                }
            }
            if (column.isRoot()) {
                Symbol defaultExpression = ref.defaultExpression();
                if (defaultExpression == null) {
                    if (ref instanceof GeneratedReference generated) {
                        return NestableCollectExpression.forFunction(
                            item -> fromGenerated(generated, item)
                        );
                    }
                    return NestableCollectExpression.constant(null);
                }
                return NestableCollectExpression.constant(
                    defaultExpression.accept(symbolEval, Row.EMPTY).value()
                );
            }
            ColumnIdent root = column.getRoot();
            int rootIdx = -1;
            for (int i = 0; i < targetColumns.size(); i++) {
                if (targetColumns.get(i).column().equals(root)) {
                    rootIdx = i;
                    break;
                }
            }
            final int rootIndex = rootIdx;
            if (rootIndex == -1) {
                return NestableCollectExpression.constant(null);
            }
            Function<IndexItem, Object> getValue = item -> {
                Object val = item.insertValues()[rootIndex];
                if (val instanceof Map<?, ?> m) {
                    List<String> path = column.path();
                    val = Maps.getByPath(m, path);
                }
                if (val == null) {
                    Symbol defaultExpression = ref.defaultExpression();
                    if (defaultExpression != null) {
                        val = defaultExpression.accept(symbolEval, Row.EMPTY).value();
                    } else if (ref instanceof GeneratedReference generated) {
                        return fromGenerated(generated, item);
                    }
                }
                return val;
            };
            return NestableCollectExpression.forFunction(getValue);
        }

        private Object fromGenerated(GeneratedReference generated, IndexItem item) {
            Symbol generatedExpression = RefReplacer.replaceRefs(generated.generatedExpression(), x -> {
                var collectExpression = getImplementation(x);
                collectExpression.setNextRow(item);
                return Literal.ofUnchecked(x.valueType(), collectExpression.value());
            });
            Input<?> accept = generatedExpression.accept(symbolEval, Row.EMPTY);
            return accept.value();
        }
    }

    /**
     * For DEFAULT expressions or GENERATED columns.
     *
     * <p>Computed values are stored and reused per-row for:</p>
     * <ul>
     *  <li>Sending values of non-deterministic functions to replica</li>
     *  <li>Computing RETURNING expression, referring to GENERATED or DEFAULT columns.</li>
     * </ul>
     *
     * Cached value must be cleared per-row.
     **/
    private static class Synthetic implements Input<Object> {

        private final ScopedRef ref;
        private final Input<?> input;
        private final ValueIndexer<Object> indexer;
        private boolean computed;
        private Object computedValue;

        public Synthetic(ScopedRef ref,
                         Input<?> input,
                         ValueIndexer<Object> indexer) {
            this.ref = ref;
            this.input = input;
            this.indexer = indexer;
        }

        public ValueIndexer<Object> indexer() {
            return indexer;
        }

        @Override
        public Object value() {
            if (!computed) {
                computedValue = input.value();
                computed = true;
            }
            return computedValue;
        }

        public void reset() {
            this.computed = false;
            this.computedValue = null;
        }

        public boolean isComputed() {
            return this.computed;
        }
    }

    public interface ColumnConstraint {

        void verify(Object providedValue);
    }

    interface TableConstraint {

        void verify(Object[] values);
    }

    record NotNullTableConstraint(ColumnIdent column, Input<?> input) implements TableConstraint {

        @Override
        public void verify(Object[] values) {
            Object value = input.value();
            if (value == null) {
                throw new IllegalArgumentException("\"" + column + "\" must not be null");
            }
        }
    }

    record TableCheckConstraint(
            Input<?> input,
            CheckConstraint<Symbol> checkConstraint) implements TableConstraint {

        @Override
        public void verify(Object[] values) {
            Object value = input.value();
            // SQL semantics: If a column is omitted from an INSERT/UPDATE statement,
            // CHECK constraints should not fail. Same for writing explicit `null` values.
            if (value instanceof Boolean bool) {
                if (!bool) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "Failed CONSTRAINT %s CHECK (%s) for values: %s",
                        checkConstraint.name(),
                        checkConstraint.expressionStr(),
                        Arrays.toString(values)));
                }
            }
        }
    }

    record MultiCheck(List<ColumnConstraint> checks) implements ColumnConstraint {

        @Override
        public void verify(Object providedValue) {
            for (var check : checks) {
                check.verify(providedValue);
            }
        }

        public static ColumnConstraint merge(ColumnConstraint check1, ColumnConstraint check2) {
            if (check1 instanceof MultiCheck multiCheck) {
                multiCheck.checks.add(check2);
                return check1;
            }
            ArrayList<ColumnConstraint> checks = new ArrayList<>(2);
            checks.add(check1);
            checks.add(check2);
            return new MultiCheck(checks);
        }
    }

    record NotNull(ColumnIdent column) implements ColumnConstraint {

        public void verify(Object providedValue) {
            if (providedValue == null) {
                throw new IllegalArgumentException("\"" + column + "\" must not be null");
            }
        }
    }

    record CheckGeneratedValue(Input<?> input, GeneratedReference ref) implements ColumnConstraint {

        @SuppressWarnings("unchecked")
        @Override
        public void verify(Object providedValue) {
            DataType<Object> valueType = (DataType<Object>) ref.valueType();
            Object generatedValue = input.value();
            int compare = Comparator
                .nullsFirst(valueType)
                .compare(
                    valueType.sanitizeValue(generatedValue),
                    valueType.sanitizeValue(providedValue)
                );
            if (compare != 0) {
                throw new IllegalArgumentException(
                    "Given value " + providedValue +
                    " for generated column " + ref.column() +
                    " does not match calculation " + ref.formattedGeneratedExpression() + " = " +
                    generatedValue
                );
            }
        }
    }

    /**
     *
     */
    public Indexer(List<String> partitionValues,
                   DocTableInfo table,
                   Version shardVersionCreated,
                   TransactionContext txnCtx,
                   NodeContext nodeCtx,
                   List<ScopedRef> targetColumns,
                   @Nullable String[] updateColumns,
                   @Nullable Symbol[] returnValues) {
        this(
            partitionValues,
            table,
            shardVersionCreated,
            txnCtx,
            nodeCtx,
            targetColumns,
            updateColumns,
            returnValues,
            false);
    }

    @VisibleForTesting
    @SuppressWarnings("unchecked")
    public Indexer(List<String> partitionValues,
                   DocTableInfo table,
                   Version shardVersionCreated,
                   TransactionContext txnCtx,
                   NodeContext nodeCtx,
                   List<ScopedRef> targetColumns,
                   @Nullable String[] updateColumns,
                   @Nullable Symbol[] returnValues,
                   boolean isOnConflictIndexer) {
        // assignedColumns are columns explicitly assigned by users; updateColumns for UPDATE and targetColumns for INSERT
        List<ScopedRef> assignedColumns;
        // insert-on-conflict: an indexer (used to index un-conflicted rows) with a child onConflictIndexer(used to index conflicted rows)
        if (updateColumns != null && updateColumns.length > 0 && !targetColumns.isEmpty() && !isOnConflictIndexer) {
            this.onConflictIndexer = new Indexer(
                partitionValues,
                table,
                shardVersionCreated,
                txnCtx,
                nodeCtx,
                targetColumns,
                updateColumns,
                returnValues,
                true
            );
            this.updateToInsert = null;
            this.columns = targetColumns;
            assignedColumns = targetColumns;
        } else if (updateColumns != null && updateColumns.length > 0) { // update
            this.onConflictIndexer = null;
            this.updateToInsert = new UpdateToInsert(
                nodeCtx,
                txnCtx,
                table,
                updateColumns,
                targetColumns
            );
            this.columns = this.updateToInsert.columns();
            assignedColumns = this.updateToInsert.updateColumns();
        } else { // insert
            this.onConflictIndexer = null;
            this.updateToInsert = null;
            this.columns = targetColumns;
            assignedColumns = targetColumns;
        }
        this.indexOrder = table.rootColumns();
        this.synthetics = new HashMap<>();
        this.writeOids = table.versionCreated().onOrAfter(DocTableInfo.COLUMN_OID_VERSION);
        this.getRef = table::getReference;
        InputFactory inputFactory = new InputFactory(nodeCtx);
        SymbolEvaluator symbolEval = new SymbolEvaluator(txnCtx, nodeCtx, SubQueryResults.EMPTY);
        var referenceResolver = new RefResolver(symbolEval, partitionValues, columns, table);
        Context<CollectExpression<IndexItem, Object>> ctxForRefs = inputFactory.ctxForRefs(
            txnCtx,
            referenceResolver
        );
        this.valueIndexers = new ArrayList<>(columns.size());
        int position = -1;
        for (var ref : columns) {
            ValueIndexer<?> valueIndexer;
            if (ref instanceof DynamicReference) {
                if (table.columnPolicy() == ColumnPolicy.STRICT) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "Cannot add column `%s` to table `%s` with column policy `strict`",
                        ref.column(),
                        table.ident()
                    ));
                }
                valueIndexer = new DynamicIndexer(ref.relation(), ref.column(), position, getRef, writeOids);
                position--;
            } else {
                valueIndexer = ref.valueType().valueIndexer(
                    table.ident(),
                    ref,
                    getRef
                );
            }
            this.valueIndexers.add(valueIndexer);
            addGeneratedToVerify(columnConstraints, table, ctxForRefs, ref);
        }
        this.tableConstraints = new ArrayList<>(table.checkConstraints().size());
        addNotNullConstraints(
            tableConstraints,
            columnConstraints,
            table,
            columns,
            ctxForRefs
        );
        for (var constraint : table.checkConstraints()) {
            Symbol expression = constraint.expression();
            Input<?> input = ctxForRefs.add(expression);
            tableConstraints.add(new TableCheckConstraint(input, constraint));
        }

        boolean isUpdate = updateColumns != null && updateColumns.length > 0 && targetColumns.isEmpty();

        for (var ref : table.defaultExpressionColumns()) {
            if (targetColumns.contains(ref) || ref.granularity() == RowGranularity.PARTITION) {
                continue;
            }
            // never re-generate default root columns for UPDATE
            // default sub-columns may need to be re-generated, i.e. update t set o={}, where o has default sub-cols
            if (isUpdate && ref.column().isRoot()) {
                continue;
            }
            ColumnIdent column = ref.column();

            createParentSynthetics(table, columns, column, getRef);

            Input<?> input = table.primaryKey().contains(column)
                ? ctxForRefs.add(ref)
                : ctxForRefs.add(ref.defaultExpression());
            ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) ref.valueType().valueIndexer(
                table.ident(),
                ref,
                getRef
            );
            Synthetic synthetic = new Synthetic(ref, input, valueIndexer);
            this.synthetics.put(column, synthetic);

            if (!ref.defaultExpression().isDeterministic()) {
                undeterministic.add(synthetic);
            }
        }
        for (var ref : table.generatedColumns()) {
            if (ref.granularity() == RowGranularity.PARTITION) {
                continue;
            }

            // never generate generated columns if assigned
            if (assignedColumns.contains(ref)) {
                continue;
            }

            createParentSynthetics(table, columns, ref.column(), getRef);

            Input<?> input = ctxForRefs.add(ref.generatedExpression());
            ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) ref.valueType().valueIndexer(
                table.ident(),
                ref,
                getRef
            );
            Synthetic synthetic = new Synthetic(ref, input, valueIndexer);
            this.synthetics.put(ref.column(), synthetic);

            if (!ref.isDeterministic()) {
                undeterministic.add(synthetic);
            }
        }
        this.indexColumns = buildIndexColumns(table.indexColumns(), table::getReference, ctxForRefs::add);
        if (returnValues == null) {
            this.returnValueInputs = null;
        } else {
            Context<Input<?>> ctxForReturnValues = inputFactory.ctxForRefs(
                txnCtx,
                ref -> {
                    // Using Synthetic if available, it caches results to ensure non-deterministic functions yield the same result
                    // across indexing and return values
                    Synthetic synthetic = synthetics.get(ref.column());
                    if (synthetic == null) {
                        Input<?> input = ctxForRefs.add(ref);
                        // If 'ref' is an object column with synthetic children, the synthetic values need to be merged
                        return () -> mergeSyntheticChildren(ref, input);
                    } else {
                        return synthetic;
                    }
                }
            );
            this.returnValueInputs = new ArrayList<>(returnValues.length);
            for (Symbol returnValue : returnValues) {
                this.returnValueInputs.add(ctxForReturnValues.add(returnValue));
            }
        }
        this.expressions = ctxForRefs.expressions();
        this.tableVersionCreated = shardVersionCreated;
    }

    public static <I> List<IndexColumn<I>> buildIndexColumns(Collection<IndexReference> indexReferences,
                                                             Function<ColumnIdent, ScopedRef> getRef,
                                                             Function<ScopedRef, ? extends I> createInput) {
        List<IndexColumn<I>> indexColumns = new ArrayList<>(indexReferences.size());
        for (var ref : indexReferences) {
            ArrayList<I> indexInputs = new ArrayList<>(ref.columns().size());

            for (var sourceRef : ref.columns()) {
                ScopedRef reference = getRef.apply(sourceRef.column());
                assert reference.equals(sourceRef) : "refs must match";

                I input = createInput.apply(sourceRef);
                indexInputs.add(input);
            }
            if (ref.indexType() != IndexType.NONE) {
                indexColumns.add(new IndexColumn<>(ref, indexInputs));
            }
        }
        return indexColumns;
    }

    /**
     * Creates a copy of the given value and merges in its synthetic children for object columns; otherwise returns the
     * original value unchanged.
     * <p>
     * Mainly used for `RETURNING` clause returning objects with synthetic sub-columns where the result set is expected
     * to contain all children.
     * <p>
     * Background: the reason for creating a copy and merging - the current design for INSERT/UPDATE does not merge the
     * insert values with synthetic sub-columns.
     * They are held separately in {@link IndexItem#insertValues} and {@link Indexer#synthetics} to save the cost
     * of streaming to replicas.
     */
    private Object mergeSyntheticChildren(ScopedRef parentRef, Input<?> input) {
        Object value = input.value();
        if (value instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = Maps.deepCopy((Map<String, Object>) value);
            ColumnIdent parentColumn = parentRef.column();
            for (Synthetic childSynthetic : synthetics.values()) {
                ColumnIdent childSyntheticColumn = childSynthetic.ref.column();
                if (!childSyntheticColumn.isChildOf(parentColumn) ||
                    // Sometimes Indexer#synthetics contains a non-synthetic object column
                    // if it is a parent of a synthetic
                    (!childSynthetic.ref.isGenerated() && childSynthetic.ref.defaultExpression() == null)) {
                    continue;
                }
                var childSyntheticPath = childSyntheticColumn.path();

                // preserve any existing synthetic children's values - even nulls
                if (Maps.pathExists(m, childSyntheticPath)) {
                    continue;
                }

                Maps.mergeInto(
                    m,
                    childSyntheticPath.getFirst(),
                    childSyntheticPath.subList(1, childSyntheticPath.size()),
                    childSynthetic.value(),
                    Map::put,
                    false);
            }
            return m;
        } else {
            return value;
        }
    }

    /**
     * To ensure default or generated expressions of object children are evaluated
     * it's necessary to ensure the parent has values (or is present in the insert)
     * This is because of how the index routine works:
     * Processing objects is recursive, it skips null values -> it needs root synthetics as entry points
     */
    @SuppressWarnings("unchecked")
    private void createParentSynthetics(DocTableInfo table,
                                        List<ScopedRef> targetColumns,
                                        ColumnIdent column,
                                        Function<ColumnIdent, ScopedRef> getRef) {
        for (ColumnIdent parent : column.parents()) {
            if (synthetics.containsKey(parent) || Symbols.hasColumn(targetColumns, parent)) {
                continue;
            }
            ScopedRef parentRef = table.getReference(parent);
            assert parentRef != null
                : "Must be able to retrieve Reference for parent of a `defaultExpressionColumn`";

            int dimensions = ArrayType.dimensions(parentRef.valueType());
            if (dimensions > 0) {
                break;
            }
            Input<?> input = HashMap::new;
            ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) parentRef.valueType().valueIndexer(
                table.ident(),
                parentRef,
                getRef
            );
            Synthetic synthetic = new Synthetic(parentRef, input, valueIndexer);
            this.synthetics.put(parent, synthetic);
        }
    }

    /**
     * Trigger resolving of columns and update indexers if needed.
     * Should be only triggered when new columns were detected by {@link #collectSchemaUpdates(IndexItem)
     * and added to the cluster state
     *
     * @param getRef A function that returns a reference for a given column ident based on the current cluster state
     */
    public void updateTargets(DocTableInfo newTable) {
        Function<ColumnIdent, ScopedRef> getRef = newTable::getReference;
        var it = columns.iterator();
        var idx = 0;
        while (it.hasNext()) {
            var oldRef = it.next();
            if (oldRef.oid() == COLUMN_OID_UNASSIGNED) {
                // try to resolve the column again, new reference may have been added to or dropped of the table or
                // the reference may be invalid
                ScopedRef newRef = getRef.apply(oldRef.column());
                if (newRef == null) {
                    // column can be of an undetermined type, e.g. `[]` (array with undefined inner type)
                    valueIndexers.get(idx).updateTargets(getRef);
                    idx++;
                    continue;
                }
                if (oldRef.equals(newRef) == false) {
                    columns.set(idx, newRef);
                    valueIndexers.set(idx, newRef.valueType().valueIndexer(
                        newRef.relation(),
                        newRef,
                        getRef
                    ));
                }
            } else if (DataTypes.isArrayOfNulls(oldRef.valueType())) {
                // null arrays may be upgraded to arrays of a defined type
                ScopedRef newRef = getRef.apply(oldRef.column());
                if (newRef == null) {
                    continue;
                }
                if (newRef.valueType().id() == ArrayType.ID) {
                    columns.set(idx, newRef);
                    valueIndexers.set(idx, newRef.valueType().valueIndexer(newRef.relation(), newRef, getRef));
                }
            } else if (ArrayType.unnest(oldRef.valueType()).id() == ObjectType.ID) {
                // object types in 'columns' may be outdated - missing newly added child types
                ScopedRef newRef = getRef.apply(oldRef.column());
                if (!newRef.equals(oldRef)) {
                    columns.set(idx, newRef);
                    valueIndexers.get(idx).updateTargets(getRef);
                }
            } else {
                valueIndexers.get(idx).updateTargets(getRef);
            }
            idx++;
        }

        for (var entry : synthetics.entrySet()) {
            ColumnIdent column = entry.getKey();
            if (!column.isRoot()) {
                continue;
            }
            Synthetic synthetic = entry.getValue();
            ValueIndexer<Object> indexer = synthetic.indexer();
            indexer.updateTargets(getRef);
        }

        if (onConflictIndexer == null) {
            indexOrder = newTable.rootColumns();
        } else {
            Map<ColumnIdent, ScopedRef> indexOrderForInsertOnConflict = newTable.rootColumns().stream()
                .collect(Collectors.toMap(ScopedRef::column, Function.identity(), (a, _) -> a, LinkedHashMap::new));

            // extend indexOrder with dynamically added columns from conflicted rows
            onConflictIndexer.columns.forEach(r -> {
                indexOrderForInsertOnConflict.putIfAbsent(r.column(), r);
            });
            indexOrder = new ArrayList<>(indexOrderForInsertOnConflict.values());
            onConflictIndexer.indexOrder = indexOrder;
        }
    }

    private static void addNotNullConstraints(List<TableConstraint> tableConstraints,
                                              Map<ColumnIdent, ColumnConstraint> columnConstraints,
                                              DocTableInfo table,
                                              List<ScopedRef> targetColumns,
                                              Context<CollectExpression<IndexItem, Object>> ctxForRefs) {
        for (var column : table.notNullColumns()) {
            ScopedRef ref = table.getReference(column);
            assert ref != null : "Column in #notNullColumns must be available via table.getReference";
            if (targetColumns.contains(ref)) {
                columnConstraints.merge(ref.column(), new NotNull(column), MultiCheck::merge);
            } else if (ref instanceof GeneratedReference generated) {
                Input<?> input = ctxForRefs.add(generated.generatedExpression());
                tableConstraints.add(new NotNullTableConstraint(column, input));
            } else {
                Input<?> input = ctxForRefs.add(ref);
                tableConstraints.add(new NotNullTableConstraint(column, input));
            }
        }
    }

    private static void addGeneratedToVerify(Map<ColumnIdent, ColumnConstraint> columnConstraints,
                                             DocTableInfo table,
                                             Context<?> ctxForRefs,
                                             ScopedRef ref) {
        if (ref instanceof GeneratedReference generated && generated.isDeterministic()) {
            Input<?> input = ctxForRefs.add(generated.generatedExpression());
            columnConstraints.put(ref.column(), new CheckGeneratedValue(input, generated));
        }
        if (ref.valueType() instanceof ObjectType objectType) {
            for (var entry : objectType.innerTypes().entrySet()) {
                String innerName = entry.getKey();
                ColumnIdent innerColumn = ref.column().getChild(innerName);
                ScopedRef reference = table.getReference(innerColumn);
                if (reference == null) {
                    continue;
                }
                addGeneratedToVerify(columnConstraints, table, ctxForRefs, reference);
            }
        }
    }


    /**
     * Looks for new columns in the values of the given IndexItem and returns them.
     */
    @SuppressWarnings("unchecked")
    public List<ScopedRef> collectSchemaUpdates(IndexItem item) throws IOException {
        LinkedHashSet<ScopedRef> newColumns = new LinkedHashSet<>();
        Consumer<? super ScopedRef> onDynamicColumn = ref -> {
            ref.column().validForCreate();
            newColumns.add(ref);
        };

        for (var expression : expressions) {
            expression.setNextRow(item);
        }

        Object[] values = item.insertValues();
        for (int i = 0; i < values.length; i++) {
            ScopedRef reference = columns.get(i);
            Object value = values[i];
            // No granularity check since PARTITIONED BY columns cannot be added dynamically.
            if (value == null) {
                continue;
            }
            ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) valueIndexers.get(i);
            valueIndexer.collectSchemaUpdates(reference.valueType().sanitizeValue(value), onDynamicColumn, synthetics::get);
        }
        // Generated columns can result in new columns. For example: details object generated always as {\"a1\" = {\"b1\" = 'test'}},
        for (var entry : synthetics.entrySet()) {
            ColumnIdent column = entry.getKey();
            if (!column.isRoot()) {
                continue;
            }
            Synthetic synthetic = entry.getValue();
            ValueIndexer<Object> indexer = synthetic.indexer();
            Object value = synthetic.value();
            if (value == null) {
                continue;
            }
            indexer.collectSchemaUpdates(
                value,
                onDynamicColumn,
                synthetics::get
            );
        }
        return newColumns.stream().toList();
    }

    /**
     * Create a {@link ParsedDocument} from {@link IndexItem}
     *
     * This must be called after any new columns (found via
     * {@link #collectSchemaUpdates(IndexItem)}) have been added to the cluster
     * state.
     */
    @VisibleForTesting
    @SuppressWarnings("unchecked")
    ParsedDocument index(IndexItem item, List<ScopedRef> indexOrder) throws IOException {
        assert item.insertValues().length <= valueIndexers.size()
            : "Number of values must be less than or equal the number of targetColumns/valueIndexers";

        for (var expression : expressions) {
            expression.setNextRow(item);
        }
        for (Synthetic synthetic : synthetics.values()) {
            synthetic.reset();
        }

        TranslogWriter translogWriter = new XContentTranslogWriter();
        IndexDocumentBuilder docBuilder = new IndexDocumentBuilder(
            translogWriter,
            synthetics::get,
            columnConstraints,
            tableVersionCreated
        );
        Object[] values = item.insertValues();

        for (ScopedRef ref : indexOrder) {
            int idx = columns.indexOf(ref);
            // for insert-on-conflict columns.size() > values.length is possible to be able to process both INSERT and UPDATE rows
            if (idx >= 0 && idx < values.length) {
                Object value = valueForInsert(ref.valueType(), values[idx]);
                ColumnConstraint check = columnConstraints.get(ref.column());
                if (check != null) {
                    check.verify(value);
                }
                if (ref.granularity() == RowGranularity.PARTITION) {
                    continue;
                }
                if (value != null) {
                    ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) valueIndexers.get(idx);
                    translogWriter.writeFieldName(valueIndexer.storageIdentLeafName());
                    valueIndexer.indexValue(value, docBuilder);
                    continue;
                }
            }
            ColumnIdent column = ref.column();
            if (!column.isRoot()) {
                continue;
            }
            Synthetic synthetic = synthetics.get(column);
            if (synthetic != null) {
                Object value = synthetic.value();
                if (value == null) {
                    continue;
                }
                ValueIndexer<Object> indexer = synthetic.indexer();
                translogWriter.writeFieldName(indexer.storageIdentLeafName());
                indexer.indexValue(value, docBuilder);
            }
        }

        addIndexColumns(indexColumns, docBuilder);

        for (var constraint : tableConstraints) {
            constraint.verify(item.insertValues());
        }

        return docBuilder.build(item.id());
    }

    public ParsedDocument index(IndexItem item) throws IOException {
        return index(item, indexOrder);
    }

    public IndexItem updateToInsert(Doc doc, @Nullable Symbol[] updateAssignments, Object[] excluded) {
        return updateToInsert.convert(doc, updateAssignments, excluded);
    }

    /**
     * Doesn't add fields for NULL values.
     */
    private static void addIndexColumns(List<IndexColumn<Input<?>>> indexColumns,
                                        IndexDocumentBuilder docBuilder) {
        for (var indexColumn : indexColumns) {
            String fqn = indexColumn.reference.storageIdent();
            for (var input : indexColumn.inputs) {
                Object value = input.value();
                if (value == null) {
                    continue;
                }
                if (value instanceof Iterable<?> it) {
                    for (Object val : it) {
                        if (val == null) {
                            continue;
                        }
                        Field field = new Field(fqn, val.toString(), FulltextIndexer.FIELD_TYPE);
                        docBuilder.addField(field);
                    }
                } else {
                    Field field = new Field(fqn, value.toString(), FulltextIndexer.FIELD_TYPE);
                    docBuilder.addField(field);
                }
            }
        }
    }

    private static <T> T valueForInsert(DataType<T> valueType, Object value) {
        return valueType.valueForInsert(valueType.sanitizeValue(value));
    }

    @Nullable
    public Object[] returnValues(IndexItem item) {
        if (this.returnValueInputs == null) {
            return null;
        }
        expressions.forEach(x -> x.setNextRow(item));
        Object[] result = new Object[this.returnValueInputs.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = this.returnValueInputs.get(i).value();
        }
        return result;
    }

    public static Consumer<IndexItem> createConstraintCheck(DocTableInfo table,
                                                            List<String> partitionValues,
                                                            TransactionContext txnCtx,
                                                            NodeContext nodeCtx,
                                                            List<ScopedRef> targetColumns) {
        var symbolEval = new SymbolEvaluator(txnCtx, nodeCtx, SubQueryResults.EMPTY);
        InputFactory inputFactory = new InputFactory(nodeCtx);
        var referenceResolver = new RefResolver(symbolEval, partitionValues, targetColumns, table);
        Context<CollectExpression<IndexItem, Object>> ctxForRefs = inputFactory.ctxForRefs(
            txnCtx,
            referenceResolver
        );
        Map<ColumnIdent, ColumnConstraint> columnConstraints = new HashMap<>();
        for (var ref : targetColumns) {
            addGeneratedToVerify(columnConstraints, table, ctxForRefs, ref);
        }
        List<TableConstraint> tableConstraints = new ArrayList<>(table.checkConstraints().size());
        addNotNullConstraints(
            tableConstraints, columnConstraints, table, targetColumns, ctxForRefs);

        for (var constraint : table.checkConstraints()) {
            Symbol expression = constraint.expression();
            Input<?> input = ctxForRefs.add(expression);
            tableConstraints.add(new TableCheckConstraint(input, constraint));
        }
        return indexItem -> {
            for (var expression : ctxForRefs.expressions()) {
                expression.setNextRow(indexItem);
            }
            Object[] values = indexItem.insertValues();
            for (int i = 0; i < values.length; i++) {
                ScopedRef reference = targetColumns.get(i);
                Object value = valueForInsert(reference.valueType(), values[i]);
                ColumnConstraint check = columnConstraints.get(reference.column());
                if (check != null) {
                    check.verify(value);
                }
                if (reference.granularity() == RowGranularity.PARTITION) {
                    continue;
                }
                if (value == null) {
                    continue;
                }
            }
            for (var constraint : tableConstraints) {
                constraint.verify(values);
            }
        };
    }

    public List<ScopedRef> columns() {
        return columns;
    }

    public List<ScopedRef> insertColumns() {
        if (undeterministic.isEmpty()) {
            return columns;
        }
        List<ScopedRef> newColumns = new ArrayList<>(columns);
        for (var synthetic : undeterministic) {
            if (synthetic.ref.column().isRoot()) {
                if (newColumns.contains(synthetic.ref) == false) {
                    newColumns.add(synthetic.ref);
                }
            } else {
                var rootIdent = synthetic.ref.column().getRoot();
                int rootIndex = UnscopedRef.indexOf(newColumns, rootIdent);
                if (rootIndex == -1) {
                    // Synthetic is a generated/default sub-column with root not listed in the insert/upsert targets.
                    // We need to add the root to replica targets
                    // since we will generate object value in addGeneratedValues().
                    ScopedRef rootRef = getRef.apply(rootIdent);
                    assert rootRef != null : "Root must exist in the table";
                    newColumns.add(rootRef);
                }
            }
        }
        return newColumns;
    }

    public Object[] addGeneratedValues(IndexItem item) {
        Object[] insertValues = item.insertValues();
        if (onConflictIndexer() == null && undeterministic.isEmpty()) {
            return insertValues;
        }
        assert onConflictIndexer() == null || new HashSet<>(onConflictIndexer().insertColumns().stream().map(ScopedRef::column).toList())
            .containsAll(insertColumns().stream().map(ScopedRef::column).toList()) : "onConflictIndexer().insertColumns() is a superset of this.insertColumns()";
        List<ScopedRef> insertColumns = onConflictIndexer() == null ? insertColumns() : onConflictIndexer().insertColumns();
        List<Object> extendedValues = new ArrayList<>(insertValues.length);
        Collections.addAll(extendedValues, insertValues);
        for (int i = insertValues.length; i < insertColumns.size(); i++) {
            extendedValues.add(null);
        }

        // insert-on-conflict: insert path may need to insert a value so the column must be streamed then update path must stream the correct value
        if (onConflictIndexer() != null) {
            for (var synthetic : synthetics.values()) {
                ScopedRef ref = synthetic.ref;
                if (ref.column().isRoot() && ref.defaultExpression() != null && ref.defaultExpression().isDeterministic() && synthetic.isComputed()) {
                    int idx = insertColumns.indexOf(ref);
                    assert idx >= 0 && idx < extendedValues.size() : "We know how many values are to be streamed: insertColumns()";
                    extendedValues.set(idx, synthetic.computedValue);
                }
            }
        }
        for (var synthetic : undeterministic) {
            // synthetic columns not yet computed imply 'null' were indexed, do not generated values for them
            if (!synthetic.isComputed()) {
                continue;
            }
            ColumnIdent column = synthetic.ref.column();
            Object value = synthetic.value();
            if (column.isRoot()) {
                int idx = UnscopedRef.indexOf(insertColumns, column);
                assert idx >= 0 && idx < extendedValues.size() : "We know how many values are to be streamed: insertColumns()";
                extendedValues.set(idx, value);
            } else {
                int valueIdx = UnscopedRef.indexOf(insertColumns, column.getRoot());
                Map<String, Object> root;
                assert valueIdx >= 0 && valueIdx < extendedValues.size() : "We know how many values are to be streamed: insertColumns()";
                root = castMapUnchecked(extendedValues.get(valueIdx));
                if (root == null) {
                    if (UnscopedRef.indexOf(this.columns, column.getRoot()) >= 0) {
                        // When a null is assigned to a parent object, do not generate nondeterministic children
                        continue;
                    } else {
                        // when the parent object is omitted in an insert stmt, generated the parent
                        root = new HashMap<>();
                        extendedValues.set(valueIdx, root);
                    }
                }
                ColumnIdent child = column.shiftRight();
                // We don't override value if it exists.
                // It's needed when:
                // - users explicitly provide the whole object (including generated sub-column), then we take user provided value.
                // - when upsert/update takes existing value from the existing document, it needs to take the whole object as is.
                Maps.mergeInto(
                    root,
                    child.name(),
                    child.path(),
                    value,
                    Map::putIfAbsent,
                    false // i.e., a user may want to set null to an object column with non-deterministic sub-cols
                );
            }
        }
        return extendedValues.toArray();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMapUnchecked(Object value) {
        return (Map<String, Object>) value;
    }

    public Indexer onConflictIndexer() {
        return this.onConflictIndexer;
    }

    public boolean hasReturnValues() {
        return returnValueInputs != null;
    }
}
