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

package io.crate.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.WhereClause;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.NestableInput;
import io.crate.expression.reference.MapLookupByPathExpression;
import io.crate.expression.symbol.DynamicReference;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

public final class SystemTable<T> implements TableInfo {

    private final RelationName name;
    private final Map<ColumnIdent, Reference> columns;
    private final Map<ColumnIdent, RowCollectExpressionFactory<T>> expressions;
    private final List<ColumnIdent> primaryKeys;
    private final List<Reference> rootColumns;
    private final GetRouting getRouting;
    private final Map<ColumnIdent, Function<ColumnIdent, DynamicReference>> dynamicColumns;
    private final Set<Operation> supportedOperations;
    private final RowGranularity rowGranularity;

    public SystemTable(RelationName name,
                       Map<ColumnIdent, Reference> columns,
                       Map<ColumnIdent, RowCollectExpressionFactory<T>> expressions,
                       List<ColumnIdent> primaryKeys,
                       Map<ColumnIdent, Function<ColumnIdent, DynamicReference>> dynamicColumns,
                       Set<Operation> supportedOperations,
                       RowGranularity rowGranularity,
                       @Nullable GetRouting getRouting) {
        this.name = name;
        this.columns = columns;
        this.supportedOperations = supportedOperations;
        this.rowGranularity = rowGranularity;
        this.getRouting = getRouting == null
            ? (state, routingProvider, sessionSettings) -> Routing.forTableOnSingleNode(name, state.nodes().getLocalNodeId())
            : getRouting;
        this.rootColumns = columns.values().stream()
            .filter(x -> x.column().isRoot())
            .toList();
        this.expressions = expressions;
        this.primaryKeys = primaryKeys;
        this.dynamicColumns = dynamicColumns;
    }

    @Nullable
    @Override
    public Reference getReference(ColumnIdent column) {
        return getReadReference(column);
    }

    @Nullable
    @Override
    public Reference getReadReference(ColumnIdent column) {
        var ref = columns.get(column);
        if (ref != null) {
            return ref;
        }
        ColumnIdent parent = column;
        do {
            var dynamic = dynamicColumns.get(parent);
            if (dynamic != null) {
                return dynamic.apply(column);
            }
        } while ((parent = parent.getParent()) != null);
        return null;
    }

    @Override
    public Routing getRouting(ClusterState state,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              CoordinatorSessionSettings sessionSettings) {
        return getRouting.getRouting(state, routingProvider, sessionSettings);
    }

    @Override
    public Collection<Reference> columns() {
        return rootColumns;
    }

    @Override
    public RowGranularity rowGranularity() {
        return rowGranularity;
    }

    @Override
    public RelationName ident() {
        return name;
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return primaryKeys;
    }

    @Override
    public Settings parameters() {
        return Settings.EMPTY;
    }

    @Override
    public Set<Operation> supportedOperations() {
        return supportedOperations;
    }

    @Override
    public RelationType relationType() {
        return RelationType.BASE_TABLE;
    }

    @Override
    @NotNull
    public Iterator<Reference> iterator() {
        return columns.values().iterator();
    }

    public Map<ColumnIdent, RowCollectExpressionFactory<T>> expressions() {
        return expressions;
    }

    @FunctionalInterface
    public interface GetRouting {

        Routing getRouting(ClusterState state, RoutingProvider routingProvider, CoordinatorSessionSettings sessionSettings);
    }

    static class Column<T, U> {

        protected final ColumnIdent column;
        protected final DataType<U> type;
        protected final Function<T, U> getProperty;
        protected final boolean isNullable;

        public Column(ColumnIdent column, DataType<U> type, Function<T, U> getProperty) {
            this(column, type, getProperty, true);
        }

        public Column(ColumnIdent column, DataType<U> type, Function<T, U> getProperty, boolean isNullable) {
            this.column = column;
            this.type = type;
            this.getProperty = getProperty;
            this.isNullable = isNullable;
        }

        @Override
        public String toString() {
            return '(' + column.sqlFqn() + ", " + type.getName() + ')';
        }

        public void addExpression(HashMap<ColumnIdent, RowCollectExpressionFactory<T>> expressions) {
            expressions.put(column, () -> new Expression<>(column, getProperty, expressions));
        }
    }

    static class DynamicColumn<T> extends Column<T, Map<String, Object>> {

        private final DataType<?> leafType;

        public DynamicColumn(ColumnIdent column,
                             DataType<?> leafType,
                             Function<T, Map<String, Object>> getObject) {
            super(column, DataTypes.UNTYPED_OBJECT, getObject);
            this.leafType = leafType;
        }

        @Override
        public void addExpression(HashMap<ColumnIdent, RowCollectExpressionFactory<T>> expressions) {
            expressions.put(column, () -> new MapLookupByPathExpression<>(
                getProperty,
                List.of(),
                leafType::implicitCast)
            );
        }
    }

    public abstract static class Builder<T> {

        public abstract <U> Builder<T> add(String column, DataType<U> type, Function<T, U> getProperty);

        protected abstract <U> Builder<T> add(Column<T, U> column);
    }

    public static class RelationBuilder<T> extends Builder<T> {

        private final RelationName name;
        private final ArrayList<Column<T, ?>> columns = new ArrayList<>();
        private final RowGranularity rowGranularity;
        private List<ColumnIdent> primaryKeys = List.of();
        private GetRouting getRouting;
        private Set<Operation> supportedOperations = Operation.SYS_READ_ONLY;

        RelationBuilder(RelationName name, RowGranularity rowGranularity) {
            this.name = name;
            this.rowGranularity = rowGranularity;
        }

        /**
         * Override the routing funciton, if not overriden it defaults to `Routing.forTableOnSingleNode`
         */
        public RelationBuilder<T> withRouting(GetRouting getRouting) {
            this.getRouting = getRouting;
            return this;
        }

        public RelationBuilder<T> withSupportedOperations(Set<Operation> supportedOperations) {
            this.supportedOperations = supportedOperations;
            return this;
        }

        public <U> RelationBuilder<T> add(String column, DataType<U> type, Function<T, U> getProperty) {
            return add(new Column<>(new ColumnIdent(column), type, getProperty));
        }

        public <U> RelationBuilder<T> addNonNull(String column, DataType<U> type, Function<T, U> getProperty) {
            return add(new Column<>(new ColumnIdent(column), type, getProperty, false));
        }

        @Override
        protected <U> RelationBuilder<T> add(Column<T, U> column) {
            columns.add(column);
            return this;
        }

        public RelationBuilder<T> addDynamicObject(String column, DataType<?> leafType, Function<T, Map<String, Object>> getObject) {
            return add(new DynamicColumn<>(new ColumnIdent(column), leafType, getObject));
        }

        public SystemTable<T> build() {
            HashMap<ColumnIdent, Function<ColumnIdent, DynamicReference>> dynamicColumns = new HashMap<>();
            LinkedHashMap<ColumnIdent, Reference> refByColumns = new LinkedHashMap<>();
            HashMap<ColumnIdent, RowCollectExpressionFactory<T>> expressions = new HashMap<>();
            columns.sort(Comparator.comparing(x -> x.column));
            for (int i = 0; i < columns.size(); i++) {
                int position = i + 1;     // start at 1
                Column<T, ?> column = columns.get(i);
                refByColumns.put(
                    column.column,
                    new SimpleReference(
                        new ReferenceIdent(name, column.column),
                        RowGranularity.DOC,
                        column.type,
                        ColumnPolicy.DYNAMIC,
                        IndexType.PLAIN,
                        column.isNullable,
                        false,
                        position,
                        COLUMN_OID_UNASSIGNED, // No oid for system tables
                        null
                    )
                );
                column.addExpression(expressions);
                if (column instanceof DynamicColumn<?>) {
                    final DataType<?> leafType = ((DynamicColumn<?>) column).leafType;
                    dynamicColumns.put(column.column, wanted -> {
                        var ref = new DynamicReference(
                            new ReferenceIdent(name, wanted),
                            RowGranularity.DOC,
                            ColumnPolicy.DYNAMIC,
                            position
                        );
                        ref.valueType(leafType);
                        return ref;
                    });
                }
            }
            return new SystemTable<>(
               name,
               refByColumns,
               expressions,
               primaryKeys,
               dynamicColumns,
               supportedOperations,
               rowGranularity,
               getRouting
           );
        }

        public ObjectBuilder<T, RelationBuilder<T>> startObject(String column) {
            return new ObjectBuilder<>(this, new ColumnIdent(column), t -> false);
        }

        public ObjectBuilder<T, RelationBuilder<T>> startObject(String column, Predicate<T> objectIsNull) {
            return new ObjectBuilder<>(this, new ColumnIdent(column), objectIsNull);
        }

        public <U> ObjectArrayBuilder<U, T, RelationBuilder<T>> startObjectArray(String column, Function<T, ? extends Collection<U>> getItems) {
            return new ObjectArrayBuilder<>(this, new ColumnIdent(column), getItems);
        }

        public RelationBuilder<T> setPrimaryKeys(ColumnIdent ... primaryKeys) {
            this.primaryKeys = Arrays.asList(primaryKeys);
            return this;
        }
    }

    public static class ObjectBuilder<T, P extends Builder<T>> extends Builder<T> {

        private final P parent;
        private final ColumnIdent baseColumn;
        private final ArrayList<Column<T, ?>> columns = new ArrayList<>();
        private final Predicate<T> objectIsNull;

        private ObjectBuilder(P parent, ColumnIdent baseColumn, Predicate<T> objectIsNull) {
            this.parent = parent;
            this.baseColumn = baseColumn;
            this.objectIsNull = objectIsNull;
        }

        public <U> ObjectBuilder<T, P> add(String column, DataType<U> type, Function<T, U> getProperty) {
            return add(new Column<>(baseColumn.getChild(column), type, getProperty));
        }

        public ObjectBuilder<T, P> addDynamicObject(String column, DataType<?> leafType, Function<T, Map<String, Object>> getObject) {
            return add(new DynamicColumn<>(baseColumn.getChild(column), leafType, getObject));
        }

        @Override
        protected <U> ObjectBuilder<T, P> add(Column<T, U> column) {
            columns.add(column);
            return this;
        }

        public <U> ObjectArrayBuilder<U, T, ObjectBuilder<T, P>> startObjectArray(String column, Function<T, ? extends Collection<U>> getItems) {
            return new ObjectArrayBuilder<>(this, baseColumn.getChild(column), getItems);
        }

        public ObjectBuilder<T, ObjectBuilder<T, P>> startObject(String column) {
            return new ObjectBuilder<>(this, baseColumn.getChild(column), objectIsNull);
        }

        public P endObject() {
            ObjectType.Builder typeBuilder = ObjectType.builder();
            ArrayList<Column<T, ?>> directChildren = new ArrayList<>();
            for (var col : columns) {
                if (col.column.path().size() == baseColumn.path().size() + 1) {
                    directChildren.add(col);
                }
            }
            for (var column : directChildren) {
                typeBuilder.setInnerType(column.column.leafName(), column.type);
            }
            ObjectType objectType = typeBuilder.build();
            parent.add(new Column<>(baseColumn, objectType, new ObjectExpression<>(directChildren, objectIsNull)));
            for (Column<T, ?> column : columns) {
                addColumnToParent(column);
            }
            return parent;
        }

        private <U> void addColumnToParent(Column<T, U> column) {
            parent.add(new Column<>(column.column, column.type, column.getProperty));
        }
    }

    public static class ObjectArrayBuilder<ItemType, ParentItemType, P extends Builder<ParentItemType>> extends Builder<ItemType> {

        private final P parent;
        private final ArrayList<Column<ItemType, ?>> columns = new ArrayList<>();
        private final ColumnIdent baseColumn;
        private final Function<ParentItemType, ? extends Collection<ItemType>> getItems;

        public ObjectArrayBuilder(P parent, ColumnIdent baseColumn, Function<ParentItemType, ? extends Collection<ItemType>> getItems) {
            this.parent = parent;
            this.baseColumn = baseColumn;
            this.getItems = getItems;
        }

        public P endObjectArray() {
            ObjectType.Builder typeBuilder = ObjectType.builder();
            ArrayList<Column<ItemType, ?>> directChildren = new ArrayList<>();
            for (var col : columns) {
                if (col.column.path().size() == baseColumn.path().size() + 1) {
                    directChildren.add(col);
                }
            }
            for (var column : directChildren) {
                typeBuilder.setInnerType(column.column.leafName(), column.type);
            }
            ObjectType objectType = typeBuilder.build();
            parent.add(new Column<>(baseColumn, new ArrayType<>(objectType), getLeafColumnValues(directChildren)));
            for (var column : columns) {
                addColumnToParent(column);
            }
            return parent;
        }

        public Function<ParentItemType, List<Map<String, Object>>> getLeafColumnValues(ArrayList<Column<ItemType, ?>> directChildren) {
            return xs -> {
                var items = getItems.apply(xs);
                ArrayList<Map<String, Object>> result = new ArrayList<>(items.size());
                for (ItemType item : items) {
                    HashMap<String, Object> map = new HashMap<>(directChildren.size());
                    for (int i = 0; i < directChildren.size(); i++) {
                        Column<ItemType, ?> column = directChildren.get(i);
                        try {
                            Object value = column.getProperty.apply(item);
                            map.put(column.column.leafName(), value);
                        } catch (NullPointerException ignored) {
                        }
                    }
                    result.add(map);
                }
                return result;
            };
        }

        private <U> void addColumnToParent(Column<ItemType, U> column) {
            parent.add(new Column<>(
                column.column,
                new ArrayType<>(column.type),
                xs -> {
                    var items = getItems.apply(xs);
                    ArrayList<U> result = new ArrayList<>(items.size());
                    for (ItemType item : items) {
                        result.add(column.getProperty.apply(item));
                    }
                    return result;
                }
            ));
        }

        @Override
        public <U> ObjectArrayBuilder<ItemType, ParentItemType, P> add(String column,
                                                                        DataType<U> type,
                                                                        Function<ItemType, U> getProperty) {
            return add(new Column<>(baseColumn.getChild(column), type, getProperty));
        }


        @Override
        protected <U> ObjectArrayBuilder<ItemType, ParentItemType, P> add(Column<ItemType, U> column) {
            columns.add(column);
            return this;
        }
    }

    private static class Expression<T, U> implements NestableCollectExpression<T, U> {

        private final ColumnIdent column;
        private final Function<T, U> getProperty;
        private final HashMap<ColumnIdent, RowCollectExpressionFactory<T>> expressions;
        private T row;

        public Expression(ColumnIdent column, Function<T, U> getProperty, HashMap<ColumnIdent, RowCollectExpressionFactory<T>> expressions) {
            this.column = column;
            this.getProperty = getProperty;
            this.expressions = expressions;
        }

        @Override
        public void setNextRow(T row) {
            this.row = row;
        }

        @Override
        public U value() {
            try {
                return getProperty.apply(row);
            } catch (NullPointerException e) {
                // This is to be able to be lazy in the column definitions and things like
                // `x -> x.a().b().c().d()`
                // Maybe not a good idea because of performance?
                return null;
            }
        }

        @Nullable
        public NestableInput<?> getChild(String name) {
            var factory = expressions.get(column.getChild(name));
            return factory == null ? null : factory.create();
        }
    }

    public static <T> RelationBuilder<T> builder(RelationName name) {
        return new RelationBuilder<>(name, RowGranularity.DOC);
    }

    public static <T> RelationBuilder<T> builder(RelationName name, RowGranularity rowGranularity) {
        return new RelationBuilder<>(name, rowGranularity);
    }

    static class ObjectExpression<T> implements Function<T, Map<String, Object>> {

        private final List<Column<T, ?>> columns;
        private final Predicate<T> objectIsNull;

        ObjectExpression(List<Column<T, ?>> columns, Predicate<T> objectIsNull) {
            this.columns = columns;
            this.objectIsNull = objectIsNull;
        }

        @Override
        public Map<String, Object> apply(T t) {
            if (objectIsNull.test(t)) {
                return null;
            }
            HashMap<String, Object> values = new HashMap<>(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                Column<T, ?> column = columns.get(i);
                try {
                    Object value = column.getProperty.apply(t);
                    values.put(column.column.leafName(), value);
                } catch (NullPointerException ignored) {
                }
            }
            return values;
        }
    }
}
