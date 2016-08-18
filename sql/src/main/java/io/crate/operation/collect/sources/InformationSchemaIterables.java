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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import io.crate.metadata.*;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.collect.files.SqlFeaturesIterable;
import io.crate.operation.reference.information.ColumnContext;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Iterator;

public class InformationSchemaIterables {

    private final Supplier<Iterable<?>> schemas;
    private final Supplier<Iterable<?>> tablesGetter;
    private final Supplier<Iterable<?>> partitionsGetter;
    private final Supplier<Iterable<?>> columnsGetter;
    private final Supplier<Iterable<?>> constraintsGetter;
    private final Supplier<Iterable<?>> routinesGetter;
    private final Supplier<Iterable<?>> featuresGetter;

    @Inject
    public InformationSchemaIterables(final Schemas schemas,
                                      FulltextAnalyzerResolver ftResolver,
                                      ClusterService clusterService) {
        this.schemas = Suppliers.<Iterable<?>>ofInstance(schemas);
        FluentIterable<TableInfo> tablesIterable = FluentIterable.from(schemas)
                .transformAndConcat(new Function<SchemaInfo, Iterable<TableInfo>>() {
                    @Nullable
                    @Override
                    public Iterable<TableInfo> apply(SchemaInfo input) {
                        assert input != null;
                        // filter out partitions
                        return FluentIterable.from(input).filter(new Predicate<TableInfo>() {
                            @Override
                            public boolean apply(TableInfo input) {
                                assert input != null;
                                return !PartitionName.isPartition(input.ident().indexName());
                            }
                        });
                    }
                });
        tablesGetter = Suppliers.<Iterable<?>>ofInstance(tablesIterable);
        partitionsGetter = Suppliers.<Iterable<?>>ofInstance(new PartitionInfos(clusterService));
        FluentIterable<ColumnContext> columnsIterable = tablesIterable.transformAndConcat(
                new Function<TableInfo, Iterable<ColumnContext>>() {
                    @Nullable
                    @Override
                    public Iterable<ColumnContext> apply(TableInfo input) {
                        assert input != null;
                        return new ColumnsIterator(input);
                    }
                });
        columnsGetter = Suppliers.<Iterable<?>>ofInstance(columnsIterable);
        constraintsGetter = Suppliers.<Iterable<?>>ofInstance(tablesIterable.filter(new Predicate<TableInfo>() {
            @Override
            public boolean apply(@Nullable TableInfo input) {
                return input != null && input.primaryKey().size() > 0;
            }
        }));

        RoutineInfos routineInfos = new RoutineInfos(ftResolver);
        routinesGetter = Suppliers.<Iterable<?>>ofInstance(FluentIterable.from(routineInfos)
                .filter(new Predicate<RoutineInfo>() {
                    @Override
                    public boolean apply(@Nullable RoutineInfo input) {
                        return input != null;
                    }
                }));
        featuresGetter = Suppliers.<Iterable<?>>ofInstance(SqlFeaturesIterable.getInstance());
    }

    public Supplier<Iterable<?>> schemas() {
        return schemas;
    }

    public Supplier<Iterable<?>> tables() {
        return tablesGetter;
    }

    public Supplier<Iterable<?>> partitions() {
        return partitionsGetter;
    }

    public Supplier<Iterable<?>> columns() {
        return columnsGetter;
    }

    public Supplier<Iterable<?>> constraints() {
        return constraintsGetter;
    }

    public Supplier<Iterable<?>> routines() {
        return routinesGetter;
    }

    public Supplier<Iterable<?>> features() {
        return featuresGetter;
    }

    static class ColumnsIterator implements Iterator<ColumnContext>, Iterable<ColumnContext> {

        private final ColumnContext context = new ColumnContext();
        private final Iterator<Reference> columns;

        ColumnsIterator(TableInfo ti) {
            context.tableInfo = ti;
            context.ordinal = 0;
            columns = FluentIterable.from(ti).filter(new Predicate<Reference>() {
                @Override
                public boolean apply(@Nullable Reference input) {
                    return input != null
                            && !input.ident().columnIdent().isSystemColumn()
                            && input.valueType() != DataTypes.NOT_SUPPORTED;
                }
            }).iterator();
        }

        @Override
        public boolean hasNext() {
            return columns.hasNext();
        }

        @Override
        public ColumnContext next() {
            context.info = columns.next();
            context.ordinal++;
            return context;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not allowed");
        }

        @Override
        public Iterator<ColumnContext> iterator() {
            return this;
        }

    }
}
