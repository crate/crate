/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.relations;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.*;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Path;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.*;

public abstract class AbstractTableRelation<T extends TableInfo> implements AnalyzedRelation, FieldResolver {

    private static final Predicate<ReferenceInfo> IS_OBJECT_ARRAY = new Predicate<ReferenceInfo>() {
        @Override
        public boolean apply(@Nullable ReferenceInfo input) {
            return input != null
                    && input.type().id() == ArrayType.ID
                    && ((ArrayType) input.type()).innerType().equals(DataTypes.OBJECT);
        }
    };

    protected T tableInfo;
    private List<Field> outputs;
    private final static FieldUnwrappingVisitor FIELD_UNWRAPPING_VISITOR = new FieldUnwrappingVisitor();
    private Map<Path, Reference> allocatedFields = new HashMap<>();

    public AbstractTableRelation(T tableInfo) {
        this.tableInfo = tableInfo;
    }

    public T tableInfo() {
        return tableInfo;
    }

    @Nullable
    @Override
    public Field getField(Path path) {
        ColumnIdent ci = getColumnIdentSafe(path);
        ReferenceInfo referenceInfo = tableInfo.getReferenceInfo(ci);
        if (referenceInfo == null) {
            return null;
        }
        referenceInfo = checkNestedArray(ci, referenceInfo);
        return allocate(ci, new Reference(referenceInfo));

    }

    protected ReferenceInfo checkNestedArray(ColumnIdent ci, ReferenceInfo referenceInfo) {
        // TODO: build type correctly as array when the tableInfo is created and remove the conversion here
        if (!ci.isColumn() && hasMatchingParent(referenceInfo, IS_OBJECT_ARRAY)) {
            if (DataTypes.isCollectionType(referenceInfo.type())) {
                // TODO: remove this limitation with next type refactoring
                throw new UnsupportedOperationException("cannot query for arrays inside object arrays explicitly");
            }
            // for child fields of object arrays
            // return references of primitive types as array
            referenceInfo = new ReferenceInfo.Builder()
                    .ident(referenceInfo.ident())
                    .columnPolicy(referenceInfo.columnPolicy())
                    .granularity(referenceInfo.granularity())
                    .type(new ArrayType(referenceInfo.type()))
                    .build();
        }
        return referenceInfo;
    }

    @Override
    public Field getWritableField(Path path) throws ColumnUnknownException {
        throw new UnsupportedOperationException("getWritableField() not implemented on TableRelation");
    }


    protected ColumnIdent getColumnIdentSafe(Path path) {
        ColumnIdent ci;
        if (path instanceof ColumnIdent) {
            ci = (ColumnIdent) path;
        } else {
            throw new UnsupportedOperationException("TableRelation requires a ColumnIdent as path to get a field");
        }
        return ci;
    }

    protected Field allocate(Path path, Reference reference) {
        allocatedFields.put(path, reference);
        return new Field(this, path, reference.valueType());
    }

    @Override
    public List<Field> fields() {
        if (outputs == null) {
            outputs = new ArrayList<>(tableInfo.columns().size());
            for (ReferenceInfo referenceInfo : tableInfo.columns()) {
                if (referenceInfo.type().equals(DataTypes.NOT_SUPPORTED)) {
                    continue;
                }
                ColumnIdent columnIdent = referenceInfo.ident().columnIdent();
                outputs.add(getField(columnIdent));
            }
        }
        return outputs;
    }

    /**
     * return true if the given {@linkplain com.google.common.base.Predicate}
     * returns true for a parent column of this one.
     * returns false if info has no parent column.
     */
    private boolean hasMatchingParent(ReferenceInfo info, Predicate<ReferenceInfo> parentMatchPredicate) {
        ColumnIdent parent = info.ident().columnIdent().getParent();
        while (parent != null) {
            ReferenceInfo parentInfo = tableInfo.getReferenceInfo(parent);
            if (parentMatchPredicate.apply(parentInfo)) {
                return true;
            }
            parent = parent.getParent();
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("table", tableInfo.ident()).toString();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractTableRelation that = (AbstractTableRelation) o;

        if (!tableInfo.equals(that.tableInfo)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return tableInfo.hashCode();
    }

    @Deprecated
    public WhereClause resolve(WhereClause whereClause) {
        if (whereClause.hasQuery()) {
            return new WhereClause(resolve(whereClause.query()), whereClause.docKeys().orNull(),
                    whereClause.partitions());
        }
        return whereClause;
    }

    @Override
    @Nullable
    public Reference resolveField(Field field) {
        if (field.relation() == this) {
            return allocatedFields.get(field.path());
        }
        return null;
    }

    @Deprecated
    public Symbol resolve(Symbol symbol) {
        assert symbol != null : "can't resolve symbol that is null";
        return FIELD_UNWRAPPING_VISITOR.process(symbol, this);
    }

    @Deprecated
    private static class FieldUnwrappingVisitor extends SymbolVisitor<AbstractTableRelation, Symbol> {

        private Reference resolveField(Field field, AbstractTableRelation relation) {
            return relation.resolveField(field);
        }

        @Override
        public Symbol visitField(Field field, AbstractTableRelation context) {
            return resolveField(field, context);
        }

        @Override
        public Symbol visitFunction(Function symbol, AbstractTableRelation context) {
            for (int i = 0; i < symbol.arguments().size(); i++) {
                symbol.setArgument(i, process(symbol.arguments().get(i), context));
            }
            return symbol;
        }

        @Override
        public Symbol visitMatchPredicate(MatchPredicate matchPredicate, AbstractTableRelation context) {
            Map<Field, Double> fieldBoostMap = matchPredicate.identBoostMap();
            Map<String, Object> fqnBoostMap = new HashMap<>(fieldBoostMap.size());

            for (Map.Entry<Field, Double> entry : fieldBoostMap.entrySet()) {
                fqnBoostMap.put(resolveField(entry.getKey(), context).info().ident().columnIdent().fqn(), entry.getValue());
            }

            return new Function(
                    io.crate.operation.predicate.MatchPredicate.INFO,
                    Arrays.<Symbol>asList(
                            Literal.newLiteral(fqnBoostMap),
                            Literal.newLiteral(matchPredicate.columnType(), matchPredicate.queryTerm()),
                            Literal.newLiteral(matchPredicate.matchType()),
                            Literal.newLiteral(matchPredicate.options())));
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, AbstractTableRelation context) {
            return symbol;
        }
    }

    public void validateOrderBy(Optional<OrderBy> orderBy) {
        return;
    }
}
