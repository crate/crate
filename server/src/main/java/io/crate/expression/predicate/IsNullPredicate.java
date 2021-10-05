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

package io.crate.expression.predicate;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.ExistsQueryBuilder;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class IsNullPredicate<T> extends Scalar<Boolean, T> {

    public static final String NAME = "op_isnull";
    public static final Signature SIGNATURE = Signature.scalar(
        NAME,
        parseTypeSignature("E"),
        DataTypes.BOOLEAN.getTypeSignature()
    ).withTypeVariableConstraints(typeVariable("E"));

    public static void register(PredicateModule module) {
        module.register(
            SIGNATURE,
            IsNullPredicate::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    private IsNullPredicate(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        assert symbol != null : "function must not be null";
        assert symbol.arguments().size() == 1 : "function's number of arguments must be 1";

        Symbol arg = symbol.arguments().get(0);
        if (arg.equals(Literal.NULL)) {
            return Literal.of(true);
        } else if (arg.symbolType().isValueSymbol()) {
            return Literal.of(((Input<?>) arg).value() == null);
        }
        return symbol;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        assert args.length == 1 : "number of args must be 1";
        return args[0] == null || args[0].value() == null;
    }

    @Override
    public Query toQuery(Function function, Context context) {
        List<Symbol> arguments = function.arguments();
        assert arguments.size() == 1 : "`<expression> IS NULL` function must have one argument";
        if (!(arguments.get(0) instanceof Reference ref)) {
            return null;
        }
        String columnName = ref.column().fqn();

        // ExistsQueryBuilder.newFilter doesn't build the correct exists query for arrays
        // because ES isn't really aware of explicit array types
        if (ref.valueType() instanceof ArrayType) {
            MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
            if (fieldType != null) {
                return Queries.not(fieldType.existsQuery(context.queryShardContext()));
            }
            MappedFieldType fieldNames = context.getFieldTypeOrNull(FieldNamesFieldMapper.NAME);
            if (fieldNames != null) {
                return Queries.not(new ConstantScoreQuery(
                    new TermQuery(new Term(FieldNamesFieldMapper.NAME, columnName))));
            }
        }
        if (ref.columnPolicy() == ColumnPolicy.IGNORED) {
            // There won't be any indexed field names for the children, need to use expensive query fallback
            return null;
        }
        return Queries.not(ExistsQueryBuilder.newFilter(context.queryShardContext(), columnName));
    }
}
