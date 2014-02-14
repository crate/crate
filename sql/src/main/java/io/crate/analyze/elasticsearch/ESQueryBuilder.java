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

package io.crate.analyze.elasticsearch;


import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.lucene.SQLToLuceneHelper;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.operator.*;
import io.crate.operator.scalar.MatchFunction;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.ESDeleteByQueryNode;
import io.crate.planner.node.ESSearchNode;
import io.crate.planner.symbol.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.*;

public class ESQueryBuilder {

    private final EvaluatingNormalizer normalizer;
    private final static Visitor visitor = new Visitor();

    /**
     * these fields are ignored in the whereClause
     * (only applies to Function with 2 arguments and if left == reference and right == literal)
     */
    private final static Set<String> filteredFields = ImmutableSet.of("_score");

    /**
     * key = columnName
     * value = error message
     *
     * (in the _version case if the primary key is present a GetPlan is built from the planner and
     *  the ESQueryBuilder is never used)
     */
    private final static Map<String, String> unsupportedFields = ImmutableMap.<String, String>builder()
            .put("_version", "\"_version\" column is only valid in the WHERE clause if the primary key column is also present")
            .build();

    /**
     * Create a ESQueryBuilder to convert a whereClause to XContent or a ESSearchNode to XContent
     *
     * @param functions         needs to contain at least the Operator functions
     * @param referenceResolver is only required if the outputs() of the ESSearchNode contains
     *                          INFOS with CLUSTER RowGranularity.
     */
    public ESQueryBuilder(Functions functions, ReferenceResolver referenceResolver) {
        normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);
    }

    /**
     * adds the "query" part to the XContentBuilder
     */
    private void whereClause(Context context, Optional<Function> whereClause) throws IOException {
        if (whereClause.isPresent()) {
            /**
             * normalize to optimize queries like eq(1, 1)
             */
            Symbol normalizedClause = normalizer.process(whereClause.get(), null);
            visitor.process(normalizedClause, context);
        } else {
            context.builder.field("match_all", new HashMap<>());
        }
    }

    /**
     * use to generate the "query" xcontent
     */
    public BytesReference convert(@Nullable Function whereClause) throws IOException {
        return convert(Optional.fromNullable(whereClause));
    }

    /**
     * use to generate the "query" xcontent
     */
    public BytesReference convert(Optional<Function> whereClause) throws IOException {
        Context context = new Context();
        context.builder = XContentFactory.jsonBuilder().startObject();
        context.builder.startObject("query");
        whereClause(context, whereClause);
        context.builder.endObject();
        context.builder.endObject();
        return context.builder.bytes();
    }

    /**
     * use to create a full elasticsearch query "statement" including fields, size, etc.
     */
    public BytesReference convert(ESSearchNode node, List<Reference> outputs) throws IOException {
        Preconditions.checkNotNull(node);
        Preconditions.checkNotNull(outputs);

        Context context = new Context();
        context.builder = XContentFactory.jsonBuilder().startObject();
        XContentBuilder builder = context.builder;

        Set<String> fields = new HashSet<>();
        for (Reference output : outputs) {
            fields.add(output.info().ident().columnIdent().fqn());
        }
        builder.field("fields", fields);

        if (fields.contains("_version")) {
            builder.field("version", true);
        }

        builder.startObject("query");
        whereClause(context, node.whereClause());
        builder.endObject();

        if (context.ignoredFields.containsKey("_score")) {
            builder.field("min_score", ((Number) context.ignoredFields.get("_score")).doubleValue());
        }

        addSorting(node.orderBy(), node.reverseFlags(), context.builder);

        builder.field("from", node.offset());
        builder.field("size", node.limit());

        builder.endObject();
        return builder.bytes();
    }

    /**
     * use to create a full elasticsearch query "statement" used by deleteByQuery actions.
     */
    public BytesReference convert(ESDeleteByQueryNode node) throws IOException {
        Preconditions.checkNotNull(node);

        Context context = new Context();
        context.builder = XContentFactory.jsonBuilder().startObject();
        XContentBuilder builder = context.builder;

        whereClause(context, node.whereClause());

        builder.endObject();
        return builder.bytes();
    }

    private void addSorting(List<Reference> orderBy, boolean[] reverseFlags, XContentBuilder builder) throws IOException {
        if (orderBy.isEmpty()) {
            return;
        }

        builder.startArray("sort");
        int i = 0;
        for (Reference reference : orderBy) {
            builder.startObject()
                    .startObject(reference.info().ident().columnIdent().fqn())
                    .field("order", reverseFlags[i] ? "desc" : "asc")
                    .field("ignore_unmapped", true)
                    .endObject()
                    .endObject();
            i++;
        }
        builder.endArray();
    }

    class Context {
        XContentBuilder builder;
        Map<String, Object> ignoredFields = new HashMap<>();
    }

    static class Visitor extends SymbolVisitor<Context, Void> {

        abstract class Converter {

            public abstract void convert(Function function, Context context) throws IOException;
        }

        class AndConverter extends Converter {

            @Override
            public void convert(Function function, Context context) throws IOException {
                context.builder.startObject("bool").startArray("must");

                for (Symbol symbol : function.arguments()) {
                    context.builder.startObject();
                    process(symbol, context);
                    context.builder.endObject();
                }
                context.builder.endArray().endObject();
            }
        }

        class OrConverter extends Converter {

            @Override
            public void convert(Function function, Context context) throws IOException {
                context.builder.startObject("bool").field("minimum_should_match", 1).startArray("should");

                for (Symbol symbol : function.arguments()) {
                    context.builder.startObject();
                    process(symbol, context);
                    context.builder.endObject();
                }

                context.builder.endArray().endObject();
            }
        }

        abstract class CmpConverter extends Converter {

            protected Tuple<String, Object> prepare(Function function) {
                Preconditions.checkNotNull(function);
                Preconditions.checkArgument(function.arguments().size() == 2);

                Symbol left = function.arguments().get(0);
                Symbol right = function.arguments().get(1);

                if (left.symbolType() == SymbolType.FUNCTION || right.symbolType() == SymbolType.FUNCTION) {
                    raiseUnsupported(function);
                }

                assert left.symbolType() == SymbolType.REFERENCE;
                assert right.symbolType().isLiteral();

                return new Tuple<>(
                        ((Reference) left).info().ident().columnIdent().fqn(),
                        ((Literal) right).value()
                );
            }
        }

        class EqConverter extends CmpConverter {
            @Override
            public void convert(Function function, Context context) throws IOException {
                Tuple<String, Object> tuple = super.prepare(function);
                context.builder.startObject("term").field(tuple.v1(), tuple.v2()).endObject();
            }
        }

        class LikeConverter extends CmpConverter {

            @Override
            public void convert(Function function, Context context) throws IOException {
                Tuple<String, Object> prepare = prepare(function);
                String like = prepare.v2().toString();
                like = SQLToLuceneHelper.convertWildcard(like);
                context.builder.startObject("wildcard").field(prepare.v1(), like).endObject();
            }
        }

        class IsNullConverter extends Converter {

            @Override
            public void convert(Function function, Context context) throws IOException {
                Preconditions.checkNotNull(function);
                Preconditions.checkArgument(function.arguments().size() == 1);

                Symbol arg = function.arguments().get(0);
                Preconditions.checkArgument(arg.symbolType() == SymbolType.REFERENCE);

                Reference reference = (Reference) arg;
                String columnName = reference.info().ident().columnIdent().fqn();

                context.builder.startObject("filtered").startObject("filter").startObject("missing")
                        .field("field", columnName)
                        .field("existence", true)
                        .field("null_value", true)
                        .endObject().endObject().endObject();
            }
        }

        class NotEqConverter extends CmpConverter {
            @Override
            public void convert(Function function, Context context) throws IOException {
                Tuple<String, Object> tuple = super.prepare(function);

                context.builder.startObject("bool").startObject("must_not");
                context.builder.startObject("term").field(tuple.v1(), tuple.v2()).endObject();
                context.builder.endObject().endObject();
            }
        }

        class NotConverter extends Converter {

            @Override
            public void convert(Function function, Context context) throws IOException {
                Preconditions.checkNotNull(function);
                Preconditions.checkArgument(function.arguments().size() == 1);

                context.builder.startObject("bool").startObject("must_not");
                process(function.arguments().get(0), context);
                context.builder.endObject().endObject();
            }
        }

        class RangeConverter extends CmpConverter {

            private final String operator;

            public RangeConverter(String operator) {
                this.operator = operator;
            }

            @Override
            public void convert(Function function, Context context) throws IOException {
                Tuple<String, Object> tuple = super.prepare(function);
                context.builder.startObject("range")
                        .startObject(tuple.v1()).field(operator, tuple.v2()).endObject()
                        .endObject();
            }
        }

        class MatchConverter extends CmpConverter {

            @Override
            public void convert(Function function, Context context) throws IOException {
                Tuple<String, Object> tuple = super.prepare(function);
                context.builder.startObject("match").field(tuple.v1(), tuple.v2()).endObject();
            }
        }

        private ImmutableMap<String, Converter> functions =
                ImmutableMap.<String, Converter>builder()
                        .put(AndOperator.NAME, new AndConverter())
                        .put(OrOperator.NAME, new OrConverter())
                        .put(EqOperator.NAME, new EqConverter())
                        .put(LtOperator.NAME, new RangeConverter("lt"))
                        .put(LteOperator.NAME, new RangeConverter("lte"))
                        .put(GtOperator.NAME, new RangeConverter("gt"))
                        .put(GteOperator.NAME, new RangeConverter("gte"))
                        .put(NotEqOperator.NAME, new NotEqConverter())
                        .put(LikeOperator.NAME, new LikeConverter())
                        .put(IsNullOperator.NAME, new IsNullConverter())
                        .put(NotOperator.NAME, new NotConverter())
                        .put(MatchFunction.NAME, new MatchConverter())
                        .build();

        @Override
        public Void visitFunction(Function function, Context context) {
            Preconditions.checkNotNull(function);

            try {
                if (fieldIgnored(function, context)) {
                    context.builder.field("match_all", new HashMap<>());
                    return null;
                }

                Converter converter = functions.get(function.info().ident().name());
                if (converter == null) {
                    return raiseUnsupported(function);
                }
                converter.convert(function, context);

            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            return null;
        }

        private boolean fieldIgnored(Function function, Context context) {
            if (function.arguments().size() == 2) {
                Symbol left = function.arguments().get(0);
                Symbol right = function.arguments().get(1);

                if (left.symbolType() == SymbolType.REFERENCE && right.symbolType().isLiteral()) {
                    String columnName = ((Reference) left).info().ident().columnIdent().name();
                    if (filteredFields.contains(columnName)) {
                        context.ignoredFields.put(columnName, ((Literal) right).value());
                        return true;
                    }

                    String unsupported = unsupportedFields.get(columnName);
                    if (unsupported != null) {
                        throw new UnsupportedOperationException(unsupported);
                    }
                }
            }
            return false;
        }

        private Void raiseUnsupported(Function function) {
            throw new UnsupportedOperationException(
                    String.format("Cannot convert function <%s> into a query", function));
        }
    }
}
