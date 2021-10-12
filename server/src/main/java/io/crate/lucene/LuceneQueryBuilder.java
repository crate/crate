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

package io.crate.lucene;

import static io.crate.expression.eval.NullEliminator.eliminateNullsIfPossible;
import static io.crate.metadata.DocReferences.inverseSourceLookup;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;

import io.crate.data.Input;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersioninigValidationException;
import io.crate.execution.engine.collect.DocInputFactory;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.DocReferences;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Reference.IndexType;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;


@Singleton
public class LuceneQueryBuilder {

    private static final Logger LOGGER = LogManager.getLogger(LuceneQueryBuilder.class);
    private static final Visitor VISITOR = new Visitor();
    private final NodeContext nodeCtx;

    @Inject
    public LuceneQueryBuilder(NodeContext nodeCtx) {
        this.nodeCtx = nodeCtx;
    }

    public Context convert(Symbol query,
                           TransactionContext txnCtx,
                           MapperService mapperService,
                           String indexName,
                           QueryShardContext queryShardContext,
                           DocTableInfo table,
                           IndexCache indexCache) throws UnsupportedFeatureException {
        var refResolver = new LuceneReferenceResolver(
            indexName,
            mapperService::fullName,
            table.partitionedByColumns()
        );
        var normalizer = new EvaluatingNormalizer(nodeCtx, RowGranularity.PARTITION, refResolver, null);
        Context ctx = new Context(
            table,
            txnCtx,
            nodeCtx,
            mapperService,
            indexCache,
            queryShardContext,
            indexName,
            table.partitionedByColumns()
        );
        CoordinatorTxnCtx coordinatorTxnCtx = CoordinatorTxnCtx.systemTransactionContext();
        ctx.query = eliminateNullsIfPossible(
            inverseSourceLookup(normalizer.normalize(query, coordinatorTxnCtx)),
            s -> normalizer.normalize(s, coordinatorTxnCtx)
        ).accept(VISITOR, ctx);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("WHERE CLAUSE [{}] -> LUCENE QUERY [{}] ", query.toString(Style.UNQUALIFIED), ctx.query);
        }
        return ctx;
    }

    static List asList(Literal literal) {
        Object val = literal.value();
        return (List) ((List) val).stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    public static class Context {
        Query query;

        final Map<String, Object> filteredFieldValues = new HashMap<>();

        private final DocTableInfo table;
        final DocInputFactory docInputFactory;
        final MapperService mapperService;
        final IndexCache indexCache;
        private final TransactionContext txnCtx;
        final QueryShardContext queryShardContext;

        final NodeContext nodeContext;


        Context(DocTableInfo table,
                TransactionContext txnCtx,
                NodeContext nodeCtx,
                MapperService mapperService,
                IndexCache indexCache,
                QueryShardContext queryShardContext,
                String indexName,
                List<Reference> partitionColumns) {
            this.table = table;
            this.nodeContext = nodeCtx;
            this.txnCtx = txnCtx;
            this.queryShardContext = queryShardContext;
            FieldTypeLookup typeLookup = mapperService::fullName;
            this.docInputFactory = new DocInputFactory(
                nodeCtx,
                new LuceneReferenceResolver(
                    indexName,
                    typeLookup,
                    partitionColumns
                )
            );
            this.mapperService = mapperService;
            this.indexCache = indexCache;
        }

        public Query query() {
            return this.query;
        }

        @Nullable
        public Float minScore() {
            Object score = filteredFieldValues.get("_score");
            if (score == null) {
                return null;
            }
            return ((Number) score).floatValue();
        }

        @Nullable
        public String unsupportedMessage(String field) {
            return UNSUPPORTED_FIELDS.get(field);
        }

        /**
         * These fields are ignored in the whereClause.
         * If a filtered field is encountered the value of the literal is written into filteredFieldValues
         * (only applies to Function with 2 arguments and if left == reference and right == literal)
         */
        static final Set<String> FILTERED_FIELDS = Set.of("_score");

        /**
         * key = columnName
         * value = error message
         * <p/>
         * (in the _version case if the primary key is present a GetPlan is built from the planner and
         * the LuceneQueryBuilder is never used)
         */
        static final Map<String, String> UNSUPPORTED_FIELDS = Map.of(
            "_version", VersioninigValidationException.VERSION_COLUMN_USAGE_MSG,
            "_seq_no", VersioninigValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG,
            "_primary_term", VersioninigValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG
        );

        @Nullable
        public MappedFieldType getFieldTypeOrNull(String fqColumnName) {
            return mapperService.fullName(fqColumnName);
        }

        public QueryShardContext queryShardContext() {
            return queryShardContext;
        }

        public SymbolVisitor<Context, Query> visitor() {
            return VISITOR;
        }

        @Nullable
        public Reference getRef(ColumnIdent column) {
            return table.getReadReference(column);
        }
    }


    static class Visitor extends SymbolVisitor<Context, Query> {

        @Override
        public Query visitFunction(Function function, Context context) {
            assert function != null : "function must not be null";
            if (fieldIgnored(function, context)) {
                return Queries.newMatchAllQuery();
            }
            function = rewriteAndValidateFields(function, context);

            FunctionImplementation implementation = context.nodeContext.functions().getQualified(
                function,
                context.txnCtx.sessionSettings().searchPath()
            );
            if (implementation instanceof FunctionToQuery funcToQuery) {
                Query query;
                try {
                    query = funcToQuery.toQuery(function, context);
                    if (query == null) {
                        query = queryFromInnerFunction(function, context);
                    }
                } catch (UnsupportedOperationException e) {
                    return genericFunctionFilter(function, context);
                }
                if (query != null) {
                    return query;
                }
            }
            return genericFunctionFilter(function, context);
        }

        private Query queryFromInnerFunction(Function parent, Context context) {
            for (Symbol arg : parent.arguments()) {
                if (arg instanceof Function inner) {
                    FunctionImplementation implementation = context.nodeContext.functions().getQualified(
                        inner, context.txnCtx.sessionSettings().searchPath()
                    );
                    Query query = implementation instanceof FunctionToQuery funcToQuery
                        ? funcToQuery.toQuery(parent, inner, context)
                        : null;
                    if (query != null) {
                        return query;
                    }
                }
            }
            return null;
        }

        private static boolean fieldIgnored(Function function, Context context) {
            if (function.arguments().size() != 2) {
                return false;
            }
            Symbol left = function.arguments().get(0);
            Symbol right = function.arguments().get(1);
            if (left.symbolType() == SymbolType.REFERENCE && right.symbolType().isValueSymbol()) {
                String columnName = ((Reference) left).column().name();
                if (Context.FILTERED_FIELDS.contains(columnName)) {
                    context.filteredFieldValues.put(columnName, ((Input) right).value());
                    return true;
                }
                String unsupportedMessage = Context.UNSUPPORTED_FIELDS.get(columnName);
                if (unsupportedMessage != null) {
                    throw new UnsupportedFeatureException(unsupportedMessage);
                }
            }
            return false;
        }

        private static Function rewriteAndValidateFields(Function function, Context context) {
            List<Symbol> arguments = function.arguments();
            if (arguments.size() == 2) {
                Symbol left = arguments.get(0);
                Symbol right = arguments.get(1);
                if (left.symbolType() == SymbolType.REFERENCE && right.symbolType().isValueSymbol()) {
                    Reference ref = (Reference) left;
                    if (ref.column().equals(DocSysColumns.UID)) {
                        return new Function(
                            function.signature(),
                            List.of(DocSysColumns.forTable(ref.ident().tableIdent(), DocSysColumns.ID), right),
                            function.valueType()
                        );
                    } else {
                        String unsupportedMessage = context.unsupportedMessage(ref.column().name());
                        if (unsupportedMessage != null) {
                            throw new UnsupportedFeatureException(unsupportedMessage);
                        }
                    }
                }
            }
            return function;
        }


        @Override
        public Query visitReference(Reference ref, Context context) {
            // called for queries like: where boolColumn
            if (ref.valueType() == DataTypes.BOOLEAN) {
                if (ref.indexType() == IndexType.NONE) {
                    return Queries.newMatchNoDocsQuery("column does not exist in this index");
                }
                return new TermQuery(new Term(ref.column().fqn(), new BytesRef("T")));
            }
            return super.visitReference(ref, context);
        }

        @Override
        public Query visitLiteral(Literal literal, Context context) {
            Object value = literal.value();
            if (value == null) {
                return Queries.newMatchNoDocsQuery("WHERE null -> no match");
            }
            try {
                return (boolean) value
                    ? Queries.newMatchAllQuery()
                    : Queries.newMatchNoDocsQuery("WHERE false -> no match");
            } catch (ClassCastException e) {
                // Throw a nice error if the top-level literal doesn't have a boolean type
                // (This is currently caught earlier, so this code is just a safe-guard)
                return visitSymbol(literal, context);
            }
        }

        @Override
        protected Query visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException(
                    Symbols.format("Can't build query from symbol %s", symbol));
        }
    }

    public static Query genericFunctionFilter(Function function, Context context) {
        if (function.valueType() != DataTypes.BOOLEAN) {
            raiseUnsupported(function);
        }
        // rewrite references to source lookup instead of using the docValues column store if:
        // - no docValues are available for the related column, currently only on objects defined as `ignored`
        // - docValues value differs from source, currently happening on GeoPoint types as lucene's internal format
        //   results in precision changes (e.g. longitude 11.0 will be 10.999999966)
        function = (Function) DocReferences.toSourceLookup(function,
            r -> r.columnPolicy() == ColumnPolicy.IGNORED
                 || r.valueType() == DataTypes.GEO_POINT);

        final InputFactory.Context<? extends LuceneCollectorExpression<?>> ctx = context.docInputFactory.getCtx(context.txnCtx);
        @SuppressWarnings("unchecked")
        final Input<Boolean> condition = (Input<Boolean>) ctx.add(function);
        final Collection<? extends LuceneCollectorExpression<?>> expressions = ctx.expressions();
        final CollectorContext collectorContext = new CollectorContext();
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
        }
        return new GenericFunctionQuery(function, expressions, condition);
    }

    private static void raiseUnsupported(Function function) {
        throw new UnsupportedOperationException(
                Symbols.format("Cannot convert function %s into a query", function));
    }

}
