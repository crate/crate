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
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersioningValidationException;
import io.crate.execution.engine.collect.DocInputFactory;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.reference.doc.lucene.StoredRowLookup;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferences;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.EqQuery;


@Singleton
public class LuceneQueryBuilder {

    public static final Setting<Integer> INDICES_MAX_CLAUSE_COUNT_SETTING = Setting.intSetting(
        "indices.query.bool.max_clause_count",
        8192,
        1,
        Integer.MAX_VALUE,
        Setting.Property.NodeScope
    );

    private static final Logger LOGGER = LogManager.getLogger(LuceneQueryBuilder.class);
    private static final Visitor VISITOR = new Visitor();
    private final NodeContext nodeCtx;

    @Inject
    public LuceneQueryBuilder(NodeContext nodeCtx) {
        this.nodeCtx = nodeCtx;
    }

    public Context convert(Symbol query,
                           TransactionContext txnCtx,
                           String indexName,
                           IndexAnalyzers indexAnalyzers,
                           DocTableInfo table,
                           QueryCache queryCache) throws UnsupportedFeatureException {
        var refResolver = new LuceneReferenceResolver(
            indexName,
            table.partitionedByColumns()
        );
        var normalizer = new EvaluatingNormalizer(nodeCtx, RowGranularity.PARTITION, refResolver, null);
        Context ctx = new Context(
            table,
            txnCtx,
            nodeCtx,
            queryCache,
            indexAnalyzers,
            indexName,
            table.partitionedByColumns(),
            query
        );
        ctx.query = eliminateNullsIfPossible(
            inverseSourceLookup(normalizer.normalize(query, txnCtx)),
            s -> normalizer.normalize(s, txnCtx)
        ).accept(VISITOR, ctx);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("WHERE CLAUSE [{}] -> LUCENE QUERY [{}] ", query.toString(Style.UNQUALIFIED), ctx.query);
        }
        return ctx;
    }

    public static class Context {
        Query query;

        final Map<String, Object> filteredFieldValues = new HashMap<>();

        private final DocTableInfo table;
        final DocInputFactory docInputFactory;
        final QueryCache queryCache;
        private final TransactionContext txnCtx;
        private final IndexAnalyzers indexAnalyzers;
        private final String indexName;

        final NodeContext nodeContext;
        private final Symbol parentQuery;

        Context(DocTableInfo table,
                TransactionContext txnCtx,
                NodeContext nodeCtx,
                QueryCache queryCache,
                IndexAnalyzers indexAnalyzers,
                String indexName,
                List<Reference> partitionColumns,
                Symbol parentQuery) {
            this.table = table;
            this.nodeContext = nodeCtx;
            this.txnCtx = txnCtx;
            this.indexAnalyzers = indexAnalyzers;
            this.docInputFactory = new DocInputFactory(
                nodeCtx,
                new LuceneReferenceResolver(
                    indexName,
                    partitionColumns
                )
            );
            this.queryCache = queryCache;
            this.parentQuery = parentQuery;
            this.indexName = indexName;
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
            "_version", VersioningValidationException.VERSION_COLUMN_USAGE_MSG,
            "_seq_no", VersioningValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG,
            "_primary_term", VersioningValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG
        );

        public NamedAnalyzer getAnalyzer(String analyzerName) {
            NamedAnalyzer namedAnalyzer = indexAnalyzers.get(analyzerName);
            if (namedAnalyzer == null) {
                throw new IllegalArgumentException("No analyzer found for [" + analyzerName + "]");
            }
            return namedAnalyzer;
        }

        public SymbolVisitor<Context, Query> visitor() {
            return VISITOR;
        }

        @Nullable
        public Reference getRef(String storageIdent) {
            return table.getReference(storageIdent);
        }

        @Nullable
        public Reference getRef(ColumnIdent column) {
            return table.getReadReference(column);
        }

        public DocTableInfo tableInfo() {
            return table;
        }

        public TransactionContext transactionContext() {
            return txnCtx;
        }

        public NodeContext nodeContext() {
            return nodeContext;
        }

        public Symbol parentQuery() {
            return parentQuery;
        }
    }


    static class Visitor extends SymbolVisitor<Context, Query> {

        @Override
        public Query visitFunction(Function function, Context context) {
            assert function != null : "function must not be null";
            if (fieldIgnored(function, context)) {
                return new MatchAllDocsQuery();
            }
            function = rewriteAndValidateFields(function, context);

            FunctionImplementation implementation = context.nodeContext.functions().getQualified(function);
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
                    FunctionImplementation implementation = context.nodeContext.functions().getQualified(inner);
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
                    context.filteredFieldValues.put(columnName, ((Input<?>) right).value());
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
                    if (ref.column().equals(SysColumns.UID)) {
                        return new Function(
                            function.signature(),
                            List.of(SysColumns.forTable(ref.ident().tableIdent(), SysColumns.ID.COLUMN), right),
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
            DataType<?> type = ref.valueType();
            // called for queries like: where boolColumn
            if (type == DataTypes.BOOLEAN) {
                EqQuery<? super Boolean> eqQuery = DataTypes.BOOLEAN.storageSupportSafe().eqQuery();
                return eqQuery.termQuery(
                    ref.storageIdent(), Boolean.TRUE, ref.hasDocValues(), ref.indexType() != IndexType.NONE);
            }
            return super.visitReference(ref, context);
        }

        @Override
        public Query visitLiteral(Literal<?> literal, Context context) {
            Object value = literal.value();
            if (value == null) {
                return new MatchNoDocsQuery("WHERE null -> no match");
            }
            try {
                return (boolean) value
                    ? new MatchAllDocsQuery()
                    : new MatchNoDocsQuery("WHERE false -> no match");
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
        function = (Function) DocReferences.toDocLookup(function,
            r -> r.columnPolicy() == ColumnPolicy.IGNORED
                 || r.valueType() == DataTypes.GEO_POINT);

        final InputFactory.Context<? extends LuceneCollectorExpression<?>> ctx = context.docInputFactory.getCtx(context.txnCtx);
        @SuppressWarnings("unchecked")
        final Input<Boolean> condition = (Input<Boolean>) ctx.add(function);
        final Collection<? extends LuceneCollectorExpression<?>> expressions = ctx.expressions();
        final CollectorContext collectorContext
            = new CollectorContext(() -> StoredRowLookup.create(context.table, context.indexName));
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
