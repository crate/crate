package io.crate.analyze.where;


import com.google.common.collect.ImmutableSet;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.GteOperator;
import io.crate.operation.operator.any.AnyEqOperator;
import io.crate.operation.predicate.NotPredicate;
import io.crate.sql.tree.ComparisonExpression;

import java.util.Locale;
import java.util.Set;
import java.util.Stack;

public abstract class WhereClauseValidator {

    private static final Visitor visitor = new Visitor();

    public static void validate(WhereClause whereClause) {
        if (whereClause.hasQuery()) {
            visitor.process(whereClause.query(), new Visitor.Context());
        }
    }

    private static class Visitor extends SymbolVisitor<Visitor.Context, Symbol> {

        static class Context {

            private final Stack<Function> functions = new Stack<>();

            private Context() {
            }
        }

        private static final String _SCORE = "_score";
        private static final Set<String> SCORE_ALLOWED_COMPARISONS = ImmutableSet.of(GteOperator.NAME);

        private static final String _VERSION = "_version";
        private static final Set<String> VERSION_ALLOWED_COMPARISONS = ImmutableSet.of(EqOperator.NAME, AnyEqOperator.NAME);

        private static final String VERSION_ERROR = "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value";

        private static final String SCORE_ERROR = String.format(Locale.ENGLISH,
            "System column '%s' can only be used within a '%s' comparison without any surrounded predicate",
            _SCORE, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL.getValue());

        @Override
        public Symbol visitField(Field field, Context context) {
            validateSysReference(context, field.path().outputName());
            return super.visitField(field, context);
        }

        @Override
        public Symbol visitReference(Reference symbol, Context context) {
            validateSysReference(context, symbol.ident().columnIdent().name());
            return super.visitReference(symbol, context);
        }

        @Override
        public Symbol visitFunction(Function function, Context context) {
            context.functions.push(function);
            continueTraversal(function, context);
            context.functions.pop();
            return function;
        }

        private Function continueTraversal(Function symbol, Context context) {
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            return symbol;
        }

        private boolean insideNotPredicate(Context context) {
            for (Function function : context.functions) {
                if (function.info().ident().name().equals(NotPredicate.NAME)) {
                    return true;
                }
            }
            return false;
        }

        private void validateSysReference(Context context, String columnName) {
            if (columnName.equalsIgnoreCase(_VERSION)) {
                validateSysReference(context, VERSION_ALLOWED_COMPARISONS, VERSION_ERROR);
            } else if (columnName.equalsIgnoreCase(_SCORE)) {
                validateSysReference(context, SCORE_ALLOWED_COMPARISONS, SCORE_ERROR);
            } else if (columnName.equalsIgnoreCase(DocSysColumns.RAW.name())) {
                throw new UnsupportedOperationException("The _raw column is not searchable and cannot be used inside a query");
            }
        }

        private void validateSysReference(Context context, Set<String> requiredFunctionNames, String error) {
            if (context.functions.isEmpty()) {
                throw new UnsupportedOperationException(error);
            }
            Function function = context.functions.lastElement();
            if (!requiredFunctionNames.contains(function.info().ident().name().toLowerCase(Locale.ENGLISH))
                || insideNotPredicate(context)) {
                throw new UnsupportedOperationException(error);
            }
            assert function.arguments().size() == 2 : "function's number of arguments must be 2";
            Symbol right = function.arguments().get(1);
            if (!right.symbolType().isValueSymbol()) {
                throw new UnsupportedOperationException(error);
            }
        }
    }
}
