package io.crate.analyze.where;


import io.crate.analyze.WhereClause;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.GteOperator;
import io.crate.operation.predicate.NotPredicate;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolVisitor;
import io.crate.sql.tree.ComparisonExpression;

import java.util.Locale;
import java.util.Stack;

public class WhereClauseValidator {

    private static final Visitor visitor = new Visitor();

    public Symbol validate(WhereClause whereClause) {
        return visitor.process(whereClause.query(), new Visitor.Context());
    }

    private static class Visitor extends SymbolVisitor<Visitor.Context, Symbol> {

        public static class Context {

            public final Stack<Function> functions = new Stack();
        }

        private final static String _SCORE = "_score";

        private final static String _VERSION = "_version";

        private final static String VERSION_ERROR = "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value";

        private final static String SCORE_ERROR = String.format(Locale.ENGLISH,
                "System column '%s' can only be used within a '%s' comparison without any surrounded predicate",
                _SCORE, ComparisonExpression.Type.GREATER_THAN_OR_EQUAL.getValue());

        @Override
        public Symbol visitReference(Reference reference, Context context) {
            String columnName = reference.info().ident().columnIdent().name();
            if(columnName.equalsIgnoreCase(_VERSION)){
                validateSysReference(context, EqOperator.NAME, VERSION_ERROR);
            } else if(columnName.equalsIgnoreCase(_SCORE)){
                validateSysReference(context, GteOperator.NAME, SCORE_ERROR);
            }
            return super.visitReference(reference, context);
        }

        @Override
        public Symbol visitFunction(Function function, Context context){
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

        private boolean insideNotPredicate(Context context){
            for(Function function : context.functions){
                if(function.info().ident().name().equals(NotPredicate.NAME)){
                    return true;
                }
            }
            return false;
        }

        private void validateSysReference(Context context, String requiredFunctionName, String error) {
            if(context.functions.isEmpty()){
                throw new UnsupportedOperationException(error);
            }
            Function function = context.functions.lastElement();
            if(!function.info().ident().name().equalsIgnoreCase(requiredFunctionName)
                    || insideNotPredicate(context)) {
                throw new UnsupportedOperationException(error);
            }
            assert function.arguments().size() == 2;
            Symbol right = function.arguments().get(1);
            if(!right.symbolType().isValueSymbol()){
                throw new UnsupportedOperationException(error);
            }
        }
    }
}
