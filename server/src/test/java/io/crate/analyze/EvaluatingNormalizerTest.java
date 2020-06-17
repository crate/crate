package io.crate.analyze;

import io.crate.action.sql.SessionContext;
import io.crate.expression.NestableInput;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.MapBackedRefResolver;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.functions.Signature;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.CoreMatchers.instanceOf;

public class EvaluatingNormalizerTest extends CrateUnitTest {

    private ReferenceResolver<NestableInput<?>> referenceResolver;
    private Functions functions;
    private Reference dummyLoadInfo;

    private final CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(SessionContext.systemSessionContext());

    @Before
    public void prepare() throws Exception {
        Map<ColumnIdent, NestableInput> referenceImplementationMap = new HashMap<>(1, 1);

        ReferenceIdent dummyLoadIdent = new ReferenceIdent(new RelationName("test", "dummy"), "load");
        dummyLoadInfo = new Reference(dummyLoadIdent, RowGranularity.NODE, DataTypes.DOUBLE, null, null);

        referenceImplementationMap.put(dummyLoadIdent.columnIdent(), constant(0.08d));
        functions = getFunctions();
        referenceResolver = new MapBackedRefResolver(referenceImplementationMap);
    }

    /**
     * prepare the following where clause as function symbol tree:
     * <p>
     * where test.dummy.load = 0.08 or name != 'x' and name != 'y'
     * <p>
     * test.dummy.load is a expression that can be evaluated on node level
     * name would be a doc level reference and is untouched
     */
    private Function prepareFunctionTree() {

        Reference load_1 = dummyLoadInfo;
        Literal<Double> d01 = Literal.of(0.08);
        Function load_eq_01 = new Function(
            functionInfo(EqOperator.SIGNATURE, DataTypes.DOUBLE),
            EqOperator.SIGNATURE,
            List.of(load_1, d01)
        );

        Symbol name_ref = new Reference(
            new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, "foo"), "name"),
            RowGranularity.DOC,
            DataTypes.STRING,
            null,
            null
        );
        Symbol x_literal = Literal.of("x");
        Symbol y_literal = Literal.of("y");

        Function name_eq_x = new Function(
            functionInfo(EqOperator.SIGNATURE, DataTypes.STRING),
            EqOperator.SIGNATURE,
            List.of(name_ref, x_literal)
        );

        Function nameNeqX = new Function(
            functionInfo(NotPredicate.SIGNATURE, DataTypes.BOOLEAN),
            NotPredicate.SIGNATURE,
            Collections.singletonList(name_eq_x));

        Function name_eq_y = new Function(
            functionInfo(EqOperator.SIGNATURE, DataTypes.STRING),
            EqOperator.SIGNATURE,
            List.of(name_ref, y_literal));

        Function nameNeqY = new Function(
            functionInfo(NotPredicate.SIGNATURE, DataTypes.BOOLEAN),
            NotPredicate.SIGNATURE,
            Collections.singletonList(name_eq_y));

        Function op_and = new Function(
            functionInfo(AndOperator.SIGNATURE, DataTypes.BOOLEAN),
            AndOperator.SIGNATURE,
            List.of(nameNeqX, nameNeqY)
        );

        return new Function(
            functionInfo(OrOperator.SIGNATURE, DataTypes.BOOLEAN),
            OrOperator.SIGNATURE,
            List.of(load_eq_01, op_and)
        );
    }

    @Test
    public void testEvaluation() {
        EvaluatingNormalizer visitor = new EvaluatingNormalizer(functions, RowGranularity.NODE, referenceResolver, null);

        Function op_or = prepareFunctionTree();

        // the dummy reference load == 0.08 evaluates to true,
        // so the whole query can be normalized to a single boolean literal
        Symbol query = visitor.normalize(op_or, coordinatorTxnCtx);
        assertThat(query, isLiteral(true));
    }

    @Test
    public void testEvaluationClusterGranularity() {
        EvaluatingNormalizer visitor = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver, null);

        Function op_or = prepareFunctionTree();
        Symbol query = visitor.normalize(op_or, coordinatorTxnCtx);
        assertThat(query, instanceOf(Function.class));
    }

    private FunctionInfo functionInfo(Signature signature, DataType dataType) {
        return functions.getQualified(signature, List.of(dataType, dataType)).info();
    }
}
