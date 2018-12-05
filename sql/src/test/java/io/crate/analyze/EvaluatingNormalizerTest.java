package io.crate.analyze;

import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SessionContext;
import io.crate.expression.NestableInput;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ClusterReferenceResolver;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.CoreMatchers.instanceOf;

public class EvaluatingNormalizerTest extends CrateUnitTest {

    private ClusterReferenceResolver referenceResolver;
    private Functions functions;
    private Reference dummyLoadInfo;

    private final CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(SessionContext.systemSessionContext());

    @Before
    public void prepare() throws Exception {
        Map<ReferenceIdent, NestableInput> referenceImplementationMap = new HashMap<>(1, 1);

        ReferenceIdent dummyLoadIdent = new ReferenceIdent(new RelationName("test", "dummy"), "load");
        dummyLoadInfo = new Reference(dummyLoadIdent, RowGranularity.NODE, DataTypes.DOUBLE);

        referenceImplementationMap.put(dummyLoadIdent, constant(0.08d));
        functions = getFunctions();
        referenceResolver = new ClusterReferenceResolver(referenceImplementationMap);
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
            functionInfo(EqOperator.NAME, DataTypes.DOUBLE), Arrays.<Symbol>asList(load_1, d01));

        Symbol name_ref = new Reference(
            new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, "foo"), "name"),
            RowGranularity.DOC,
            DataTypes.STRING);
        Symbol x_literal = Literal.of("x");
        Symbol y_literal = Literal.of("y");

        Function name_eq_x = new Function(
            functionInfo(EqOperator.NAME, DataTypes.STRING), Arrays.<Symbol>asList(name_ref, x_literal));

        Function name_neq_x = new Function(
            functionInfo(NotPredicate.NAME, DataTypes.BOOLEAN, true), Arrays.<Symbol>asList(name_eq_x));

        Function name_eq_y = new Function(
            functionInfo(EqOperator.NAME, DataTypes.STRING), Arrays.<Symbol>asList(name_ref, y_literal));

        Function name_neq_y = new Function(
            functionInfo(NotPredicate.NAME, DataTypes.BOOLEAN, true), Arrays.<Symbol>asList(name_eq_y));

        Function op_and = new Function(
            functionInfo(AndOperator.NAME, DataTypes.BOOLEAN), Arrays.<Symbol>asList(name_neq_x, name_neq_y));

        return new Function(
            functionInfo(OrOperator.NAME, DataTypes.BOOLEAN), Arrays.<Symbol>asList(load_eq_01, op_and));
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

    private FunctionInfo functionInfo(String name, DataType dataType, boolean isPredicate) {
        ImmutableList<DataType> dataTypes;
        if (isPredicate) {
            dataTypes = ImmutableList.of(dataType);
        } else {
            dataTypes = ImmutableList.of(dataType, dataType);
        }
        return functions.getQualified(new FunctionIdent(name, dataTypes)).info();
    }

    private FunctionInfo functionInfo(String name, DataType dataType) {
        return functionInfo(name, dataType, false);
    }
}
