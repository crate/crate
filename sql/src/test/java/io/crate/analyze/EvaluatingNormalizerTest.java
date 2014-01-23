package io.crate.analyze;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.metadata.sys.SysExpression;
import io.crate.operator.operator.*;
import io.crate.operator.reference.sys.NodeLoadExpression;
import io.crate.operator.reference.sys.SysExpressionModule;
import io.crate.operator.reference.sys.SysObjectReference;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.node.service.NodeService;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EvaluatingNormalizerTest {

    private ReferenceResolver referenceResolver;
    private Functions functions;

    @Before
    public void setUp() throws Exception {
        Map<ReferenceIdent, ReferenceImplementation> referenceImplementationMap = new HashMap<>(1, 1);
        referenceImplementationMap.put(
                NodeLoadExpression.INFO_LOAD.ident(), new SysObjectReference<Double>() {

            @Override
            public ReferenceInfo info() {
                return NodeLoadExpression.INFO_LOAD;
            }

            @Override
            public SysExpression<Double> getChildImplementation(String name) {
                return new SysExpression<Double>() {
                            @Override
                            public Double value() {
                                return 0.08;
                            }

                            @Override
                            public ReferenceInfo info() {
                                return NodeLoadExpression.INFO_LOAD_1;
                            }
                };
            }
        });

        functions = new ModulesBuilder().add(new OperatorModule())
                .createInjector()
                .getInstance(Functions.class);

        referenceResolver = new GlobalReferenceResolver(referenceImplementationMap);
    }

    @Test
    public void testEvaluation() {

        EvaluatingNormalizer visitor = new EvaluatingNormalizer(
                functions, RowGranularity.NODE, referenceResolver);

        /**
         * prepare the following where clause as function symbol tree:
         *
         *  where load['1'] = 0.08 or name != 'x' and name != 'y'
          */

        Reference load_1 = new Reference(NodeLoadExpression.INFO_LOAD_1);
        DoubleLiteral d01 = new DoubleLiteral(0.08);
        Function load_eq_01 = new Function(
                new FunctionInfo(
                        new FunctionIdent(EqOperator.NAME, Arrays.asList(DataType.DOUBLE, DataType.DOUBLE)),
                        DataType.BOOLEAN
                ),
                Arrays.<Symbol>asList(load_1, d01)
        );

        ValueSymbol name_ref = new Reference(
                new ReferenceInfo(
                        new ReferenceIdent(new TableIdent(null, "foo"), "name"),
                        RowGranularity.DOC,
                        DataType.STRING
                )
        );
        ValueSymbol x_literal = new StringLiteral("x");
        ValueSymbol y_literal = new StringLiteral("y");

        Function name_neq_x = new Function(
                new FunctionInfo(
                        new FunctionIdent(NotEqOperator.NAME, ImmutableList.of(DataType.STRING, DataType.STRING)),
                        DataType.BOOLEAN
                ),
                Arrays.<Symbol>asList(name_ref, x_literal)
        );
        Function name_neq_y = new Function(
                new FunctionInfo(
                        new FunctionIdent(NotEqOperator.NAME, ImmutableList.of(DataType.STRING, DataType.STRING)),
                        DataType.BOOLEAN
                ),
                Arrays.<Symbol>asList(name_ref, y_literal)
        );

        Function op_and = new Function(
                new FunctionInfo(
                        new FunctionIdent(AndOperator.NAME, Arrays.asList(DataType.BOOLEAN, DataType.BOOLEAN)),
                        DataType.BOOLEAN
                ),
                Arrays.<Symbol>asList(name_neq_x, name_neq_y)
        );

        Function op_or = new Function(
                new FunctionInfo(
                        new FunctionIdent(OrOperator.NAME, Arrays.asList(DataType.BOOLEAN, DataType.BOOLEAN)),
                        DataType.BOOLEAN
                ),
                Arrays.<Symbol>asList(load_eq_01, op_and)
        );


        // the load['1'] == 0.08 parts evaluates to true and therefore the whole query is optimized to true
        Symbol query = visitor.process(op_or, null);
        assertThat(query, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) query).value(), is(true));
    }
}
