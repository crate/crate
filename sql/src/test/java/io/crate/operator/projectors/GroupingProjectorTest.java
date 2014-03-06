package io.crate.operator.projectors;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.operator.Input;
import io.crate.operator.aggregation.AggregationFunction;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.aggregation.impl.CountAggregation;
import io.crate.operator.operations.AggregationContext;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Symbol;
import io.crate.DataType;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GroupingProjectorTest {

    /**
     * NOTE:
     *
     * the remaining tests for the GroupingProjector are in {@link io.crate.operator.projectors.ProjectionToProjectorVisitorTest}
     **/

    @Test
    public void testAggregationToPartial() {

        ImmutableList<Input<?>> keys = ImmutableList.<Input<?>>of(
                new DummyInput("one", "one", "three"));


        FunctionInfo countInfo = new FunctionInfo(new FunctionIdent("count", ImmutableList.<DataType>of()), DataType.LONG);
        Aggregation countAggregation =
                new Aggregation(countInfo, ImmutableList.<Symbol>of(), Aggregation.Step.ITER, Aggregation.Step.PARTIAL);

        Functions functions = new ModulesBuilder()
                .add(new AggregationImplModule()).createInjector().getInstance(Functions.class);

        AggregationContext aggregationContext = new AggregationContext(
                (AggregationFunction)functions.get(countInfo.ident()),
                countAggregation);

        AggregationContext[] aggregations = new AggregationContext[] { aggregationContext };
        GroupingProjector projector = new GroupingProjector(
                keys,
                ImmutableList.<CollectExpression<?>>of(),
                aggregations
        );

        projector.startProjection();
        projector.setNextRow();
        projector.setNextRow();
        projector.setNextRow();
        projector.finishProjection();
        Object[][] rows = projector.getRows();
        assertThat(rows.length, is(2));
        assertThat(rows[0][1], instanceOf(CountAggregation.CountAggState.class));
    }

    class DummyInput implements Input<String> {

        private final String[] values;
        private int idx;

        DummyInput(String... values)  {
            this.values = values;
            this.idx = 0;
        }

        @Override
        public String value() {
            return values[idx++];
        }
    }
}
