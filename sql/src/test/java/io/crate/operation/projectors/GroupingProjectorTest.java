package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.AggregationContext;
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
     * the remaining tests for the GroupingProjector are in {@link io.crate.operation.projectors.ProjectionToProjectorVisitorTest}
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
        projector.upstreamFinished();
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
