package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.operation.AggregationContext;
import io.crate.operation.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.JobCollectContext;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;


public class GroupingPipeTest extends CrateUnitTest {

    protected static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

    /**
     * NOTE:
     *
     * the remaining tests for the GroupingProjector are in {@link io.crate.operation.projectors.ProjectionToProjectorVisitorTest}
     **/

    @Test
    public void testAggregationToPartial() throws Exception {

        ImmutableList<Input<?>> keys = ImmutableList.<Input<?>>of(
                new DummyInput(new BytesRef("one"), new BytesRef("one"), new BytesRef("three")));


        FunctionInfo countInfo = new FunctionInfo(new FunctionIdent("count", ImmutableList.<DataType>of()), DataTypes.LONG);
        Aggregation countAggregation =
                Aggregation.partialAggregation(countInfo, DataTypes.LONG, ImmutableList.<Symbol>of());

        Functions functions = new ModulesBuilder()
                .add(new AggregationImplModule()).createInjector().getInstance(Functions.class);

        AggregationContext aggregationContext = new AggregationContext(
                (AggregationFunction)functions.get(countInfo.ident()),
                countAggregation);

        AggregationContext[] aggregations = new AggregationContext[] { aggregationContext };
        GroupingPipe projector = new GroupingPipe(
                Arrays.asList(DataTypes.STRING),
                keys,
                new CollectExpression[0],
                aggregations,
                RAM_ACCOUNTING_CONTEXT
        );

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        projector.downstream(rowReceiver);

        Row emptyRow = new RowN(new Object[]{});

        projector.prepare(mock(JobCollectContext.class));
        projector.setNextRow(emptyRow);
        projector.setNextRow(emptyRow);
        projector.setNextRow(emptyRow);
        projector.finish();
        Bucket rows = rowReceiver.result();
        assertThat(rows.size(), is(2));
        assertThat(rows.iterator().next().get(1), instanceOf(Long.class));
    }

    class DummyInput implements Input<BytesRef> {

        private final BytesRef[] values;
        private int idx;

        DummyInput(BytesRef... values)  {
            this.values = values;
            this.idx = 0;
        }

        @Override
        public BytesRef value() {
            return values[idx++];
        }
    }
}
