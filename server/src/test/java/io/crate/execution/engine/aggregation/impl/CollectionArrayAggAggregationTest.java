package io.crate.execution.engine.aggregation.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.breaker.MemoryCircuitBreaker;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class CollectionArrayAggAggregationTest {

    private RamAccounting ramAccounting = RamAccounting.NO_ACCOUNTING;
    private MemoryManager memoryManager = new MemoryManager(
        new MemoryCircuitBreaker(Settings.EMPTY, "test"),
        MemorySizeValue.memorySizeValue("0b")
    );

    private CollectionArrayAggAggregation createAgg() {
        return new CollectionArrayAggAggregation(
            Signature.builder("collection_array_agg", FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING)
                .returnType(DataTypes.STRING_ARRAY)
                .build(),
            new BoundSignature("collection_array_agg", List.of(DataTypes.STRING), DataTypes.STRING_ARRAY)
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_array_agg_distinct_values() throws CircuitBreakingException {
        CollectionArrayAggAggregation agg = createAgg();

        Set<Object> set = new HashSet<>();
        set.add("a");
        set.add("b");
        set.add("a"); // duplicate

        Input<Object> input = () -> set;

        Object state = agg.newState(ramAccounting, Version.CURRENT, memoryManager);
        state = agg.iterate(ramAccounting, memoryManager, state, input);
        List<Object> result = (List<Object>) agg.terminatePartial(ramAccounting, state);

        assertEquals(2, result.size());
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_array_agg_empty_set() throws CircuitBreakingException {
        CollectionArrayAggAggregation agg = createAgg();

        Set<Object> set = new HashSet<>();
        Input<Object> input = () -> set;

        Object state = agg.newState(ramAccounting, Version.CURRENT, memoryManager);
        state = agg.iterate(ramAccounting, memoryManager, state, input);
        List<Object> result = (List<Object>) agg.terminatePartial(ramAccounting, state);

        assertTrue(result.isEmpty());
    }
}
