package com.carrotsearch.junitbenchmarks.db;

import com.carrotsearch.junitbenchmarks.Result;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;

import java.lang.reflect.Method;
import java.util.HashSet;

/**
 * Collector of {@link com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart} annotations.
 */
class MethodChartVisitor implements IChartAnnotationVisitor
{
    /**
     * Types for which method-level chart should be generated.
     */
    private HashSet<Class<?>> types = new HashSet<Class<?>>();

    /*
     * 
     */
    public void generate(DbConsumer c) throws Exception
    {
        for (Class<?> clazz : types)
        {
            MethodChartGenerator g = new MethodChartGenerator( 
                GeneratorUtils.getFilePrefix(
                    clazz, clazz.getAnnotation(BenchmarkMethodChart.class).filePrefix(), c.getChartsDir()),
                clazz.getName(),
                c);

            AxisRange ann = clazz.getAnnotation(AxisRange.class);
            if (ann != null)
            {
                g.min = ann.min();
                g.max = ann.max();
            }

            g.generate();
        }
    }

    /*
     * 
     */
    public void visit(Class<?> clazz, Method method, Result result)
    {
        if (clazz.isAnnotationPresent(BenchmarkMethodChart.class)
            && !types.contains(clazz))
        {
            types.add(clazz);
        }
    }
}
