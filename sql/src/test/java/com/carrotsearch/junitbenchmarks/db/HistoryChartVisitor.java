package com.carrotsearch.junitbenchmarks.db;

import java.lang.reflect.Method;
import java.util.*;

import com.carrotsearch.junitbenchmarks.Result;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;

class HistoryChartVisitor implements IChartAnnotationVisitor
{
    /**
     * Cache of all types and methods for which 
     * {@link com.carrotsearch.junitbenchmarks.db.HistoryChartGenerator} should be invoked.
     */
    private HashMap<Class<?>, List<Method>> types = new HashMap<Class<?>, List<Method>>();

    /*
     * 
     */
    public void visit(Class<?> clazz, Method method, Result result)
    {
        boolean onMethod = method != null && method.isAnnotationPresent(BenchmarkHistoryChart.class);
        boolean onClass = clazz.isAnnotationPresent(BenchmarkHistoryChart.class);

        if (onMethod || onClass)
        {
            if (!types.containsKey(clazz))
            {
                types.put(clazz, new ArrayList<Method>());
            }
         
            if (onMethod)
            {
                types.get(clazz).add(method);
            }
        }
    }

    /*
     * 
     */
    public void generate(DbConsumer c) throws Exception
    {
        for (Map.Entry<Class<?>, List<Method>> e : types.entrySet())
        {
            final Class<?> clazz = e.getKey();
            final List<Method> methods = e.getValue();

            /*
             * Check if the annotation is present on the class. If so, generate a chart
             * for all methods of this class. 
             */
            final BenchmarkHistoryChart ann = clazz.getAnnotation(BenchmarkHistoryChart.class);
            if (ann != null)
            {
                HistoryChartGenerator gen = new HistoryChartGenerator(
                    GeneratorUtils.getFilePrefix(clazz, ann.filePrefix(), c.getChartsDir()), 
                    clazz.getName(), 
                    ann.labelWith(),
                    c);
                gen.updateMaxRuns(ann.maxRuns());
                updateMinMax(clazz.getAnnotation(AxisRange.class), gen);
                gen.generate();
            }

            /*
             * Now check per-method annotations. Partition by file prefix first.
             */
            HashMap<String, List<Method>> byPrefix = new HashMap<String, List<Method>>();
            for (Method m : methods)
            {
                BenchmarkHistoryChart methodAnn = m.getAnnotation(BenchmarkHistoryChart.class);
                String prefix = GeneratorUtils.getFilePrefix(clazz, methodAnn.filePrefix(), c.getChartsDir());
                if (!byPrefix.containsKey(prefix))
                {
                    byPrefix.put(prefix, new ArrayList<Method>());
                }
                byPrefix.get(prefix).add(m);
            }
            
            /*
             * Remove method-level entries which are already covered by class-level annotation.
             */
            if (ann != null)
            {
                byPrefix.remove(GeneratorUtils.getFilePrefix(clazz, ann.filePrefix(), c.getChartsDir()));
            }

            for (Map.Entry<String, List<Method>> e2 : byPrefix.entrySet())
            {
                
                HistoryChartGenerator gen = new HistoryChartGenerator(
                    e2.getKey(), 
                    clazz.getName(),
                    e2.getValue().get(0).getAnnotation(BenchmarkHistoryChart.class).labelWith(),
                    c);

                for (Method m : e2.getValue())
                {
                    gen.updateMaxRuns(m.getAnnotation(BenchmarkHistoryChart.class).maxRuns());
                    updateMinMax(m.getAnnotation(AxisRange.class), gen);
                    gen.includeMethod(m.getName());
                }
                gen.generate();
            }
        }
    }

    private void updateMinMax(AxisRange ann, HistoryChartGenerator gen)
    {
        if (ann != null)
        {
            gen.updateMinMax(ann);
        }
    }
}
