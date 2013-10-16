package com.carrotsearch.junitbenchmarks.db;

import java.lang.reflect.Method;

import com.carrotsearch.junitbenchmarks.Result;

/**
 * 
 */
interface IChartAnnotationVisitor
{
    void visit(Class<?> clazz, Method method, Result result);
    void generate(DbConsumer consumer) throws Exception;
}
