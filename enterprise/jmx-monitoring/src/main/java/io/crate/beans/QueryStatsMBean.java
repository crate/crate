/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.beans;

/**
 * The QueryStatsMBean interface is required to define a standard MBean,
 * such as a standard MBean is composed of an MBean interface and a class.
 * <p>
 * The QueryStatsMBean interface lists the methods for all exposed attributes.
 *
 * @see <a href="https://docs.oracle.com/javase/tutorial/jmx/mbeans/standard.html">
 * https://docs.oracle.com/javase/tutorial/jmx/mbeans/standard.html</a>
 */
public interface QueryStatsMBean {

    long getSelectQueryTotalCount();

    long getInsertQueryTotalCount();

    long getUpdateQueryTotalCount();

    long getDeleteQueryTotalCount();

    long getManagementQueryTotalCount();

    long getDDLQueryTotalCount();

    long getCopyQueryTotalCount();

    long getUndefinedQueryTotalCount();

    long getSelectQuerySumOfDurations();

    long getInsertQuerySumOfDurations();

    long getUpdateQuerySumOfDurations();

    long getDeleteQuerySumOfDurations();

    long getManagementQuerySumOfDurations();

    long getDDLQuerySumOfDurations();

    long getCopyQuerySumOfDurations();

    long getUndefinedQuerySumOfDurations();

    long getSelectQueryFailedCount();

    long getInsertQueryFailedCount();

    long getUpdateQueryFailedCount();

    long getDeleteQueryFailedCount();

    long getManagementQueryFailedCount();

    long getDDLQueryFailedCount();

    long getCopyQueryFailedCount();

    long getUndefinedQueryFailedCount();
}
