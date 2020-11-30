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

import java.util.List;

/**
 * The {@link NodeInfoMXBean | interface is required to define a standard MXBean,
 * such as a standard MXBean is composed of an MXBean interface and a class.
 *
 * This interface lists the methods for all exposed attributes.
 *
 * @see <a href="https://docs.oracle.com/javase/tutorial/jmx/mbeans/mxbeans.html">
 *     https://docs.oracle.com/javase/tutorial/jmx/mbeans/mxbeans.html</a>
 */
public interface NodeInfoMXBean {

    String getNodeId();

    String getNodeName();

    long getClusterStateVersion();

    ShardStats getShardStats();

    List<ShardInfo> getShardInfo();

}
