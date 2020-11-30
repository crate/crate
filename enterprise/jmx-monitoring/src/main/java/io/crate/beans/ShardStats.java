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

import java.beans.ConstructorProperties;

public class ShardStats {

    private final int total;

    private final int primaries;

    private final int replicas;

    private final int unassigned;

    @ConstructorProperties({"name", "total", "primaries", "replicas", "unassigned"})
    public ShardStats(int total, int primaries, int replicas, int unassigned) {
        this.total = total;
        this.primaries = primaries;
        this.replicas = replicas;
        this.unassigned = unassigned;
    }

    public int getTotal() {
        return total;
    }

    public int getPrimaries() {
        return primaries;
    }

    public int getReplicas() {
        return replicas;
    }

    public int getUnassigned() {
        return unassigned;
    }

}
