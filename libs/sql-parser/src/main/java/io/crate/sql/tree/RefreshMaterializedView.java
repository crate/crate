/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.crate.sql.tree;

import java.util.Objects;

public final class RefreshMaterializedView extends Statement {

    private final QualifiedName name;

    public RefreshMaterializedView(QualifiedName name) {
        this.name = name;
    }

    public QualifiedName name() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshMaterializedView(this, context);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RefreshMaterializedView that && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "RefreshMaterializedView{" + name + '}';
    }
}
