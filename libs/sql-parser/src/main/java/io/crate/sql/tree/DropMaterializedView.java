/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.crate.sql.tree;

import java.util.Objects;

public final class DropMaterializedView extends Statement {

    private final QualifiedName name;
    private final boolean ifExists;

    public DropMaterializedView(QualifiedName name, boolean ifExists) {
        this.name = name;
        this.ifExists = ifExists;
    }

    public QualifiedName name() {
        return name;
    }

    public boolean ifExists() {
        return ifExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropMaterializedView(this, context);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof DropMaterializedView that
            && ifExists == that.ifExists
            && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, ifExists);
    }

    @Override
    public String toString() {
        return "DropMaterializedView{" +
               "name=" + name +
               ", ifExists=" + ifExists +
               '}';
    }
}
