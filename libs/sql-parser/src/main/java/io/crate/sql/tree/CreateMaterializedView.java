/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.crate.sql.tree;

import java.util.Objects;

public final class CreateMaterializedView extends Statement {

    private final QualifiedName name;
    private final Query query;
    private final boolean ifNotExists;

    public CreateMaterializedView(QualifiedName name, Query query, boolean ifNotExists) {
        this.name = name;
        this.query = query;
        this.ifNotExists = ifNotExists;
    }

    public QualifiedName name() {
        return name;
    }

    public Query query() {
        return query;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaterializedView(this, context);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CreateMaterializedView that
            && ifNotExists == that.ifNotExists
            && name.equals(that.name)
            && query.equals(that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query, ifNotExists);
    }

    @Override
    public String toString() {
        return "CreateMaterializedView{" +
               "name=" + name +
               ", query=" + query +
               ", ifNotExists=" + ifNotExists +
               '}';
    }
}
