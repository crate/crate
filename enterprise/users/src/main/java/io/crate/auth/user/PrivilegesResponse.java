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

package io.crate.auth.user;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class PrivilegesResponse extends AcknowledgedResponse {

    private List<String> unknownUserNames;
    private long affectedRows;

    PrivilegesResponse() {
    }

    PrivilegesResponse(boolean acknowledged, long affectedRows, List<String> unknownUserNames) {
        super(acknowledged);
        this.unknownUserNames = unknownUserNames;
        this.affectedRows = affectedRows;
    }

    List<String> unknownUserNames() {
        return unknownUserNames;
    }

    long affectedRows() {
        return affectedRows;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        affectedRows = in.readLong();
        unknownUserNames = in.readList(StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(affectedRows);
        out.writeStringCollection(unknownUserNames);
    }
}
