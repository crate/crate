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

package io.crate.operation.user;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DropUserRequest extends MasterNodeRequest<DropUserRequest> {

    private String userName;
    private boolean ifExists;

    public DropUserRequest() {
    }

    public DropUserRequest(String userName, boolean ifExists) {
        this.userName = userName;
        this.ifExists = ifExists;
    }

    public String userName() {
        return userName;
    }

    public boolean ifExists() {
        return ifExists;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (userName == null) {
            return ValidateActions.addValidationError("userName is missing", null);
        }
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        userName = in.readString();
        ifExists = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(userName);
        out.writeBoolean(ifExists);
    }
}
