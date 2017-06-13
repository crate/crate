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

import io.crate.analyze.user.Privilege;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class PrivilegesRequest extends AcknowledgedRequest<PrivilegesRequest> {

    private Collection<String> userNames;
    private Collection<Privilege> privileges;

    public PrivilegesRequest() {
    }

    public PrivilegesRequest(Collection<String> userNames, Collection<Privilege> privileges) {
        this.userNames = userNames;
        this.privileges = privileges;
    }

    public Collection<String> userNames() {
        return userNames;
    }

    public Collection<Privilege> privileges() {
        return privileges;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (userNames == null || userNames.isEmpty()) {
            validationException = ValidateActions.addValidationError("userNames are missing", null);
        }
        if (privileges == null || privileges.isEmpty()) {
            validationException = ValidateActions.addValidationError("privileges are missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int userNamesSize = in.readVInt();
        userNames = new ArrayList<>(userNamesSize);
        for (int i = 0; i < userNamesSize; i++) {
            userNames.add(in.readString());
        }
        int privilegesSize = in.readVInt();
        privileges = new ArrayList<>(privilegesSize);
        for (int i = 0; i < privilegesSize; i++) {
            Privilege privilege = new Privilege();
            privilege.readFrom(in);
            privileges.add(privilege);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(userNames.size());
        for (String userName : userNames) {
            out.writeString(userName);
        }
        out.writeVInt(privileges.size());
        for (Privilege privilege : privileges) {
            privilege.writeTo(out);
        }
    }
}
