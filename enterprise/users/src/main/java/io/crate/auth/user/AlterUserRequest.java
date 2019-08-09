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

import io.crate.user.SecureHash;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AlterUserRequest extends AcknowledgedRequest<AlterUserRequest> {

    private final String userName;
    private final SecureHash secureHash;

    public AlterUserRequest(String userName, @Nullable SecureHash secureHash) {
        this.userName = userName;
        this.secureHash = secureHash;
    }

    public String userName() {
        return userName;
    }

    @Nullable
    public SecureHash secureHash() {
        return secureHash;
    }

    public AlterUserRequest(StreamInput in) throws IOException {
        super(in);
        userName = in.readString();
        secureHash = in.readOptionalWriteable(SecureHash::readFrom);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(userName);
        out.writeOptionalWriteable(secureHash);
    }
}
