/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.user;

import io.crate.analyze.user.Privilege;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class PrivilegesRequest extends MasterNodeRequest<PrivilegesRequest> {

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
