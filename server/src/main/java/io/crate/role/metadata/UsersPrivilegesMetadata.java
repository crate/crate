/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.role.metadata;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.jetbrains.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.metadata.RelationName;
import io.crate.role.Privilege;
import io.crate.role.PrivilegeIdent;
import io.crate.role.PrivilegeState;

public class UsersPrivilegesMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "users_privileges";

    /**
     * Returns a copy of {@link UsersPrivilegesMetadata} including a copied list of privileges.
     */
    public static UsersPrivilegesMetadata copyOf(@Nullable UsersPrivilegesMetadata oldMetadata) {
        if (oldMetadata == null) {
            return new UsersPrivilegesMetadata();
        }

        Map<String, Set<Privilege>> userPrivileges = new HashMap<>(oldMetadata.usersPrivileges.size());
        for (Map.Entry<String, Set<Privilege>> entry : oldMetadata.usersPrivileges.entrySet()) {
            userPrivileges.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        return new UsersPrivilegesMetadata(userPrivileges);
    }

    /**
     * Returns a copy of the {@link UsersPrivilegesMetadata} including a copied list of privileges if at least one
     * privilege was replaced. Otherwise returns the NULL to indicate that nothing was changed.
     * Privileges of class {@link Privilege.Clazz#TABLE} whose idents are matching the given source ident are replaced
     * by a copy where the ident is changed to the given target ident.
     */
    @Nullable
    public static UsersPrivilegesMetadata maybeCopyAndReplaceTableIdents(UsersPrivilegesMetadata oldMetadata,
                                                                         String sourceIdent,
                                                                         String targetIdent) {
        boolean privilegesChanged = false;
        Map<String, Set<Privilege>> userPrivileges = new HashMap<>(oldMetadata.usersPrivileges.size());
        for (Map.Entry<String, Set<Privilege>> entry : oldMetadata.usersPrivileges.entrySet()) {
            Set<Privilege> privileges = new HashSet<>(entry.getValue().size());
            for (Privilege privilege : entry.getValue()) {
                PrivilegeIdent privilegeIdent = privilege.ident();
                if (privilegeIdent.clazz().equals(Privilege.Clazz.TABLE) == false) {
                    privileges.add(privilege);
                    continue;
                }

                String ident = privilegeIdent.ident();
                assert ident != null : "ident must not be null for privilege class 'TABLE'";
                if (ident.equals(sourceIdent)) {
                    privileges.add(new Privilege(privilege.state(), privilegeIdent.type(), privilegeIdent.clazz(),
                        targetIdent, privilege.grantor()));
                    privilegesChanged = true;
                } else {
                    privileges.add(privilege);
                }
            }
            userPrivileges.put(entry.getKey(), privileges);
        }

        if (privilegesChanged) {
            return new UsersPrivilegesMetadata(userPrivileges);
        }
        return null;
    }

    private final Map<String, Set<Privilege>> usersPrivileges;

    @VisibleForTesting
    public UsersPrivilegesMetadata() {
        usersPrivileges = new HashMap<>();
    }

    @VisibleForTesting
    public UsersPrivilegesMetadata(Map<String, Set<Privilege>> usersPrivileges) {
        this.usersPrivileges = usersPrivileges;
    }

    public static UsersPrivilegesMetadata swapPrivileges(UsersPrivilegesMetadata usersPrivileges,
                                                         RelationName source,
                                                         RelationName target) {
        HashMap<String, Set<Privilege>> privilegesByUser = new HashMap<>();
        for (Map.Entry<String, Set<Privilege>> userPrivileges : usersPrivileges.usersPrivileges.entrySet()) {
            String user = userPrivileges.getKey();
            Set<Privilege> privileges = userPrivileges.getValue();
            Set<Privilege> updatedPrivileges = new HashSet<>();
            for (Privilege privilege : privileges) {
                PrivilegeIdent ident = privilege.ident();
                if (ident.clazz() == Privilege.Clazz.TABLE) {
                    if (source.fqn().equals(ident.ident())) {
                        updatedPrivileges.add(
                            new Privilege(privilege.state(), ident.type(), ident.clazz(), target.fqn(), privilege.grantor()));
                    } else if (target.fqn().equals(ident.ident())) {
                        updatedPrivileges.add(
                            new Privilege(privilege.state(), ident.type(), ident.clazz(), source.fqn(), privilege.grantor()));
                    } else {
                        updatedPrivileges.add(privilege);
                    }
                } else {
                    updatedPrivileges.add(privilege);
                }
            }
            privilegesByUser.put(user, updatedPrivileges);
        }
        return new UsersPrivilegesMetadata(privilegesByUser);
    }

    /**
     * Applies the provided privileges to the specified users.
     *
     * NOTE: the provided privileges collection will be mutated
     *
     * @return the number of affected privileges (doesn't count no-ops eg. granting a privilege a user already has)
     */
    public long applyPrivileges(Collection<String> userNames, Iterable<Privilege> newPrivileges) {
        long affectedPrivileges = 0L;
        for (String userName : userNames) {
            affectedPrivileges += applyPrivilegesToUser(userName, newPrivileges);
        }
        return affectedPrivileges;
    }

    private long applyPrivilegesToUser(String userName, Iterable<Privilege> newPrivileges) {
        Set<Privilege> userPrivileges = usersPrivileges.get(userName);
        // privileges set is expected, it must be created on user creation
        assert userPrivileges != null : "privileges must not be null for user=" + userName;

        long affectedCount = 0L;
        for (Privilege newPrivilege : newPrivileges) {
            Iterator<Privilege> iterator = userPrivileges.iterator();
            boolean userHadPrivilegeOnSameObject = false;
            while (iterator.hasNext()) {
                Privilege userPrivilege = iterator.next();
                PrivilegeIdent privilegeIdent = userPrivilege.ident();
                if (privilegeIdent.equals(newPrivilege.ident())) {
                    userHadPrivilegeOnSameObject = true;
                    if (newPrivilege.state().equals(PrivilegeState.REVOKE)) {
                        iterator.remove();
                        affectedCount++;
                        break;
                    } else {
                        // we only want to process a new GRANT/DENY privilege if the user doesn't already have it
                        if (userPrivilege.equals(newPrivilege) == false) {
                            iterator.remove();
                            userPrivileges.add(newPrivilege);
                            affectedCount++;
                        }
                        break;
                    }
                }
            }

            if (userHadPrivilegeOnSameObject == false && newPrivilege.state().equals(PrivilegeState.REVOKE) == false) {
                // revoking a privilege that was not granted is a no-op
                affectedCount++;
                userPrivileges.add(newPrivilege);
            }
        }

        return affectedCount;
    }

    public long dropTableOrViewPrivileges(String tableOrViewIdent) {
        long affectedPrivileges = 0L;
        for (Set<Privilege> privileges : usersPrivileges.values()) {
            Iterator<Privilege> privilegeIterator = privileges.iterator();
            while (privilegeIterator.hasNext()) {
                Privilege privilege = privilegeIterator.next();
                PrivilegeIdent privilegeIdent = privilege.ident();
                Privilege.Clazz clazz = privilegeIdent.clazz();
                if (clazz.equals(Privilege.Clazz.TABLE) == false && clazz.equals(Privilege.Clazz.VIEW) == false) {
                    continue;
                }

                String ident = privilegeIdent.ident();
                assert ident != null : "ident must not be null for privilege class 'TABLE'";
                if (ident.equals(tableOrViewIdent)) {
                    privilegeIterator.remove();
                    affectedPrivileges++;
                }
            }
        }
        return affectedPrivileges;
    }

    @Nullable
    public Set<Privilege> getUserPrivileges(String userName) {
        return usersPrivileges.get(userName);
    }

    public void createPrivileges(String userName, Set<Privilege> privileges) {
        usersPrivileges.put(userName, privileges);
    }

    public void dropPrivileges(String userName) {
        usersPrivileges.remove(userName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UsersPrivilegesMetadata that = (UsersPrivilegesMetadata) o;
        return Objects.equals(usersPrivileges, that.usersPrivileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(usersPrivileges);
    }

    public UsersPrivilegesMetadata(StreamInput in) throws IOException {
        int numUsersPrivileges = in.readVInt();
        usersPrivileges = new HashMap<>(numUsersPrivileges);
        for (int i = 0; i < numUsersPrivileges; i++) {
            String userName = in.readString();
            int numPrivileges = in.readVInt();
            Set<Privilege> privileges = new HashSet<>(numPrivileges);
            for (int j = 0; j < numPrivileges; j++) {
                privileges.add(new Privilege(in));
            }
            usersPrivileges.put(userName, privileges);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(usersPrivileges.size());
        for (Map.Entry<String, Set<Privilege>> entry : usersPrivileges.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue().size());
            for (Privilege privilege : entry.getValue()) {
                privilege.writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, Set<Privilege>> entry : usersPrivileges.entrySet()) {
            builder.startArray(entry.getKey());
            for (Privilege privilege : entry.getValue()) {
                privilegeToXContent(privilege, builder);
            }
            builder.endArray();
        }

        return builder;
    }

    public static UsersPrivilegesMetadata fromXContent(XContentParser parser) throws IOException {
        UsersPrivilegesMetadata metadata = new UsersPrivilegesMetadata();
        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            String userName = parser.currentName();
            Set<Privilege> privileges = metadata.getUserPrivileges(userName);
            if (privileges == null) {
                privileges = new HashSet<>();
                metadata.createPrivileges(userName, privileges);
            }
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    privilegeFromXContent(parser, privileges);
                }
            }
        }
        return metadata;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
    }

    private static void privilegeToXContent(Privilege privilege, XContentBuilder builder) throws IOException {
        builder.startObject()
            .field("state", privilege.state().ordinal())
            .field("type", privilege.ident().type().ordinal())
            .field("class", privilege.ident().clazz().ordinal())
            .field("ident", privilege.ident().ident())
            .field("grantor", privilege.grantor())
            .endObject();
    }

    private static void privilegeFromXContent(XContentParser parser, Set<Privilege> privileges) throws IOException {
        XContentParser.Token currentToken;
        PrivilegeState state = null;
        Privilege.Type type = null;
        Privilege.Clazz clazz = null;
        String ident = null;
        String grantor = null;
        while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (currentToken == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                currentToken = parser.nextToken();
                switch (currentFieldName) {
                    case "state":
                        if (currentToken != XContentParser.Token.VALUE_NUMBER) {
                            throw new ElasticsearchParseException(
                                "failed to parse privilege, 'state' value is not a number [{}]", currentToken);
                        }
                        state = PrivilegeState.values()[parser.intValue()];
                        break;
                    case "type":
                        if (currentToken != XContentParser.Token.VALUE_NUMBER) {
                            throw new ElasticsearchParseException(
                                "failed to parse privilege, 'type' value is not a number [{}]", currentToken);
                        }
                        type = Privilege.Type.values()[parser.intValue()];
                        break;
                    case "class":
                        if (currentToken != XContentParser.Token.VALUE_NUMBER) {
                            throw new ElasticsearchParseException(
                                "failed to parse privilege, 'class' value is not a number [{}]", currentToken);
                        }
                        clazz = Privilege.Clazz.values()[parser.intValue()];
                        break;
                    case "ident":
                        if (currentToken != XContentParser.Token.VALUE_STRING
                            && currentToken != XContentParser.Token.VALUE_NULL) {
                            throw new ElasticsearchParseException(
                                "failed to parse privilege, 'ident' value is not a string or null [{}]", currentToken);
                        }
                        ident = parser.textOrNull();
                        break;
                    case "grantor":
                        if (currentToken != XContentParser.Token.VALUE_STRING) {
                            throw new ElasticsearchParseException(
                                "failed to parse privilege, 'grantor' value is not a string [{}]", currentToken);
                        }
                        grantor = parser.text();
                        break;
                    default:
                        throw new ElasticsearchParseException("failed to parse privilege");
                }
            } else if (currentToken == XContentParser.Token.END_ARRAY) {
                // empty privileges set
                return;
            }
        }
        privileges.add(new Privilege(state, type, clazz, ident, grantor));
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_3_0_1;
    }
}
