package io.crate.sql.tree;

import java.util.List;

public class RevokePrivilege extends PrivilegeStatement {

    public RevokePrivilege(List<String> userNames) {
        super(userNames);
    }

    public RevokePrivilege(List<String> userNames, List<String> privilegeTypes) {
        super(userNames, privilegeTypes);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRevokePrivilege(this, context);
    }

    @Override
    public String toString() {
        return "RevokePrivilege{" +
               "allPrivileges=" + all +
               "privilegeTypes=" + privilegeTypes +
               ", userNames=" + userNames +
               '}';
    }
}
