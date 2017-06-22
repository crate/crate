package io.crate.sql.tree;

import java.util.List;

public class GrantPrivilege extends PrivilegeStatement {

    public GrantPrivilege(List<String> userNames) {
        super(userNames);
    }

    public GrantPrivilege(List<String> userNames, List<String> privilegeTypes) {
        super(userNames, privilegeTypes);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantPrivilege(this, context);
    }

    @Override
    public String toString() {
        return "GrantPrivilege{" +
               "allPrivileges=" + all +
               "privilegeTypes=" + privilegeTypes +
               ", userNames=" + userNames +
               '}';
    }
}
