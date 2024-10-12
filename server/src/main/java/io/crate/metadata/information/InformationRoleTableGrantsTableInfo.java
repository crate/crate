package io.crate.metadata.information;

import io.crate.expression.roles.RoleTableGrant;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.types.DataTypes;

public class InformationRoleTableGrantsTableInfo {
    public static final String NAME = "role_table_grants";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static SystemTable<RoleTableGrant> INSTANCE = SystemTable.<RoleTableGrant>builder(IDENT)
        .add("grantor", DataTypes.STRING, RoleTableGrant::getGrantor)
        .add("grantee", DataTypes.STRING, RoleTableGrant::getGrantee)
        .add("table_catalog", DataTypes.STRING, RoleTableGrant::getTableCatalog)
        .add("table_schema", DataTypes.STRING, RoleTableGrant::getTableSchema)
        .add("table_name", DataTypes.STRING, RoleTableGrant::getTableName)
        .add("privilege_type", DataTypes.STRING, RoleTableGrant::getPrivilegeType)
        .add("is_grantable", DataTypes.BOOLEAN, RoleTableGrant::isGrantable)
        .add("with_hierarchy", DataTypes.BOOLEAN, RoleTableGrant::withHierarchy)
        .build();
}
