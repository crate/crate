package io.crate.operation.user;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.WhereClause;
import io.crate.analyze.user.Privilege;
import io.crate.exceptions.PermissionDeniedException;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.T3;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserPriviligeValidationTest extends CrateDummyClusterServiceUnitTest {

    private static final User crateUser = new User("crateUser",
        ImmutableSet.of(User.Role.SUPERUSER),
        Collections.EMPTY_SET);

    private static final User dmlUser = new User("dmlUser",
        Collections.EMPTY_SET,
        ImmutableSet.of(new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "dmlUser")));

    private static final User ddlUser = new User("ddlUser",
        Collections.EMPTY_SET,
        ImmutableSet.of(new Privilege(Privilege.State.GRANT, Privilege.Type.DDL, Privilege.Clazz.CLUSTER, null, "crate")));

    private static final User dqlUser = new User("dqlUser",
        Collections.EMPTY_SET,
        ImmutableSet.of(new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate")));

    private static final User allUser = new User("allUser",
        Collections.EMPTY_SET,
        ImmutableSet.of(new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate"),
            new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate"),
            new Privilege(Privilege.State.GRANT, Privilege.Type.DDL, Privilege.Clazz.CLUSTER, null, "crate")));

    private static final User noPriviligeUser = new User("noPriviligeUser",
        Collections.EMPTY_SET,
        Collections.EMPTY_SET);

    private UserManagerService userManagerService;
    private Schemas schemasWithSysTables;
    private Schemas schemas;
    private TableIdent authorized = new TableIdent("sys", "authorized");

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void beforeTest() throws Exception {
        userManagerService = new UserManagerService(null, null, null, clusterService);
        SysSchemaInfo sysSchemaInfo = new SysSchemaInfo(clusterService);

        // register table which requires permission
        sysSchemaInfo.registerSysTable(new StaticTableInfo(authorized,
            ImmutableMap.of(), null, ImmutableList.of()) {

            @Override
            public RowGranularity rowGranularity() {
                return RowGranularity.DOC;
            }

            @Override
            public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
                return null;
            }

            @Override
            public Set<User.Role> requiredUserRoles() {
                return ImmutableSet.of();
            }
        });
        schemasWithSysTables = new Schemas(Settings.EMPTY, ImmutableMap.of(
            "sys", sysSchemaInfo
        ), null, null, ()->userManagerService);

        SchemaInfo schemaInfo = mock(SchemaInfo.class);
        when(schemaInfo.getTableInfo(T3.T1_INFO.ident().name())).thenReturn(T3.T1_INFO);
        when(schemaInfo.name()).thenReturn(T3.T1_INFO.ident().schema());

        schemas = getReferenceInfos(schemaInfo);
    }

    @Test
    public void testReadFromTableDoesntThrowAnExceptionForUserWithDQL() throws Exception {
        // if the test succeeds this method doesn't return an error
        schemas.getTableInfo(T3.T1_INFO.ident(), Operation.READ, dqlUser);
    }

    @Test
    public void testDropTableDoesntThrowAnExceptionForUserWithDDL() throws Exception {
        // if the test succeeds this method doesn't return an error
        schemas.getTableInfo(T3.T1_INFO.ident(), Operation.DROP, ddlUser);
    }

    @Test
    public void testCOPYTOFromTableDoesntThrowAnExceptionForUserWithDQL() throws Exception {
        // if the test succeeds this method doesn't return an error
        schemas.getTableInfo(T3.T1_INFO.ident(), Operation.COPY_TO, dqlUser);
    }

    @Test
    public void testReadFromTableThrowsAnExceptionForUserWithDMLOnly() throws Exception {
        expectedException.expect(PermissionDeniedException.class);
        expectedException.expectMessage(
            "Missing 'DQL' Privilege for user 'dmlUser'");
        schemas.getTableInfo(T3.T1_INFO.ident(), Operation.READ, dmlUser);
    }

    @Test
    public void testReadFromTableThrowsAnExceptionForUserWithNoPrivilege() throws Exception {
        expectedException.expect(PermissionDeniedException.class);
        expectedException.expectMessage(
            "Missing 'DQL' Privilege for user 'noPriviligeUser'");
        schemas.getTableInfo(T3.T1_INFO.ident(), Operation.READ, noPriviligeUser);
    }

    @Test
    public void testReadFromTableDoesntThrowsAnExceptionForSuperUser() throws Exception {
        // if the test succeeds this method doesn't return an error
        schemas.getTableInfo(T3.T1_INFO.ident(), Operation.READ, crateUser);
    }

    @Test
    public void testInsertDoesntThrowsAnExceptionForSuperUser() throws Exception {
        // if the test succeeds this method doesn't return an error
        schemas.getTableInfo(T3.T1_INFO.ident(), Operation.INSERT, crateUser);
    }

    @Test
    public void testDropDoesntThrowsAnExceptionForSuperUser() throws Exception {
        // if the test succeeds this method doesn't return an error
        schemas.getTableInfo(T3.T1_INFO.ident(), Operation.DROP, crateUser);
    }

    @Test
    public void testInsertThrowsAnExceptionForSuperUserOnReadOnlyTable() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
            "The relation \"sys.authorized\" doesn't support or allow INSERT operations, as it is read-only.");
        schemasWithSysTables.getTableInfo(authorized, Operation.INSERT, crateUser);
    }

    @Test
    public void testDropSysTableThrowsExceptionForUserWithAllPermissions() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
            "The relation \"sys.authorized\" doesn't support or allow DROP operations, as it is read-only.");
        schemasWithSysTables.getTableInfo(authorized, Operation.DROP, allUser);
    }

    private Schemas getReferenceInfos(SchemaInfo schemaInfo) {
        Map<String, SchemaInfo> builtInSchema = new HashMap<>();
        builtInSchema.put(schemaInfo.name(), schemaInfo);
        return new Schemas(Settings.EMPTY, builtInSchema, clusterService, mock(DocSchemaInfoFactory.class), () -> userManagerService);
    }
}

