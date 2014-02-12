package io.crate.metadata.doc;

import io.crate.metadata.TableIdent;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.TableUnknownException;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.indices.IndexMissingException;

import java.io.IOException;

public class DocTableInfoBuilder {

    private final TableIdent ident;
    private final MetaData metaData;
    private final boolean checkAliasSchema;
    private final ClusterService clusterService;
    private String[] concreteIndices;

    public DocTableInfoBuilder(TableIdent ident, ClusterService clusterService, boolean checkAliasSchema) {
        this.clusterService = clusterService;
        this.metaData = clusterService.state().metaData();
        this.ident = ident;
        this.checkAliasSchema = checkAliasSchema;
    }

    public DocIndexMetaData docIndexMetaData() {
        DocIndexMetaData docIndexMetaData;
        try {
            concreteIndices = metaData.concreteIndices(new String[]{ident.name()}, IgnoreIndices.NONE, true);
        } catch (IndexMissingException ex) {
            throw new TableUnknownException(ident.name(), ex);
        }
        docIndexMetaData = buildDocIndexMetaData(concreteIndices[0]);
        if (concreteIndices.length == 1 || !checkAliasSchema) {
            return docIndexMetaData;
        }
        for (int i = 1; i < concreteIndices.length; i++) {
            docIndexMetaData = docIndexMetaData.merge(buildDocIndexMetaData(concreteIndices[i]));
        }
        return docIndexMetaData;
    }

    private DocIndexMetaData buildDocIndexMetaData(String index) {
        DocIndexMetaData docIndexMetaData;
        try {
            docIndexMetaData = new DocIndexMetaData(metaData.index(index), ident);
        } catch (IOException e) {
            throw new CrateException("Unable to build DocIndexMetaData", e);
        }
        return docIndexMetaData.build();
    }

    public DocTableInfo build() {
        DocIndexMetaData md = docIndexMetaData();
        return new DocTableInfo(ident, md.columns(), md.references(), md.primaryKey(), md.routingCol(),
                concreteIndices, clusterService);
    }

}
