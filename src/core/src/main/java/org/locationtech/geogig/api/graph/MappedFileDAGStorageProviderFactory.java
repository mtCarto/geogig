package org.locationtech.geogig.api.graph;

import java.io.IOException;

import org.locationtech.geogig.storage.ObjectStore;

import com.google.common.base.Throwables;

public class MappedFileDAGStorageProviderFactory implements DAGStorageProviderFactory {

    private final ObjectStore treeStore;

    public MappedFileDAGStorageProviderFactory(ObjectStore treeStore) {
        this.treeStore = treeStore;
    }

    @Override
    public DAGStorageProvider canonical() {
        try {
            return new MappedFileDAGStorageProvider(treeStore);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public DAGStorageProvider quadtree() {
        throw new UnsupportedOperationException();
    }

}
