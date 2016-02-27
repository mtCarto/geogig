package org.locationtech.geogig.api.graph;

import org.locationtech.geogig.storage.ObjectStore;

import com.google.common.base.Preconditions;

public class HeapDAGStorageProviderFactory implements DAGStorageProviderFactory {

    private final ObjectStore treeSource;

    public HeapDAGStorageProviderFactory(final ObjectStore treeSource) {
        Preconditions.checkNotNull(treeSource, "Argument treeSource is null");
        this.treeSource = treeSource;
    }

    @Override
    public DAGStorageProvider canonical() {
        return new HeapDAGStorageProvider(treeSource);
    }

    @Override
    public DAGStorageProvider quadtree() {
        return new HeapDAGStorageProvider(treeSource);
    }

}
