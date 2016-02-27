package org.locationtech.geogig.api.graph;

import org.locationtech.geogig.storage.ObjectStore;

class CachingDAGStorageProviderFactory implements DAGStorageProviderFactory {

    private final ObjectStore source;

    CachingDAGStorageProviderFactory(final ObjectStore source) {
        this.source = source;
    }

    @Override
    public DAGStorageProvider canonical() {
        return new CachingDAGStorageProvider(source);
    }

    @Override
    public DAGStorageProvider quadtree() {
        throw new UnsupportedOperationException();
    }

}
