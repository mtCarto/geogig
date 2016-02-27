package org.locationtech.geogig.api.graph;

import org.locationtech.geogig.storage.ObjectStore;

public class CanonicalClusteringStrategyHeapStorageTest extends CanonicalClusteringStrategyTest {

    @Override
    protected DAGStorageProviderFactory createStorageProvider(ObjectStore source) {
        return new HeapDAGStorageProviderFactory(source);
    }

}
