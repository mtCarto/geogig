package org.locationtech.geogig.api.graph;

import org.locationtech.geogig.storage.ObjectStore;

public class CanonicalClusteringStrategyMappedFileStorageTest extends
        CanonicalClusteringStrategyTest {

    @Override
    protected DAGStorageProviderFactory createStorageProvider(ObjectStore source) {
        return new MappedFileDAGStorageProviderFactory(source);
    }

}
