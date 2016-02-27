package org.locationtech.geogig.api.graph;

public interface DAGStorageProviderFactory {

    public DAGStorageProvider canonical();

    public DAGStorageProvider quadtree();
}
