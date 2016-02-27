package org.locationtech.geogig.api;

public interface TreeBuilder {

    public TreeBuilder put(Node node);

    public TreeBuilder remove(String featureId);

    public RevTree build();
}