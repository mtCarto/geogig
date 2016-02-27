/* Copyright (c) 2015 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.api.graph;

import java.util.Map;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevTree;

public interface DAGStorageProvider {

    public TreeCache getTreeCache();

    public DAG getOrCreateTree(TreeId treeId);

    public DAG getOrCreateTree(TreeId treeId, ObjectId originalTreeId);

    public Node getNode(NodeId nodeId);

    public void saveNode(NodeId nodeId, Node node);

    public void saveNodes(Map<NodeId, DAGNode> nodeMappings);

    public void dispose();

    @Nullable
    public RevTree getTree(ObjectId originalId);

    public void save(TreeId bucketId, DAG bucketDAG);

    long nodeCount();
}
