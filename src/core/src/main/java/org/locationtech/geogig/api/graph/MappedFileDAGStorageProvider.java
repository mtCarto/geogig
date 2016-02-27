package org.locationtech.geogig.api.graph;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.storage.ObjectStore;

import com.google.common.base.Preconditions;

class MappedFileDAGStorageProvider implements DAGStorageProvider {

    private final ConcurrentMap<TreeId, DAG> trees;

    private final OffHeapNodeStore nodes;

    private TreeCache treeCache;

    private AtomicLong nodeCount = new AtomicLong();

    MappedFileDAGStorageProvider(final ObjectStore source) throws IOException {
        this(source, new TreeCache(source));
    }

    MappedFileDAGStorageProvider(final ObjectStore source, final TreeCache treeCache)
            throws IOException {
        this.trees = new ConcurrentHashMap<>();
        this.treeCache = treeCache;
        this.nodes = new OffHeapNodeStore();
    }

    @Override
    public TreeCache getTreeCache() {
        return treeCache;
    }

    @Override
    public RevTree getTree(ObjectId treeId) {
        return treeCache.getTree(treeId);
    }

    private DAG createTree(TreeId treeId, ObjectId originalTreeId) {
        DAG dag = new DAG(originalTreeId);
        DAG existing = trees.putIfAbsent(treeId, dag);
        Preconditions.checkState(existing == null, "DAG %s[%s] already exists: %s", treeId,
                originalTreeId, existing);
        return dag;
    }

    @Override
    public DAG getOrCreateTree(TreeId treeId) {
        return getOrCreateTree(treeId, RevTree.EMPTY_TREE_ID);
    }

    @Override
    public DAG getOrCreateTree(TreeId treeId, ObjectId originalTreeId) {
        DAG dag = trees.get(treeId);
        if (dag == null) {
            dag = createTree(treeId, originalTreeId);
        }
        return dag;
    }

    @Override
    public void save(TreeId bucketId, DAG bucketDAG) {
        trees.put(bucketId, bucketDAG);
    }

    @Override
    public Node getNode(NodeId nodeId) {
        DAGNode dagNode = nodes.get(nodeId);
        Node node = dagNode.resolve(treeCache);
        return node;
    }

    @Override
    public void saveNode(NodeId nodeId, Node node) {
        nodes.put(nodeId, DAGNode.of(node));
        nodeCount.incrementAndGet();
    }

    @Override
    public void saveNodes(Map<NodeId, DAGNode> nodeMappings) {
        for (Entry<NodeId, DAGNode> e : nodeMappings.entrySet()) {
            nodes.put(e.getKey(), e.getValue());
        }
        nodeCount.addAndGet(nodeMappings.size());
    }

    @Override
    public long nodeCount() {
        return nodeCount.get();
    }

    @Override
    public void dispose() {
        trees.clear();
        nodes.dispose();
    }
}
