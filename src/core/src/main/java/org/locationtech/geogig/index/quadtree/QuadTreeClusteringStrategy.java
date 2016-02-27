package org.locationtech.geogig.index.quadtree;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.api.graph.ClusteringStrategy;
import org.locationtech.geogig.api.graph.DAGStorageProvider;

import com.vividsolutions.jts.geom.Envelope;

public class QuadTreeClusteringStrategy extends ClusteringStrategy {

    private final Envelope maxBounds;

    private final int maxDepth;

    public QuadTreeClusteringStrategy(RevTree original, DAGStorageProvider storageProvider,
            Envelope maxBounds, int maxDepth) {
        super(original, storageProvider);
        this.maxBounds = maxBounds;
        this.maxDepth = maxDepth;
    }

    // @Override
    // protected int maxDepth() {
    // return 12;
    // }
    //
    @Override
    protected int maxBuckets(final int depthIndex) {
        return 4;
    }

    @Override
    public int normalizedSizeLimit(final int depthIndex) {
        return 128;
    }

    @Override
    public @Nullable QuadTreeNodeId computeId(final Node node) {
        QuadTreeNodeId nodeId = null;
        if (node.bounds().isPresent()) {
            nodeId = computeIdInternal(node);
        }
        return nodeId;
    }

    private QuadTreeNodeId computeIdInternal(Node node) {

        final int maxDepth = this.maxDepth;

        Envelope nodeBounds = node.bounds().get();
        List<Quadrant> quadrantsByDepth = new ArrayList<>(maxDepth);

        final Quadrant[] quadrants = Quadrant.values();

        Envelope parentQuadrantBounds = this.maxBounds;

        for (int depth = 0; depth < maxDepth; depth++) {
            for (int q = 0; q < 4; q++) {
                Quadrant quadrant = quadrants[q];
                Envelope qBounds = quadrant.slice(parentQuadrantBounds);
                if (qBounds.contains(nodeBounds)) {
                    quadrantsByDepth.add(quadrant);
                    parentQuadrantBounds = qBounds;
                    break;
                }
            }
        }

        Quadrant[] nodeQuadrants = quadrantsByDepth.toArray(new Quadrant[quadrantsByDepth.size()]);
        return new QuadTreeNodeId(node.getName(), nodeQuadrants);
    }

}