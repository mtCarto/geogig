package org.locationtech.geogig.index.quadtree;

import java.io.PrintStream;

import org.eclipse.jdt.annotation.Nullable;

import com.google.common.base.Strings;
import com.vividsolutions.jts.geom.Envelope;

abstract class QuadNode {

    public abstract boolean isLeaf();

    /**
     * @return {@code true} if this node has no direct children nor non empty quadrants.
     */
    public abstract boolean isEmpty();

    public abstract Envelope getBounds();

    public abstract boolean intersects(QuadNode qnode);

    public abstract boolean intersects(Envelope bounds);

    public void print(PrintStream out) {
        print(out, 0, null);
    }

    protected void print(PrintStream out, int level, @Nullable Quadrant q) {
        if (isLeaf()) {
            // out.print(Strings.repeat("  ", level));
            // out.printf("LEAF: %s %s\n", ((LeafNode) this).getObjectId(), getBounds());
        } else {
            out.print(Strings.repeat("  ", level));
            QuadTree tree = (QuadTree) this;
            out.printf(
                    "%d NODE %s: non promotable leaves: %,d leaves: %,d, size: %,d, bounds: %s\n",
                    level, q == null ? "" : q, tree.nonPromotableLeaves.size(),
                    tree.leaves.size(), tree.size(), tree.getBounds());
            if (!isEmpty()) {
                for (LeafNode qn : tree.leaves()) {
                    qn.print(out, level + 1, null);
                }
                if (tree.quadrants.maxDepth > 0) {
                    for (Quadrant childQuadrant : Quadrant.values()) {
                        QuadTree quadrant = tree.getQuadrant(childQuadrant);
                        if (!quadrant.isEmpty()) {
                            quadrant.print(out, level + 1, childQuadrant);
                        }
                    }
                }
            }
        }
    }
}