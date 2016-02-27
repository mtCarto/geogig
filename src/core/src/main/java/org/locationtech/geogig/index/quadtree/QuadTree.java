package org.locationtech.geogig.index.quadtree;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.locationtech.geogig.api.ObjectId;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

class QuadTree extends QuadNode {

    /**
     * Pointers to SW, NW, SE, and NE subquadrants.
     */
    final Quadrants quadrants;

    final List<LeafNode> leaves = new ArrayList<>();

    final List<LeafNode> nonPromotableLeaves = new LinkedList<>();

    public QuadTree(Envelope maxBounds, int maxDepth, final int sizeSoftLimit) {
        this.quadrants = new Quadrants(maxBounds, maxDepth, sizeSoftLimit);
    }

    public int getDepth() {
        int depth = 0;
        if (!quadrants.isEmpty()) {
            int maxSubDepth = 0;
            for (Quadrant q : Quadrant.values()) {
                QuadTree quadrant = quadrants.getQuadrant(q);
                maxSubDepth = Math.max(maxSubDepth, quadrant.getDepth());
            }
            depth = 1 + maxSubDepth;
        }
        return depth;
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return leaves.isEmpty() && quadrants.isEmpty();
    }

    @Override
    public Envelope getBounds() {
        return quadrants.maxBounds;
    }

    /**
     * A tree can contain both feature nodes and subtrees (quadrants), in which case it's a "mixed"
     * tree.
     */
    public synchronized void put(LeafNode node) {
        Preconditions.checkArgument(this.intersects(node));
        if (canPromote(node)) {
            leaves.add(node);
        } else {
            nonPromotableLeaves.add(node);
        }

        final int maxDepth = this.quadrants.maxDepth;
        if (maxDepth == 0) {
            // reached max depth, can't promote to quadrants, just add the node the the
            // leaves
            return;
        }

        if (leaves.size() == quadrants.sizeSoftLimit) {
            pack(false);
        }
    }

    void pack(boolean recursive) {
        if (this.quadrants.maxDepth == 0) {
            return;
        }
        if (this.quadrants.isEmpty() && leaves.size() <= quadrants.sizeSoftLimit) {
            return;
        }
        for (LeafNode leaf : leaves) {
            QuadTree container = null;
            for (Quadrant q : Quadrant.values()) {
                QuadTree quadrant = quadrants.getQuadrant(q);
                if (quadrant.contains(leaf)) {
                    // found the single quadrant that fully contains the node, add it and
                    // return
                    quadrant.put(leaf);
                    container = quadrant;
                    break;
                }
            }
            if (container == null) {
                throw new IllegalStateException(leaf.node.toString());
            }
        }
        leaves.clear();
        if (recursive) {
            for (Quadrant q : Quadrant.values()) {
                QuadTree quadrant = quadrants.getQuadrant(q);
                quadrant.pack(true);
            }
        }
    }

    private boolean canPromote(LeafNode node) {
        final Envelope maxBounds = getBounds();
        Coordinate centre = maxBounds.centre();
        Envelope bounds = node.getBounds();
        if (!maxBounds.contains(bounds)) {
            return false;
        }
        if (bounds.getMinX() < centre.x && bounds.getMaxX() > centre.x) {
            return false;
        }
        if (bounds.getMinY() < centre.y && bounds.getMaxY() > centre.y) {
            return false;
        }
        return true;
    }

    Envelope buf = new Envelope();

    private boolean contains(LeafNode currNode) {
        buf.setToNull();
        currNode.getNode().expand(buf);
        boolean contains = getBounds().contains(buf);
        return contains;
    }

    private LeafNode findAndRemoveGreatestObjectId(List<LeafNode> keepList) {
        Preconditions.checkState(!keepList.isEmpty());

        ObjectId biggestId = keepList.get(0).getObjectId();
        int biggestOidIdx = 0;
        for (int i = 1; i < keepList.size(); i++) {
            ObjectId id = keepList.get(i).getObjectId();
            biggestId = ObjectId.NATURAL_ORDER.max(biggestId, id);
            if (biggestId == id) {
                biggestOidIdx = i;
            }
        }
        LeafNode removed = keepList.remove(biggestOidIdx);
        return removed;
    }

    List<LeafNode> leaves() {
        return leaves;
    }

    public QuadTree getQuadrant(Quadrant q) {
        return quadrants.getQuadrant(q);
    }

    public long size() {
        long size = nonPromotableLeaves.size() + leaves.size() + quadrants.size();
        return size;
    }

    @Override
    public boolean intersects(QuadNode qnode) {
        return qnode.intersects(getBounds());
    }

    @Override
    public boolean intersects(Envelope bounds) {
        return getBounds().intersects(bounds);
    }
}