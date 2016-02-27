package org.locationtech.geogig.index.quadtree;

import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.ObjectId;

import com.vividsolutions.jts.geom.Envelope;

class LeafNode extends QuadNode implements Comparable<LeafNode> {

    final Node node;

    public LeafNode(Node node) {
        this.node = node;
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public Envelope getBounds() {
        Envelope bounds = new Envelope();
        node.expand(bounds);
        return bounds;
    }

    public ObjectId getObjectId() {
        return node.getObjectId();
    }

    @Override
    public boolean intersects(Envelope bounds) {
        return node.intersects(bounds);
    }

    @Override
    public boolean intersects(QuadNode qnode) {
        return node.intersects(qnode.getBounds());
    }

    public Node getNode() {
        return node;
    }

    @Override
    public int compareTo(LeafNode o) {
        return getObjectId().compareTo(o.getObjectId());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LeafNode) {
            node.getObjectId().equals(((LeafNode) o).getObjectId());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 17 * node.hashCode();
    }

    @Override
    public String toString() {
        return String.format("NODE: %s -> %s", node.getName(), node.getObjectId());
    }
}