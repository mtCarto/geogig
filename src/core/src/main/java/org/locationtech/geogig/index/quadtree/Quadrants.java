package org.locationtech.geogig.index.quadtree;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.vividsolutions.jts.geom.Envelope;

class Quadrants {
    /**
     * Pointers to SW, NW, NE, and SE subquadrants. May be {@link #isEmpty()}, but not null.
     */
    private List<QuadTree> quadrants = ImmutableList.of();

    final Envelope maxBounds;

    final int maxDepth;

    final int sizeSoftLimit;

    public Quadrants(Envelope maxBounds, int maxDepth, int sizeSoftLimit) {
        this.maxBounds = maxBounds;
        this.maxDepth = maxDepth;
        this.sizeSoftLimit = sizeSoftLimit;
    }

    public QuadTree getQuadrant(Quadrant quadrant) {
        return getQuadrants().get(quadrant.ordinal());
    }

    public List<QuadTree> getQuadrants() {
        if (this.maxDepth == 0) {
            throw new IllegalStateException("reached max depth");
        }
        if (quadrants.isEmpty()) {
            final double minX = this.maxBounds.getMinX();
            final double minY = this.maxBounds.getMinY();
            final double maxX = this.maxBounds.getMaxX();
            final double maxY = this.maxBounds.getMaxY();
            final double qwidth = this.maxBounds.getWidth() / 2;
            final double qheight = this.maxBounds.getHeight() / 2;

            QuadTree sw = new QuadTree(new Envelope(minX, minX + qwidth, minY, minY + qheight),
                    maxDepth - 1, sizeSoftLimit);
            QuadTree nw = new QuadTree(new Envelope(minX, minX + qwidth, minY + qheight, maxY),
                    maxDepth - 1, sizeSoftLimit);
            QuadTree ne = new QuadTree(new Envelope(minX + qwidth, maxX, minY + qheight, maxY),
                    maxDepth - 1, sizeSoftLimit);
            QuadTree se = new QuadTree(new Envelope(minX + qwidth, maxX, minY, minY + qheight),
                    maxDepth - 1, sizeSoftLimit);

            List<QuadTree> qs = ImmutableList.of(sw, nw, ne, se);
            synchronized (quadrants) {
                if (quadrants.isEmpty()) {
                    quadrants = qs;
                }
            }
        }
        return quadrants;
    }

    public boolean isEmpty() {
        if (quadrants.isEmpty()) {
            return true;
        }
        for (QuadNode q : quadrants) {
            if (!q.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public long size() {
        if (quadrants.isEmpty()) {
            return 0L;
        }
        long size = 0L;
        for (QuadTree q : quadrants) {
            size += q.size();
        }
        return size;
    }
}