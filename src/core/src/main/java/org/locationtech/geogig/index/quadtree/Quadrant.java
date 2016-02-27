package org.locationtech.geogig.index.quadtree;

import com.vividsolutions.jts.geom.Envelope;

public enum Quadrant {
    SW(0, 0), NW(0, 1), NE(1, 1), SE(1, 0);

    private final int offsetX;

    private final int offsetY;

    Quadrant(int offsetX, int offsetY) {
        this.offsetX = offsetX;
        this.offsetY = offsetY;

    }

    public Envelope slice(Envelope parent) {

        double w = parent.getWidth() / 2.0;
        double h = parent.getHeight() / 2.0;

        double x1 = parent.getMinX() + offsetX * w;
        double x2 = x1 + w;
        double y1 = parent.getMinY() + offsetY * h;
        double y2 = y1 + h;

        return new Envelope(x1, x2, y1, y2);

    }
}