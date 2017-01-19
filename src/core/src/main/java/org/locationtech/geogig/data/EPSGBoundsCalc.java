/*
 *  Copyright (c) 2017 Boundless and others.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Distribution License v1.0
 *  which accompanies this distribution, and is available at
 *  https://www.eclipse.org/org/documents/edl-v10.html
 *
 *  Contributors:
 *  Morgan Thompson (Boundless) - initial implementation
 *  Alex Goudine (Boundless)
 */

package org.locationtech.geogig.data;

import com.google.common.base.Optional;
import com.vividsolutions.jts.geom.Envelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.util.logging.Logging;
import org.locationtech.geogig.model.RevFeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.PropertyDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.ReferenceIdentifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.metadata.extent.Extent;
import org.opengis.metadata.extent.GeographicBoundingBox;
import org.opengis.metadata.extent.GeographicExtent;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.operation.CoordinateOperation;
import org.opengis.referencing.operation.CoordinateOperationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a code string (EPSG:####) or {@link RevFeatureType} , find the CRS bounds and return as an
 * Envelope
 */
public class EPSGBoundsCalc {

    private static final Logger LOGGER = LoggerFactory.getLogger(Logging.class);

    private static CoordinateReferenceSystem wgs84 = DefaultGeographicCRS.WGS84;
    private static CoordinateReferenceSystem wgs84_3d = DefaultGeographicCRS.WGS84_3D;

    /**
     * Get the bounds of the desired CRS, uses JTS ReferencedEnvelope transform to properly
     * handle polar projections
     * @param crs the target CoordinateReferenceSystem
     * @return bounds an Envelope containing the CRS bounds, throws a NoSuchAuthorityCodeException
     * if the crs cannot be found
     */
    public Envelope getExtents(CoordinateReferenceSystem crs) throws Exception {

        final Extent domainOfValidity = crs.getDomainOfValidity();

        if (null == domainOfValidity) {
            throw new Exception("No domain of validity provided by CRS definition");
        }

        Collection<? extends GeographicExtent> geographicElements;
        geographicElements = domainOfValidity.getGeographicElements();

        GeographicExtent geographicExtent = geographicElements.iterator().next();
        GeographicBoundingBox geographicBoundingBox = (GeographicBoundingBox) geographicExtent;

        double minx = geographicBoundingBox.getWestBoundLongitude();
        double miny = geographicBoundingBox.getSouthBoundLatitude();
        double maxx = geographicBoundingBox.getEastBoundLongitude();
        double maxy = geographicBoundingBox.getNorthBoundLatitude();

        CoordinateOperationFactory coordOpFactory = CRS.getCoordinateOperationFactory(true);
        CoordinateOperation op = coordOpFactory.createOperation(wgs84,crs);

        ReferencedEnvelope refEnvelope = new ReferencedEnvelope(minx, maxx, miny, maxy, wgs84);
        GeneralEnvelope genEnvelope = CRS.transform(op, refEnvelope);

        double xmax = genEnvelope.getMaximum(0);
        double ymax = genEnvelope.getMaximum(1);
        double xmin = genEnvelope.getMinimum(0);
        double ymin = genEnvelope.getMinimum(1);
        Envelope envelope = new Envelope(xmin, xmax, ymin, ymax);
        return envelope;
    }

    /**
     * Search for the given CRS (EPSG code), return the bounds (domain of validity)
     * @param refId the input CRS
     * @return projectionBounds an Envelope describing the CRS bounds, throws
     * NoSuchAuthorityException if the
     * CRS is not found
     */
    public Envelope findCode(String refId) throws Exception {
        CRSAuthorityFactory authorityFactory = CRS.getAuthorityFactory(true);

        CoordinateReferenceSystem crs = authorityFactory.createCoordinateReferenceSystem(refId);

        Envelope projectionBounds = getExtents(crs);
        return projectionBounds;
    }

    /**
     * Search for the given CRS (EPSG code), return the bounds (domain of validity)
     * @param featureType the RevFeatureType to find the CRS bounds of
     * @return bounds an Envelope describing the CRS bounds, throws NoSuchAuthorityException if the
     * CRS is not found
     */
    public Envelope getCRSBounds(Optional<RevFeatureType> featureType) throws Exception {
        ReferenceIdentifier code = null;

        CoordinateReferenceSystem crs = featureType.get().type().getGeometryDescriptor().getCoordinateReferenceSystem();
        Envelope bounds = getExtents(crs);

        return bounds;
    }
}
