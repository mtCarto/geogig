/* Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage;

import org.locationtech.geogig.repository.RepositoryConnectionException;

import com.google.inject.Provider;

public class ForwardingObjectDatabase extends ForwardingObjectStore implements ObjectDatabase {

    public ForwardingObjectDatabase(Provider<? extends ObjectDatabase> odb) {
        super(odb);
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        ((ObjectDatabase) subject.get()).configure();
    }

    @Override
    public void checkConfig() throws RepositoryConnectionException {
        ((ObjectDatabase) subject.get()).checkConfig();
    }

    @Override
    public boolean isReadOnly() {
        return ((ObjectDatabase) subject.get()).isReadOnly();
    }

    public ConflictsDatabase getConflictsDatabase() {
        return ((ObjectDatabase) subject.get()).getConflictsDatabase();
    }

    @Override
    public ObjectInserter newObjectInserter() {
        return ((ObjectDatabase) subject.get()).newObjectInserter();
    }

    @Override
    public BlobStore getBlobStore() {
        return ((ObjectDatabase) subject.get()).getBlobStore();
    }
}
