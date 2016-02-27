/* Copyright (c) 2012-2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.test.integration.je;

import java.io.IOException;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.TestPlatform;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.bdbje.EnvironmentBuilder;
import org.locationtech.geogig.storage.bdbje.JEObjectDatabase_v0_2;
import org.locationtech.geogig.storage.fs.IniFileConfigDatabase;

import com.google.common.base.Throwables;

public class JERevTreeBuilderIntegrationTest
        extends org.locationtech.geogig.test.integration.RevTreeBuilderIntegrationTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Override
    protected ObjectStore createObjectStore() {
        Hints hints = new Hints();
        Platform platform;
        try {
            platform = new TestPlatform(tmpFolder.newFolder(".geogig"));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        ConfigDatabase configDB = new IniFileConfigDatabase(platform);
        EnvironmentBuilder envProvider = new EnvironmentBuilder(platform, hints);
        return new JEObjectDatabase_v0_2(configDB, envProvider, hints);
    }

}
