package org.locationtech.geogig.cli.porcelain;

import java.io.IOException;

import org.locationtech.geogig.api.GeoGIG;
import org.locationtech.geogig.cli.AbstractCommand;
import org.locationtech.geogig.cli.CLICommand;
import org.locationtech.geogig.cli.CommandFailedException;
import org.locationtech.geogig.cli.Console;
import org.locationtech.geogig.cli.GeogigCLI;
import org.locationtech.geogig.cli.InvalidParameterException;
import org.locationtech.geogig.cli.annotation.ReadOnly;
import org.locationtech.geogig.di.StorageProvider;
import org.locationtech.geogig.di.VersionedFormat;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.RefDatabase;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(commandNames = { "storage" }, commandDescription = "Show current repository storage type or list available backends")
@ReadOnly
public class Storage extends AbstractCommand implements CLICommand {

    @Parameter(names = { "-l", "--list" }, description = "List available storage backends")
    boolean list;

    @Override
    protected void runInternal(GeogigCLI cli) throws InvalidParameterException,
            CommandFailedException, IOException {

        Console console = cli.getConsole();
        try {
            if (list) {
                console.println("Available storage providers:");
                for (StorageProvider sp : StorageProvider.findProviders()) {
                    printProvider(console, sp);
                }
            } else {
                GeoGIG geogig = cli.getGeogig();
                if (geogig == null) {
                    throw new CommandFailedException("Not in a geogig repository");
                }
                console.println("Repository storage:");
                try {
                    Repository repository = geogig.getRepository();
                    ObjectDatabase objects = repository.objectDatabase();
                    GraphDatabase graph = repository.graphDatabase();
                    RefDatabase refs = repository.refDatabase();
                    printFormats(console, objects.getVersion(), graph.getVersion(),
                            refs.getVersion());
                } finally {
                    geogig.close();
                }
            }
        } finally {
            console.flush();
        }
    }

    private void printProvider(Console console, StorageProvider sp) throws IOException {

        String name = sp.getName();
        String description = sp.getDescription();
        String version = sp.getVersion();
        VersionedFormat objects = sp.getObjectDatabaseFormat();
        VersionedFormat graph = sp.getGraphDatabaseFormat();
        VersionedFormat refs = sp.getRefsDatabaseFormat();
        console.println(String.format("Name: %s, version: %s", name, version));
        console.println(String.format("Description: %s", description));
        printFormats(console, objects, graph, refs);
    }

    private void printFormats(Console console, VersionedFormat objects, VersionedFormat graph,
            VersionedFormat refs) throws IOException {
        console.println(String.format("\tObjects: %s %s", objects.getFormat(), objects.getVersion()));
        console.println(String.format("\tGraph  : %s %s", graph.getFormat(), graph.getVersion()));
        console.println(String.format("\tRefs   : %s %s", refs.getFormat(), refs.getVersion()));
    }

}
