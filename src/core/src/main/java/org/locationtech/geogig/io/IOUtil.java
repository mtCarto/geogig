package org.locationtech.geogig.io;

import java.nio.ByteBuffer;

import org.eclipse.jdt.annotation.Nullable;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

@SuppressWarnings("restriction")
public class IOUtil {

    public static void clean(@Nullable final ByteBuffer buffer) {

        if (buffer instanceof DirectBuffer) {
            Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
            if (cleaner != null) {
                cleaner.clean();
            }
        }

    }
}
