package org.molgenis.io.strategy;

import org.molgenis.io.TableReader;
import org.molgenis.io.ZipTableReader;

import java.io.File;
import java.io.IOException;
import java.util.zip.ZipFile;

public class ZipFileStrategy implements CreateReaderStrategy {

    @Override
    public TableReader createTableReader(File file, String name) throws IOException {
        return new ZipTableReader(new ZipFile(file));
    }
}
