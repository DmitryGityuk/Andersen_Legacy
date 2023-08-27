package org.molgenis.io.strategy;

import org.molgenis.io.TableReader;

import java.io.File;
import java.io.IOException;

public interface CreateReaderStrategy {

    TableReader createTableReader(File file, String name) throws IOException;
}
