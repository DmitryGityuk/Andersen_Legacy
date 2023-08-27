package org.molgenis.io;

import org.molgenis.io.strategy.CreateReaderStrategy;
import org.molgenis.io.strategy.TsvFileStrategy;
import org.molgenis.io.strategy.TxtOrCsvFileStrategy;
import org.molgenis.io.strategy.XlsOrXlsxFileStrategy;
import org.molgenis.io.strategy.ZipFileStrategy;

public enum TypeFiles {

    CSV(".csv", new TxtOrCsvFileStrategy()),
    TXT(".txt", new TxtOrCsvFileStrategy()),
    TSV(".tsv", new TsvFileStrategy()),
    XLS(".xls", new XlsOrXlsxFileStrategy()),
    XLSX(".xlsx", new XlsOrXlsxFileStrategy()),
    ZIP(".zip", new ZipFileStrategy());

    private final String format;
    private final CreateReaderStrategy strategy;

    TypeFiles(String format, CreateReaderStrategy strategy) {
        this.format = format;
        this.strategy = strategy;
    }

    public String getFormat() {
        return format;
    }

    public CreateReaderStrategy getStrategy() {
        return strategy;
    }
}
