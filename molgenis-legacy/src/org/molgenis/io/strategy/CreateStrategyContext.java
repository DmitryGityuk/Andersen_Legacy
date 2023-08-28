package org.molgenis.io.strategy;

import org.molgenis.io.TableReader;
import org.molgenis.io.TypeFiles;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.io.Files.getFileExtension;

public class CreateStrategyContext {
    private final Map<TypeFiles, CreateReaderStrategy> creationStrategies;

    public CreateStrategyContext() {
        this.creationStrategies = initStrategies();
    }

    public TableReader executeCreationStrategy(File file) throws IOException {
        String name = file.getName();
        TypeFiles fileType = checkFileFormat(name);
        CreateReaderStrategy strategy = getCreationStrategy(fileType);
        return strategy.createTableReader(file, name);
    }

    private static TypeFiles checkFileFormat(String name) throws IOException {
        String inputFileExtension = getFileExtension(name);
        return Arrays.stream(TypeFiles.values())
                .filter(fileType -> fileType.getFormat().equals(inputFileExtension))
                .findFirst()
                .orElseThrow(() -> new IOException("unknown file type: " + inputFileExtension));
    }

    private Map<TypeFiles, CreateReaderStrategy> initStrategies() {
        return Arrays.stream(TypeFiles.values())
                .collect(Collectors.toMap(fileType -> fileType, TypeFiles::getStrategy));
    }

    private CreateReaderStrategy getCreationStrategy(TypeFiles fileType) throws IOException {
        CreateReaderStrategy strategy = creationStrategies.get(fileType);
        if (strategy == null) {
            throw new IOException("unknown file type: " + fileType.getFormat());
        }
        return strategy;
    }
}
