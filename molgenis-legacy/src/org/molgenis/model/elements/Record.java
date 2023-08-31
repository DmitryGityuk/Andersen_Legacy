package org.molgenis.model.elements;


import java.util.List;

import org.molgenis.model.MolgenisModelException;

/**
 * This interface describes the functionality for a Record. A record is defined
 * as a single or a number of tables, which can yield data. This means that ...
 *
 * @author RA Scheltema
 */
public interface Record {

    /**
     *
     */
    String getName();

    /**
     *
     */
    String getLabel();

    /**
     * @throws MolgenisModelException
     */
    List<Field> getFields() throws MolgenisModelException;

    /**
     *
     */
    List<String> getParents();

    // small utility methods

    /**
     *
     */
    boolean hasXRefs();
}
