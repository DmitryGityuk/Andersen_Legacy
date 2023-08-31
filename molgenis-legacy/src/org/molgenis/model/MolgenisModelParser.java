/**
 * File: invengine_generate/parser/XmlParser.java <br>
 * Copyright: Inventory 2000-2006, GBIC 2005, all rights reserved <br>
 * Changelog:
 * <ul>
 * <li>2005-12-08; 1.0.0; RA Scheltema Creation.
 * </ul>
 */
package org.molgenis.model;

import org.apache.log4j.Logger;
import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.molgenis.MolgenisFieldTypes;
import org.molgenis.fieldtypes.UnknownField;
import org.molgenis.model.elements.DBSchema;
import org.molgenis.model.elements.Entity;
import org.molgenis.model.elements.Field;
import org.molgenis.model.elements.Form;
import org.molgenis.model.elements.Form.SortOrder;
import org.molgenis.model.elements.Index;
import org.molgenis.model.elements.Matrix;
import org.molgenis.model.elements.Menu;
import org.molgenis.model.elements.Method;
import org.molgenis.model.elements.MethodQuery;
import org.molgenis.model.elements.Model;
import org.molgenis.model.elements.Module;
import org.molgenis.model.elements.Parameter;
import org.molgenis.model.elements.Plugin;
import org.molgenis.model.elements.Plugin.Flavor;
import org.molgenis.model.elements.Record;
import org.molgenis.model.elements.Tree;
import org.molgenis.model.elements.UISchema;
import org.molgenis.model.elements.View;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * TODO: refactor: spread over multiple files.
 */
public class MolgenisModelParser {

    /**
     * @param model
     * @param element a DOM element that looks like <entity name="a"
     *                abstract="true"><field name="a" type="....
     * @throws MolgenisModelException
     */
    public static Entity parseEntity(Model model, Element element) throws MolgenisModelException {
        String[] keywords = new String[]
                {"name", "label", "extends", "implements", "abstract", "description", "system", "decorator", "xref_label",
                        "allocationSize"};
        List<String> key_words = new ArrayList<String>(Arrays.asList(keywords));
        for (int i = 0; i < element.getAttributes().getLength(); i++) {
            if (!key_words.contains(element.getAttributes().item(i).getNodeName())) {
                throw new MolgenisModelException("attribute '" + element.getAttributes().item(i).getNodeName()
                        + "' not allowed for <entity>");
            }
        }

        if (element.getAttribute("name").trim().isEmpty()) {
            String message = "name is missing for entity " + element;
            logger.error(message);
            throw new MolgenisModelException(message);
        }

        Entity entity = new Entity(element.getAttribute("name").trim(), element.getAttribute("label"),
                model.getDatabase());
        entity.setNamespace(model.getName());

        String anExtends = element.getAttribute("extends");
        if (anExtends != null) {
            Vector<String> parents = new Vector<>();
            StringTokenizer tokenizer = new StringTokenizer(anExtends, ",");
            while (tokenizer.hasMoreTokens()) {
                parents.add(tokenizer.nextToken().trim());
            }

            entity.setParents(parents);
        }

        String anImplements = element.getAttribute("implements");
        if (anImplements != null && !anImplements.isEmpty()) {
            entity.setImplements(new Vector<String>(Arrays.asList(anImplements.split(","))));
        }

        entity.setAbstract(Boolean.parseBoolean(element.getAttribute("abstract")));

        entity.setSystem(Boolean.parseBoolean(element.getAttribute("system")));

        String xref_label = element.getAttribute("xref_label");
        if (xref_label != null && !xref_label.isEmpty()) {
            List<String> xref_labels = new ArrayList<>(Arrays.asList(xref_label.split(",")));
            entity.setXrefLabels(xref_labels);
        } else {
            entity.setXrefLabels(null);
        }

        if (element.hasAttribute("decorator")) {
            entity.setDecorator(element.getAttribute("decorator"));
        }

        NodeList elements = element.getElementsByTagName("description");
        for (int j = 0; j < elements.getLength(); j++) {
            entity.setDescription(elementValueToString((Element) elements.item(j)));
        }

        elements = element.getElementsByTagName("field");
        for (int j = 0; j < elements.getLength(); j++) {
            Element elem = (Element) elements.item(j);
            parseField(entity, elem);
        }

        elements = element.getElementsByTagName("unique");
        for (int j = 0; j < elements.getLength(); j++) {
            Element elem = (Element) elements.item(j);
            List<String> keys = new ArrayList<>();

            if (elem.hasAttribute("fields")) {
                for (String name : elem.getAttribute("fields").split(",")) {
                    Field f = entity.getField(name);
                    if (f == null) {
                        f = entity.getAllField(name);
                        if (f == null) throw new
                                MolgenisModelException("Missing unique field '" + name
                                + "' in entity '" + entity.getName() + "'");

                        f = new Field(f);
                        f.setEntity(entity);
                        f.setSystem(true);
                        entity.addField(f);
                    }
                    keys.add(name);
                }
            }

            NodeList key_elements = elem.getElementsByTagName("keyfield");
            for (int k = 0; k < key_elements.getLength(); k++) {
                elem = (Element) key_elements.item(k);

                String name = elem.getAttribute("name");

                keys.add(name);
            }

            String key_description = null;
            if (elem.hasAttribute("description")) {
                key_description = elem.getAttribute("description");
            }

            if (keys.size() == 0) {
                throw new MolgenisModelException("missing fields on unique of '" + entity.getName()
                        + "'. Expected <unique fields=\"field1[,field2,..]\" description=\"...\"/>");
            }

            try {
                entity.addKey(keys, elem.getAttribute("subclass").equals("true"), key_description);
            } catch (Exception e) {
                throw new MolgenisModelException(e.getMessage());
            }
        }

        elements = element.getElementsByTagName("indices");
        if (elements.getLength() == 1) {
            Element elem = (Element) elements.item(0);

            NodeList index_elements = elem.getElementsByTagName("index");
            for (int k = 0; k < index_elements.getLength(); k++) {
                elem = (Element) index_elements.item(k);

                Index index = new Index(elem.getAttribute("name"));

                NodeList indexfield_elements = elem.getElementsByTagName("indexfield");
                for (int l = 0; l < indexfield_elements.getLength(); l++) {
                    elem = (Element) indexfield_elements.item(l);

                    Field f = entity.getField(elem.getAttribute("name"));
                    if (f == null) {
                        throw new MolgenisModelException("Missing index field: " + elem.getAttribute("name"));
                    }

                    try {
                        index.addField(elem.getAttribute("name"));
                    } catch (Exception e) {
                        throw new MolgenisModelException(e.getMessage());
                    }
                }

                try {
                    entity.addIndex(index);
                } catch (Exception e) {
                }
            }
        } else if (elements.getLength() > 1) {
            throw new MolgenisModelException("Multiple indices elements");
        }
        if (element.hasAttribute("allocationSize")) {
            int allocationSize = Integer.parseInt(element.getAttribute("allocationSize"));
            entity.setAllocationSize(allocationSize);
        }

        logger.debug("read: " + entity.getName());
        return entity;
    }

    public static void parseMatrix(Model model, Element element) throws MolgenisModelException {
        String[] keywords = {"name", "content_entity", "content", "container", "row", "col", "row_entity", "col_entity"};
        List<String> keyWords = Arrays.asList(keywords);

        for (int i = 0; i < element.getAttributes().getLength(); i++) {
            if (!keyWords.contains(element.getAttributes().item(i).getNodeName())) {
                throw new MolgenisModelException("attribute '" + element.getAttributes().item(i).getNodeName()
                        + "' unknown for <entity>");
            }
        }

        if (element.getAttribute("name").isEmpty()) {
            String message = "name is missing for entity " + element;
            logger.error(message);
            throw new MolgenisModelException(message);
        }

        Matrix matrix = new Matrix(element.getAttribute("name"), model.getDatabase());
        matrix.setCol(element.getAttribute("col"));
        matrix.setRow(element.getAttribute("row"));
        matrix.setContentEntity(element.getAttribute("content_entity"));
        matrix.setContainer(element.getAttribute("container"));
        matrix.setColEntityName(element.getAttribute("col_entity"));
        matrix.setRowEntityName(element.getAttribute("row_entity"));
        matrix.setContent(element.getAttribute("content"));

        logger.debug("read: " + matrix);
    }

    public static void parseField(Entity entity, Element element) throws MolgenisModelException {

        String[] keywords = new String[]
                {"type", "name", "label", "auto", "nillable", "optional", "readonly", "default", "description", "desc",
                        "unique", "hidden", "length", "index", "enum_options", "default_code", "xref", "xref_entity",
                        "xref_field", "xref_label", "xref_name", "mref_name", "mref_localid", "mref_remoteid", "filter",
                        "filtertype", "filterField", "filtervalue", "xref_cascade" + "", "allocationSize", "jpaCascade"};
        List<String> key_words = new ArrayList<>(Arrays.asList(keywords));
        for (int i = 0; i < element.getAttributes().getLength(); i++) {
            if (!key_words.contains(element.getAttributes().item(i).getNodeName())) {
                throw new MolgenisModelException("attribute '" + element.getAttributes().item(i).getNodeName()
                        + "' not allowed for <field>");
            }
        }

        String name = Optional.of(element.getAttribute("name"))
                .orElseThrow(() -> new MolgenisModelException("name is missing for a field in entity '" + entity.getName() + "'"));

        String type = Optional.of(element.getAttribute("type"))
                .orElseThrow(() -> new MolgenisModelException("type is missing for field '" + name + "' of entity '" + entity.getName() + "'"));

        String label = Optional.of(element.getAttribute("label"))
                .orElse(name);

        String auto = Optional.of(element.getAttribute("auto"))
                .orElse("");
        String nillable = Optional.of(element.getAttribute("nillable")).orElse(element.getAttribute("optional"));
        String readonly = element.getAttribute("readonly");
        String default_value = element.getAttribute("default");

        String description = Optional.of(element.getAttribute("description")).orElse(element.getAttribute("desc"));
        String unique = element.getAttribute("unique");
        String hidden = element.getAttribute("hidden");
        String length = element.getAttribute("length");
        String index = element.getAttribute("index");
        String enum_options = element.getAttribute("enum_options").replace('[', ' ').replace(']', ' ').trim();
        String default_code = element.getAttribute("default_code");

        String xref_entity = element.getAttribute("xref_entity");
        final String[] xref_field = {Optional.of(element.getAttribute("xref_field")).orElse(element.getAttribute("xref"))};
        String xref_label = element.getAttribute("xref_label");
        String mref_name = element.getAttribute("mref_name");
        String mref_localid = element.getAttribute("mref_localid");
        String mref_remoteid = element.getAttribute("mref_remoteid");

        xref_entity = Optional.ofNullable(xref_field[0])
                .filter(f -> !f.isEmpty() && !element.hasAttribute("xref_entity"))
                .map(f -> f.split("\\."))
                .filter(arr -> arr.length == 2)
                .map(arr -> {
                    xref_field[0] = arr[1];
                    return arr[0];
                })
                .orElse(xref_entity);


        String filter = Optional.of(element.getAttribute("filter"))
                .orElse("");

        String filterType = Optional.of(element.getAttribute("filtertype"))
                .orElse("");

        String filterField = Optional.of(element.getAttribute("filterfield"))
                .orElse("");

        String filterValue = Optional.of(element.getAttribute("filtervalue"))
                .orElse("");

        Map<String, String> typeMappings = new HashMap<>();
        typeMappings.put("varchar", "string");
        typeMappings.put("number", "int");
        typeMappings.put("boolean", "bool");
        typeMappings.put("xref_single", "xref");
        typeMappings.put("xref_multiple", "mref");
        typeMappings.put("autoid", "int");

        String finalType1 = type;
        type = Optional.ofNullable(typeMappings.get(type))
                .orElseThrow(() -> new MolgenisModelException("type '" + finalType1 + "' unknown for field '" + name + "' of entity '" + entity.getName() + "'"));

        String finalType = type;
        if (finalType.equals("autoid")) {
            finalType = "int";
            nillable = "false";
            auto = "true";
            readonly = "true";
            unique = "true";
            default_value = "";
        }

        if (finalType.isEmpty()) {
            throw new MolgenisModelException("type is missing for field '" + name + "' of entity '" + entity.getName() + "'");
        }

        if (MolgenisFieldTypes.getType(type) instanceof UnknownField) {
            throw new MolgenisModelException("type '" + type + "' unknown for field '" + name + "' of entity '" + entity.getName() + "'");
        }

        if (hidden.equals("true") && !nillable.equals("true") && (default_value.isEmpty() && !auto.equals("true"))) {
            throw new MolgenisModelException("field '" + name + "' of entity '" + entity.getName() + "' must have a default value. A field that is not nillable and hidden must have a default value.");
        }

        String jpaCascade = Optional.of(type)
                .filter(t -> t.equals("mref") || t.equals("xref"))
                .flatMap(t -> Optional.of(element.getAttribute("jpaCascade")))
                .orElse(null);

        Field field = new Field(entity, MolgenisFieldTypes.getType(type), name, label,
                Boolean.parseBoolean(auto), Boolean.parseBoolean(nillable),
                Boolean.parseBoolean(readonly), default_value, jpaCascade);
        logger.debug("read: " + field);

        Optional.of(description)
                .filter(desc -> !desc.isEmpty())
                .ifPresent(desc -> field.setDescription(desc.trim()));

        Optional.of(hidden.equals("true"))
                .ifPresent(field::setHidden);

        Optional.of(default_code)
                .filter(code -> !code.isEmpty())
                .ifPresent(field::setDefaultCode);

        Optional.of(filter.equals("true"))
                .ifPresent(mainFilters -> {
                    logger.warn("filter set for field '" + name + "' of entity '" + entity.getName() + "'");
                    logger.warn(filterField + " " + filterType + " " + filterValue);
                    logger.warn(System.currentTimeMillis() + " - filter bool: '" + Boolean.parseBoolean(String.valueOf(mainFilters)) + "'");

                    if ((filterType != null && filterType.isEmpty()) || (filterField != null && filterField.isEmpty())) {
                        try {
                            throw new MolgenisModelException("field '" + name + "' of entity '" + entity.getName()
                                    + "': when the filter is set to true, the filtertype, filterField and filtervalue must be set");
                        } catch (MolgenisModelException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    Optional.ofNullable(filterValue)
                            .filter(value -> !value.isEmpty())
                            .orElseGet(() -> {
                                logger.warn("no value specified for filter in field '" + name + "' of entity '" +
                                        entity.getName() + "'");
                                return null;
                            });

                    field.setFilter(Boolean.parseBoolean(String.valueOf(mainFilters)));
                    field.setFiltertype(filterType);
                    field.setFilterfield(filterField);
                    field.setFiltervalue(filterValue);
                });

        if (type.equals("string")) {
            if (!length.isEmpty()) {
                field.setVarCharLength(Integer.parseInt(length));
            } else {
                field.setVarCharLength(255);
            }
        } else if (type.equals("enum")) {
            List<String> options = new ArrayList<>();
            StringTokenizer tokenizer = new StringTokenizer(enum_options, ",");
            while (tokenizer.hasMoreElements()) {
                options.add(tokenizer.nextToken().trim());
            }
            if (options.size() < 1) {
                throw new MolgenisModelException("enum_options must be ',' delimited for field '" + field.getName()
                        + "' of entity '" + entity.getName() + "'");
            }

            field.setEnumOptions(options);
        } else if (type.equals("xref") || type.equals("mref")) {
            if (mref_name.isEmpty() && xref_entity.isEmpty()) {
                throw new MolgenisModelException("xref_entity must be set for xref field '" + field.getName()
                        + "' of entity '" + entity.getName() + "'");
            }

            List<String> xref_labels = null;
            if (xref_label != null) {
                xref_labels = Arrays.asList(xref_label.split(","));
            }

            field.setXRefVariables(xref_entity, xref_field[0], xref_labels);

            if (type.equals("mref")) {
                if (!mref_name.isEmpty()) {
                    field.setMrefName(mref_name);
                }
                if (!mref_localid.isEmpty()) {
                    field.setMrefLocalid(mref_localid);
                }
                if (!mref_remoteid.isEmpty()) {
                    field.setMrefRemoteid(mref_remoteid);
                }
            }

            if (!element.getAttribute("xref_cascade").isEmpty()) {
                if (element.getAttribute("xref_cascade").equalsIgnoreCase("true")) {
                    field.setXrefCascade(true);
                } else {
                    throw new MolgenisModelException("Unknown option on xref_cascade: '"
                            + element.getAttribute("xref_cascade") + "'");
                }
            }
        }

        try {
            entity.addField(field);

        } catch (Exception e) {
            throw new MolgenisModelException("duplicate field '" + field.getName() + "' in entity '" + entity.getName()
                    + "'");
        }

        if (index.equals("true")) {
            Index i = new Index(name);
            try {
                i.addField(name);
            } catch (Exception e) {
                throw new MolgenisModelException("duplicate field '" + field.getName() + "' in entity '"
                        + entity.getName() + "'");
            }

            entity.addIndex(i);
        }

        if (unique.equals("true")) {
            entity.addKey(field.getName(), null);
        }

        if (element.getChildNodes().getLength() >= 1) {
            String annotations = org.apache.commons.lang.StringUtils.deleteWhitespace(
                    element.getChildNodes().item(1).getTextContent()).trim();
            field.setAnnotations(annotations);
        }
    }

    public static void parseView(Model model, Element element) throws MolgenisModelException {
        String name = Optional.of(element.getAttribute("name")).orElseThrow(() ->
                new MolgenisModelException("name is missing for view " + element));
        String label = Optional.of(element.getAttribute("label")).orElse(name);
        String entities = Optional.of(element.getAttribute("entities")).orElseThrow(() ->
                new MolgenisModelException("entities is missing for view " + element));

        List<String> entityList = Arrays.asList(entities.split(","));
        if (entityList.size() < 2) {
            throw new MolgenisModelException("a view needs at least 2 entities, define as entities=\"e1,e2\": " + element);
        }

        View view = new View(name, label, model.getDatabase());

        entityList.forEach(viewEntity -> {
            if (view.getEntities().contains(viewEntity)) {
                try {
                    throw new MolgenisModelException("view " + name + " has duplicate viewentity entries (" + viewEntity + ")");
                } catch (MolgenisModelException e) {
                    throw new RuntimeException(e);
                }
            }
            view.addEntity(viewEntity);
        });
    }

    public static void parseMethod(Model model, Element element) throws MolgenisModelException {
        if (element.getAttribute("name").isEmpty()) {
            String message = "name is missing for method " + element;
            logger.error(message);
            throw new MolgenisModelException(message);
        }

        Method method = new Method(element.getAttribute("name"), model.getMethodSchema());

        NodeList nodes = element.getChildNodes();
        for (int nodeId = 0; nodeId < nodes.getLength(); ++nodeId) {
            Node node = nodes.item(nodeId);
            if (node.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            Element nodeElement = (Element) node;
            String tagName = nodeElement.getTagName();
            switch (tagName) {
                case "description":
                    method.setDescription(nodeElement.getTextContent().trim());
                    break;
                case "parameter":
                    parseParameter(method, nodeElement);
                    break;
                case "return":
                    parseReturnType(model, method, nodeElement);
                    break;
                case "query":
                    parseQuery(model, method, nodeElement);
                    break;
                default:
                    logger.warn("Unknown element: " + tagName);
                    break;
            }
        }
    }

    public static void parseParameter(Method method, Element element) throws MolgenisModelException {
        if (element.getAttribute("name").isEmpty()) {
            String message = "name is missing for parameter " + element;
            logger.error(message);
            throw new MolgenisModelException(message);
        }
        if (element.getAttribute("type").isEmpty()) {
            String message = "type is missing for parameter " + element;
            logger.error(message);
            throw new MolgenisModelException(message);
        }
        Parameter parameter = new Parameter(method, Parameter.Type.getType(element.getAttribute("type")),
                element.getAttribute("name"), element.getAttribute("label"), false, element.getAttribute("default"));
        try {
            method.addParameter(parameter);
        } catch (Exception e) {
            throw new MolgenisModelException("duplicate parameter '" + parameter.getName() + "' in method '"
                    + method.getName() + "'");
        }
    }

    public static void parseReturnType(Model model, Method method, Element element) throws MolgenisModelException {
        if (element.getAttribute("type").isEmpty()) {
            String message = "type is missing for returntype " + element;
            logger.error(message);
            throw new MolgenisModelException(message);
        }

        Entity entity = model.getEntity(element.getAttribute("type"));

        try {
            method.setReturnType(entity);
        } catch (Exception e) {
        }
    }

    public static void parseQuery(Model model, Method method, Element element) throws MolgenisModelException {
        String type = element.getAttribute("entity");
        if (type.isEmpty()) {
            String message = "type is missing for returntype " + element;
            logger.error(message);
            throw new MolgenisModelException(message);
        }

        MethodQuery query = new MethodQuery(type);
        method.setQuery(query);

        NodeList nodes = element.getChildNodes();
        IntStream.range(0, nodes.getLength())
                .mapToObj(nodes::item)
                .filter(node -> node.getNodeType() == Node.ELEMENT_NODE)
                .filter(node -> ((Element) node).getTagName().equals("rule"))
                .forEach(node -> {
                    try {
                        parseQueryRule(model, query, (Element) node);
                    } catch (MolgenisModelException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public static void parseQueryRule(Model model, MethodQuery query, Element element) throws MolgenisModelException {
        validateAttribute(element, "field", "type is missing for field " + element);
        validateAttribute(element, "operator", "type is missing for operator " + element);
        validateAttribute(element, "parameter", "type is missing for parameter " + element);

        query.addRule(new MethodQuery.Rule(
                element.getAttribute("field"),
                element.getAttribute("operator"),
                element.getAttribute("parameter")
        ));
    }

    private static void validateAttribute(Element element, String attributeName, String errorMessage) throws MolgenisModelException {
        if (element.getAttribute(attributeName).isEmpty()) {
            logger.error(errorMessage);
            throw new MolgenisModelException(errorMessage);
        }
    }

    public static Model parseDbSchema(String xml) throws MolgenisModelException {
        Model model = new Model("molgenis");

        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

            ByteArrayInputStream is = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));

            Document document = builder.parse(is);

            parseXmlDocument(model, document);
        } catch (Exception e) {
            e.printStackTrace();
            throw new MolgenisModelException(e.getMessage());
        }

        return model;
    }

    public static Model parseDbSchema(List<String> filenames) throws MolgenisModelException {
        Model model = new Model("molgenis");

        for (String filename : filenames) {
            DocumentBuilder builder;
            try {
                builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                Document document = builder.parse(filename.trim());
                parseXmlDocument(model, document);
            } catch (ParserConfigurationException e) {
                throw new RuntimeException(e);
            } catch (Exception e) {
                try {
                    builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                    Document document = builder.parse(ClassLoader.getSystemResourceAsStream(filename.trim()));
                    parseXmlDocument(model, document);
                } catch (Exception e2) {
                    logger.error("parsing of file '" + filename + "' failed.");
                    e.printStackTrace();
                    throw new MolgenisModelException("Parsing of DSL (schema) failed: " + e2.getMessage());
                }
            }
        }

        return model;
    }

    private static Document parseXmlDocument(Model model, Document document) throws MolgenisModelException {
        try {
            Element document_root = document.getDocumentElement();

            Optional.ofNullable(document_root.getAttribute("name"))
                    .filter(String::isEmpty)
                    .ifPresent(name -> document_root.setAttribute("name", "molgenis"));

            String modelName = document_root.getAttribute("name");
            String modelLabel = document_root.getAttribute("label");

            model.setName(modelName);

            Optional.of(modelLabel)
                    .filter(label -> !label.isEmpty())
                    .ifPresent(model::setLabel);

            NodeList children = document_root.getChildNodes();

            IntStream.range(0, children.getLength())
                    .mapToObj(children::item)
                    .filter(child -> child.getNodeType() == Node.ELEMENT_NODE)
                    .map(Element.class::cast)
                    .forEach(element -> parseElementType(model, element));

            return document;
        } catch (Exception e) {
            throw new MolgenisModelException("Error parsing XML document" + e.getMessage());
        }
    }

    private static void parseElementType(Model model, Element element) {
        Map<String, Consumer<Element>> elementHandlers = new HashMap<>();
        elementHandlers.put("module", elem -> {
            try {
                parseModule(model, elem);
            } catch (MolgenisModelException e) {
                throw new RuntimeException(e);
            }
        });
        elementHandlers.put("entity", elem -> {
            try {
                parseEntity(model, elem);
            } catch (MolgenisModelException e) {
                throw new RuntimeException(e);
            }
        });
        elementHandlers.put("matrix", elem -> {
            try {
                parseMatrix(model, elem);
            } catch (MolgenisModelException e) {
                throw new RuntimeException(e);
            }
        });
        elementHandlers.put("view", elem -> {
            try {
                parseView(model, elem);
            } catch (MolgenisModelException e) {
                throw new RuntimeException(e);
            }
        });
        elementHandlers.put("method", elem -> {
            try {
                parseMethod(model, elem);
            } catch (MolgenisModelException e) {
                throw new RuntimeException(e);
            }
        });
        elementHandlers.put("description", elem -> model.setDBDescription(model.getDBDescription() + elementValueToString(elem)));

        Consumer<Element> handler = elementHandlers.getOrDefault(element.getTagName(), elem -> {
        });
        handler.accept(element);
    }

    public static void parseModule(Model model, Element element) throws MolgenisModelException {
        String[] keywords = {"name", "label"};

        List<String> keyWords = Arrays.asList(keywords);

        IntStream.range(0, element.getAttributes().getLength())
                .mapToObj(i -> element.getAttributes().item(i).getNodeName())
                .filter(nodeName -> !keyWords.contains(nodeName))
                .findFirst()
                .ifPresent(nodeName -> {
                    try {
                        throw new MolgenisModelException("attribute '" + nodeName + "' unknown for <module " + element.getAttribute("name") + ">");
                    } catch (MolgenisModelException e) {
                        throw new RuntimeException(e);
                    }
                });

        if (element.getAttribute("name").trim().isEmpty()) {
            String message = "name is missing for module " + element;
            logger.error(message);
            throw new MolgenisModelException(message);
        }

        Module module = new Module(model.getName() + "." + element.getAttribute("name").trim(), model);

        Optional.ofNullable(element.getAttribute("label"))
                .filter(label -> !label.isEmpty())
                .ifPresent(module::setLabel);

        NodeList elements = element.getElementsByTagName("description");
        NodeList finalElements = elements;
        IntStream.range(0, elements.getLength())
                .mapToObj(i -> (Element) finalElements.item(i))
                .filter(elm -> elm.getParentNode().equals(element))
                .findFirst()
                .map(MolgenisModelParser::elementValueToString)
                .ifPresent(module::setDescription);

        elements = element.getElementsByTagName("entity");
        IntStream.range(0, elements.getLength())
                .mapToObj(i -> (Element) finalElements.item(i))
                .map(elem -> {
                    Entity e;
                    try {
                        e = parseEntity(model, elem);
                    } catch (MolgenisModelException ex) {
                        throw new RuntimeException(ex);
                    }
                    e.setNamespace(module.getName());
                    e.setModule(module);
                    return e;
                })
                .forEach(module.getEntities()::add);
    }

    //todo не переписан
    private static void parseUiSchema(Model model, Element element, UISchema parent) throws MolgenisModelException {
        UISchema new_parent = null;

        if (element.getTagName().equals("include")) {
            String fileName = element.getAttribute("file");
            if (fileName == null || fileName.isEmpty()) {
                throw new MolgenisModelException("include failed: no file attribute set");
            }
            try {
                Document document_root = parseXmlFile(fileName);
                // include all its elements, ignore the root node
                NodeList rootChildren = document_root.getChildNodes();
                for (int i = 0; i < rootChildren.getLength(); i++) {
                    NodeList children = rootChildren.item(i).getChildNodes();
                    for (int j = 0; j < children.getLength(); j++) {
                        Node child = children.item(j);
                        if (child.getNodeType() != Node.ELEMENT_NODE) {
                            continue;
                        }
                        try {

                            parseUiSchema(model, (Element) child, parent);

                        } catch (Exception e) {
                            throw new MolgenisModelException(e.getMessage());
                        }

                    }
                }

            } catch (Exception e) {
                throw new MolgenisModelException("include failed: " + e.getMessage());
            }
        } else if (element.getTagName().equals("description")) {
            logger.warn("currently the '<description>' tag is ignored in ui.xml");
        } else {

            String name = element.getAttribute("name").trim();
            String namespace = model.getName();
            String label = element.getAttribute("label");
            String group = element.getAttribute("group");
            String groupRead = element.getAttribute("groupRead");

            // check required properties
            if ((name == null || name.isEmpty()) && !element.getTagName().equals("form")) {
                throw new MolgenisModelException("name is missing for subform of screen '" + parent.getName() + "'");
            }
            if (label != null && label.isEmpty()) {
                label = name;
            }

            if (group.isEmpty()) {
                group = null; // TODO: Discuss with Erik/Morris/Robert!
            }

            if (groupRead.isEmpty()) {
                groupRead = null; // TODO: Discuss with Erik/Morris/Robert!
            }

            // add this element to the meta-model
            if (element.getTagName().equals("menu")) {
                Menu menu = new Menu(name, parent);
                menu.setLabel(label);
                menu.setGroup(group);
                menu.setGroupRead(groupRead);
                menu.setNamespace(namespace);

                if (group != null && group.equals(groupRead)) {
                    throw new MolgenisModelException(
                            "You cannot assign both read/write and read rights on a single menu");
                }

                if (element.getAttribute("position") == null || !element.getAttribute("position").isEmpty()) {
                    menu.setPosition(Menu.Position.getPosition(element.getAttribute("position")));
                }

                new_parent = menu;
            } else if (element.getTagName().equals("form")) {
                if (name.isEmpty()) {
                    name = element.getAttribute("entity");
                }
                Form form = new Form(name, parent);
                form.setLabel(label);
                form.setGroup(group);
                form.setGroupRead(groupRead);

                if (group != null && group.equals(groupRead)) {
                    throw new MolgenisModelException(
                            "You cannot assign both read/write and read rights on a single form");
                }

                /** Optional custom header for the selected form screen */
                String header = element.getAttribute("header");
                if (!header.isEmpty()) form.setHeader(header);

                /** Optional description for the selected form screen */
                String description = element.getAttribute("description");
                if (!description.isEmpty()) form.setDescription(description);

                form.setNamespace(namespace);
                new_parent = form;

                // VIEWTYPE
                if (element.getAttribute("view").equals("record")) {
                    element.setAttribute("view", "edit");
                }
                if (element.getAttribute("view").isEmpty()) {
                    if (element.getChildNodes().getLength() > 0) {
                        element.setAttribute("view", "edit");
                    } else {
                        element.setAttribute("view", "list");
                    }
                }
                if (Form.ViewType.parseViewType(element.getAttribute("view")) == Form.ViewType.VIEWTYPE_UNKNOWN) {
                    throw new MolgenisModelException("view '" + element.getAttribute("view") + "' unknown for form '"
                            + form.getName() + "'");
                }
                form.setViewType(Form.ViewType.parseViewType(element.getAttribute("view")));

                // LIMIT
                form.setLimit(10);
                String limit = element.getAttribute("limit");
                if (limit != null && !limit.isEmpty()) {
                    form.setLimit(Integer.parseInt(limit));
                }

                // ACTIONS
                form.setCommands(new ArrayList<String>());
                String commands = element.getAttribute("commands");
                if (commands != null && !commands.isEmpty()) {
                    String[] commandArray = commands.split(",");
                    for (String command : commandArray) {
                        form.getCommands().add(command.trim());
                    }
                }

                // SORT
                String sortby = element.getAttribute("sortby");
                if (sortby != null && !sortby.isEmpty()) {
                    // TODO ensure valid sort field
                    form.setSortby(sortby);
                }
                String sortorder = element.getAttribute("sortorder");
                if (sortorder != null && !sortorder.isEmpty()) {
                    if (!sortorder.equalsIgnoreCase(Form.SortOrder.ASC.toString())
                            && !sortorder.equalsIgnoreCase(Form.SortOrder.DESC.toString())) {
                        throw new MolgenisModelException(
                                "sortorder can only be 'asc' or 'desc'. Parser found <form name=\"" + form.getName()
                                        + "\" sortorder=\"" + sortorder + "\"");
                    } else {

                        form.setSortorder(SortOrder.parse(sortorder));
                    }
                }

                // FILTER
                String filter = element.getAttribute("filter");
                if (filter != null && filter.equals("true")) {
                    if (element.getAttribute("filterfield") != null && element.getAttribute("filterfield").isEmpty()) {
                        throw new MolgenisModelException("filterfield is missing for subform of screen '"
                                + parent.getName() + "'");
                    }
                    if (element.getAttribute("filtertype") != null && element.getAttribute("filtertype").isEmpty()) {
                        throw new MolgenisModelException("filtertype is missing for subform of screen '"
                                + parent.getName() + "'");
                    }
                    if (element.getAttribute("filtervalue") != null && element.getAttribute("filtervalue").isEmpty()) {
                        logger.warn("filtervalue is missing for subform of screen '" + parent.getName() + "'");
                    }
                    form.setFilter(true);
                    form.setFilterfield(element.getAttribute("filterfield"));
                    form.setFiltertype(element.getAttribute("filtertype"));
                    form.setFiltervalue(element.getAttribute("filtervalue"));
                }

                // READONLY
                form.setReadOnly(false);
                String readonly = element.getAttribute("readonly");
                if (readonly != null) {
                    form.setReadOnly(Boolean.parseBoolean(readonly));
                }

                // ENTITY
                // TODO: whould have expected this in the constructor!
                Entity entity = (Entity) model.getDatabase().getChild(element.getAttribute("entity"));
                if (entity == null) {
                    throw new MolgenisModelException("Could not find the specified entity '"
                            + element.getAttribute("entity") + "' for form '" + form.getName() + "'");
                }
                form.setRecord(entity);// form.setEntity(entity);

                // HIDDEN FIELDS
                form.setHideFields(new ArrayList<String>());
                String hide_fields = element.getAttribute("hide_fields");
                if (hide_fields != null && !hide_fields.isEmpty()) {
                    String[] hiddenFieldArray = hide_fields.split(",");
                    for (String field : hiddenFieldArray) {
                        Field f = entity.getAllField(field.trim());
                        if (f == null) {
                            throw new MolgenisModelException("Could not find field '" + field
                                    + "' defined in hide_fields='" + element.getAttribute("hide_fields")
                                    + "' in form '" + form.getName() + "'");
                        }
                        // use name from 'f' to correct for case problems
                        form.getHideFields().add(f.getName());
                    }
                }

                // COMPACT_FIELDS
                if (!element.getAttribute("compact_view").isEmpty()) {
                    String[] fields = element.getAttribute("compact_view").split(",");
                    // check if the fields are there
                    List<String> compact_fields = new ArrayList<String>();
                    for (String field : fields) {
                        Field f = entity.getAllField(field);
                        if (f == null) {
                            throw new MolgenisModelException("Could not find field '" + field
                                    + "' defined in compact_view='" + element.getAttribute("compact_view")
                                    + "' in form '" + form.getName() + "'");
                        }
                        // use name from 'f' to correct for case problems

                        compact_fields.add(form.getEntity().getName() + "_" + f.getName());
                    }
                    form.setCompactView(compact_fields);
                }
            } else if (element.getTagName().equals("tree")) {
                // check required properties
                if (element.getAttribute("parentfield") != null && element.getAttribute("parentfield").isEmpty()) {
                    throw new MolgenisModelException("parentfield is missing for tree screen '" + name + "'");
                }
                if (element.getAttribute("idfield") != null && element.getAttribute("idfield").isEmpty()) {
                    throw new MolgenisModelException("idfield is missing for tree screen '" + name + "'");
                }
                if (element.getAttribute("labelfield") != null && element.getAttribute("labelfield").isEmpty()) {
                    throw new MolgenisModelException("labelfield is missing for tree screen '" + name + "'");
                }

                Tree tree = new Tree(name, parent, element.getAttribute("parentfield"),
                        element.getAttribute("idfield"), element.getAttribute("labelfield"));
                tree.setLabel(label);
                tree.setGroup(group);
                tree.setGroupRead(groupRead);

                if (group != null && group.equals(groupRead)) {
                    throw new MolgenisModelException(
                            "You cannot assign both read/write and read rights on a single tree");
                }

                tree.setNamespace(namespace);
                new_parent = tree;

                // READONLY
                tree.setReadOnly(true);
                String readonly = element.getAttribute("readonly");
                if (readonly != null) {
                    tree.setReadOnly(Boolean.parseBoolean(readonly));
                }

                // ENTITY
                // TODO: whould have expected this in the constructor!
                DBSchema entity = model.getDatabase().getChild(element.getAttribute("entity"));
                if (entity == null) {
                    throw new MolgenisModelException("Could not find the specified entity '"
                            + element.getAttribute("entity") + "'");
                }
                tree.setRecord((Record) entity);
            } else if (element.getTagName().equals("plugin")) {
                if (element.getAttribute("type") != null && element.getAttribute("type").isEmpty()) {
                    throw new MolgenisModelException("plugin has no name");
                }
                Plugin plugin = new Plugin(name, parent, element.getAttribute("type"));
                plugin.setLabel(label);
                plugin.setGroup(group);
                plugin.setGroupRead(groupRead);

                if (group != null && group.equals(groupRead)) {
                    throw new MolgenisModelException(
                            "You cannot assign both read/write and read rights on a single plugin");
                }

                plugin.setNamespace(namespace);
                new_parent = plugin;

                // METHOD
                String method = element.getAttribute("flavor");
                if (!"".equals(method)) {
                    plugin.setPluginMethod(Flavor.getPluginMethod(method));
                }

                // READONLY
                plugin.setReadOnly(false);
                String readonly = element.getAttribute("readonly");
                if (readonly != null) {
                    plugin.setReadOnly(Boolean.parseBoolean(readonly));
                }
            }
            /*
             * else { // this is the unexpected throw new Exception("Encountered
             * unknown element: " + element.getTagName()); }
             */

            // recurse the children
            NodeList children = element.getChildNodes();

            for (int i = 0; i < children.getLength(); i++) {
                Node child = children.item(i);

                if (child.getNodeType() != Node.ELEMENT_NODE) {
                    continue;
                }

                parseUiSchema(model, (Element) child, new_parent);
            }
        }
    }

    private static Document parseXmlFile(String filename) throws MolgenisModelException {
        Document document;
        DocumentBuilder builder;

        try {
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            document = builder.parse(getFileInputStream(filename));
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("parsing of file '" + filename + "' failed.");
            e.printStackTrace();
            throw new MolgenisModelException("Parsing of DSL (ui) failed: " + e.getMessage());
        }

        return document;
    }

    private static InputStream getFileInputStream(String filename) throws Exception {
        Path getFilePath = Paths.get(filename);
        return Files.newInputStream(getFilePath);
    }

    public static Model parseUiSchema(String filename, Model model) throws MolgenisModelException {
        logger.debug("parsing ui file: " + filename);

        if (filename == null || filename.isEmpty()) {
            return model;
        }

        Document document = parseXmlFile(filename);
        Element document_root = document.getDocumentElement();

        String modelName = Optional.ofNullable(document_root.getAttribute("name"))
                .filter(name -> !name.isEmpty())
                .orElse("molgenis");
        model.setName(modelName);

        String label = document_root.getAttribute("label");
        Optional.of(label).filter(s -> !s.isEmpty()).ifPresent(model::setLabel);

        NodeList children = document_root.getChildNodes();
        IntStream.range(0, children.getLength())
                .mapToObj(children::item)
                .filter(child -> child.getNodeType() == Node.ELEMENT_NODE)
                .forEach(child -> {
                    try {
                        String nodeName = child.getNodeName();
                        if (List.of("description", "form", "plugin", "menu", "include").contains(nodeName)) {
                            parseUiSchema(model, (Element) child, model.getUserinterface());
                        } else {
                            throw new MolgenisModelException("Unrecognized element: " + nodeName);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        try {
                            throw new MolgenisModelException(e.getMessage());
                        } catch (MolgenisModelException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                });

        return model;
    }

    private static String elementValueToString(Element element) {
        String xml;

        try (StringWriter writer = new StringWriter()) {
            OutputFormat format = new OutputFormat(element.getOwnerDocument());
            format.setIndenting(true);
            format.setOmitXMLDeclaration(true);

            XMLSerializer serializer = new XMLSerializer(writer, format);
            serializer.serialize(element);

            xml = writer.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        xml = xml.replaceAll("</?" + element.getTagName() + "[^>]*>", "");

        return xml;
    }

    private static final Logger logger = Logger.getLogger(MolgenisModelParser.class.getName());
}
