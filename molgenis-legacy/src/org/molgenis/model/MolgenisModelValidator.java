package org.molgenis.model;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.molgenis.MolgenisOptions;
import org.molgenis.fieldtypes.EnumField;
import org.molgenis.fieldtypes.IntField;
import org.molgenis.fieldtypes.MrefField;
import org.molgenis.fieldtypes.StringField;
import org.molgenis.fieldtypes.TextField;
import org.molgenis.fieldtypes.XrefField;
import org.molgenis.framework.db.DatabaseException;
import org.molgenis.model.elements.Entity;
import org.molgenis.model.elements.Field;
import org.molgenis.model.elements.Model;
import org.molgenis.model.elements.Unique;
import org.molgenis.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Vector;
import java.util.stream.Collectors;

public class MolgenisModelValidator {
    private static final Logger logger = Logger.getLogger(MolgenisModelValidator.class.getSimpleName());

    public static void validate(Model model, MolgenisOptions options) throws MolgenisModelException, DatabaseException {
        logger.debug("validating model and adding defaults:");

        validateNamesAndReservedWords(model, options);
        validateExtendsAndImplements(model);

        if (options.object_relational_mapping.equals(MolgenisOptions.SUBCLASS_PER_TABLE)) {
            addTypeFieldInSubclasses(model);
        }

        validateKeys(model);
        addXrefLabelsToEntities(model);
        validatePrimaryKeys(model);
        validateForeignKeys(model);
        validateViews(model);
        validateOveride(model);
        correctXrefCaseSensitivity(model);
        moveMrefsFromInterfaceAndCopyToSubclass(model);
        createLinkTablesForMrefs(model, options);
        copyDefaultXrefLabels(model);
        copyDecoratorsToSubclass(model);

        if (options.object_relational_mapping.equals(MolgenisOptions.CLASS_PER_TABLE)) {
            addInterfaces(model);
        }

        copyFieldsToSubclassToEnforceConstraints(model);

        validateNameSize(model, options);

    }

    /**
     * As mrefs are a linking table between to other tables, interfaces cannot
     * be part of mrefs (as they don't have a linking table). To solve this
     * issue, mrefs will be removed from interface class and copied to subclass.
     *
     * @throws MolgenisModelException
     */
    public static void moveMrefsFromInterfaceAndCopyToSubclass(Model model) {
        logger.debug("copy fields to subclass for constrain checking...");

        model.getEntities().forEach(entity -> {
            try {
                entity.getImplements().forEach(iface -> {
                    try {
                        iface.getFieldsOf(new MrefField()).forEach(mref -> {
                            Field f = new Field(mref);
                            f.setEntity(entity);

                            String mrefName = entity.getName() + "_" + f.getName();
                            if (mrefName.length() > 30) {
                                mrefName = mrefName.substring(0, 25) + Integer.toString(mrefName.hashCode()).substring(0, 5);
                            }
                            f.setMrefName(mrefName);
                            entity.addField(0, f);
                        });
                    } catch (MolgenisModelException e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (MolgenisModelException e) {
                throw new RuntimeException(e);
            }
        });

        model.getEntities().forEach(entity -> {
            if (entity.isAbstract()) {
                try {
                    entity.getFieldsOf(new MrefField()).forEach(entity::removeField);
                } catch (MolgenisModelException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /**
     * Subclasses can override fields of superclasses. This should only be used
     * with caution! Only good motivation is to limit xref type.
     */
    public static void validateOveride(Model model) {
        // TODO

    }

    public static void validateNameSize(Model model, MolgenisOptions options) {
        model.getEntities().forEach(e -> {
            validateNameLength(options, e.getName(), "table");
            e.getFields().forEach(f -> validateNameLength(options, f.getName(), "field"));
        });
    }

    private static void validateNameLength(MolgenisOptions options, String name, String type) {
        if (options.db_driver.toLowerCase().contains("oracle") && name.length() > 30) {
            throw new RuntimeException(new MolgenisModelException(String.format("%s name %s is longer than %d", type, name, 30)));
        }
    }

    public static void validateUI(Model model) {
        logger.debug("validating UI and adding defaults:");

        validateHideFields(model);
    }

    public static void validateHideFields(Model model) {
        model.getUserinterface().getAllForms().forEach(form -> {
            List<String> hideFields = form.getHideFields();
            hideFields.forEach(fieldName -> {
                Entity entity = form.getEntity();
                Field field = Optional.ofNullable(entity.getAllField(fieldName)).orElseThrow(() -> new RuntimeException(String.format("error in hide_fields for form name=%s: cannot find field '%s' in form entity='%s'", form.getName(), fieldName, entity.getName())));

                if (!form.getReadOnly() && !field.isNillable() && !field.isAuto() && field.getDefaultValue().equals("")) {
                    logger.warn("you can get trouble with hiding field '" + fieldName + "' for form name=" + form.getName() + ": record is not null and doesn't have a default value (unless decorator fixes this!)");
                }
            });
        });
    }


    public static void addXrefLabelsToEntities(Model model) {
        model.getEntities().stream().filter(e -> e.getXrefLabels() == null).forEach(e -> {
            List<String> result = new ArrayList<>();
            if (e.getAllKeys().size() > 1) {
                e.getAllKeys().get(1).getFields().forEach(f -> result.add(f.getName()));
            } else if (e.getAllKeys().size() > 0) {
                e.getAllKeys().get(0).getFields().forEach(f -> result.add(f.getName()));
            }
            e.setXrefLabels(result);
            logger.debug("added default xref_label=" + e.getXrefLabels() + " to entity=" + e.getName());
        });
    }

    public static void validatePrimaryKeys(Model model) {
        model.getEntities().stream().filter(e -> !e.isAbstract()).filter(e -> e.getKeys().size() == 0).forEach(MolgenisModelValidator::accept);
    }

    /**
     * Default xref labels can come from: - the xref_entity (or one of its
     * superclasses)
     *
     * @param model
     * @throws MolgenisModelException
     */
    public static void copyDefaultXrefLabels(Model model) {
        model.getEntities().forEach(e -> e.getFields().forEach(f -> {
            if ((f.getType() instanceof XrefField || f.getType() instanceof MrefField) && f.getXrefLabelNames().size() > 0 && f.getXrefLabelNames().get(0).equals(f.getXrefFieldName()) && f.getXrefEntity().getXrefLabels() != null) {
                logger.debug("copying xref_label " + f.getXrefEntity().getXrefLabels() + " from " + f.getXrefEntityName() + " to field " + f.getEntity().getName() + "." + f.getName());
                f.setXrefLabelNames(f.getXrefEntity().getXrefLabels());
            }
        }));
    }

    /**
     * In each entity of an entity subclass hierarchy a 'type' field is added to
     * enable filtering. This method adds this type as 'enum' field such that
     * all subclasses are an enum option.
     *
     * @param model
     * @throws MolgenisModelException
     */
    public static void addTypeFieldInSubclasses(Model model) {
        logger.debug("add a 'type' field in subclasses to enable instanceof at database level...");
        model.getEntities().forEach(e -> {
            Vector<Entity> subclasses = e.getAllDescendants();
            Vector<String> enumOptions = new Vector<>();
            enumOptions.add(firstToUpper(e.getName()));
            subclasses.forEach(subclass -> enumOptions.add(firstToUpper(subclass.getName())));

            Field type_field = new Field(e, new EnumField(), Field.TYPE_FIELD, Field.TYPE_FIELD, true, false, true, null);
            type_field.setDescription("Subtypes have to be set to allow searching");
            type_field.setHidden(e.isRootAncestor());

            if (e.isRootAncestor()) {
                e.addField(0, type_field);
            } else {
                e.removeField(e.getField(Field.TYPE_FIELD));
            }

            e.getField(Field.TYPE_FIELD).setEnumOptions(enumOptions);
        });
    }

    /**
     * Add link tables for many to many relationships
     * <ul>
     * <li>A link table entity will have the name of [from_entity]_[to_entity]
     * <li>A link table has two xrefs to the from/to entity respectively
     * <li>The column names are those of the respective fields
     * <li>In case of a self reference, the second column name is '_self'
     * </ul>
     *
     * @param model
     * @throws MolgenisModelException
     */
    public static void createLinkTablesForMrefs(Model model, MolgenisOptions options) throws MolgenisModelException {
        logger.debug("add linktable entities for mrefs...");
        for (Entity xref_entity_from : model.getEntities()) {

            for (Field xref_field_from : xref_entity_from.getImplementedFieldsOf(new MrefField())) {
                try {
                    Entity xref_entity_to = xref_field_from.getXrefEntity();
                    Field xref_field_to = xref_field_from.getXrefField();
                    String mref_name = xref_field_from.getMrefName();

                    if (options.db_driver.toLowerCase().contains("oracle") && mref_name.length() > 30) {
                        throw new MolgenisModelException("mref_name cannot be longer then 30 characters, found: " + mref_name);
                    }
                    Entity mrefEntity = model.getEntity(mref_name);

                    if (mrefEntity == null) {
                        mrefEntity = new Entity(mref_name, mref_name, model.getDatabase());
                        mrefEntity.setNamespace(xref_entity_from.getNamespace());
                        mrefEntity.setAssociation(true);
                        mrefEntity.setDescription("Link table for many-to-many relationship '" + xref_entity_from.getName() + "." + xref_field_from.getName() + "'.");
                        mrefEntity.setSystem(true);
                        Field idField = new Field(mrefEntity, new IntField(), "autoid", "autoid", true, false, false, null);
                        idField.setHidden(true);
                        idField.setDescription("automatic id field to ensure ordering of mrefs");
                        mrefEntity.addField(idField);
                        mrefEntity.addKey(idField.getName(), "unique auto key to ensure ordering of mrefs");
                        Field field;
                        Vector<String> unique = new Vector<>();

                        field = new Field(mrefEntity, new XrefField(), xref_field_from.getMrefRemoteid(), null, false, false, false, null);
                        field.setXRefVariables(xref_entity_to.getName(), xref_field_to.getName(), xref_field_from.getXrefLabelNames());
                        if (xref_field_from.isXrefCascade()) field.setXrefCascade(true);
                        mrefEntity.addField(field);

                        unique.add(field.getName());
                        for (Field key : xref_entity_from.getKeyFields(Entity.PRIMARY_KEY)) {
                            field = new Field(mrefEntity, new XrefField(), xref_field_from.getMrefLocalid(), null, false, false, false, null);
                            field.setXRefVariables(xref_entity_from.getName(), key.getName(), null);

                            mrefEntity.addField(field);
                            unique.add(field.getName());
                        }
                        mrefEntity.addKey(unique, false, null);

                    } else {
                        Field xrefField = mrefEntity.getAllField(xref_field_to.getName());
                        if (xrefField != null) {
                            logger.debug("adding xref_label " + xref_field_to.getXrefLabelNames() + "'back' for " + xrefField.getName());
                            xrefField.setXrefLabelNames(xref_field_from.getXrefLabelNames());

                        }
                    }
                    xref_field_from.setMrefName(mrefEntity.getName());
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }
    }

    /**
     * Check if the view objects are an aggregate of known entities.
     *
     * @param model
     * @throws MolgenisModelException
     */
    public static void validateViews(Model model) {
        model.getViews().forEach(view -> {
            Vector<Entity> entities = new Vector<>();
            Vector<Pair<Entity, Entity>> references = new Vector<>();

            view.getEntities().forEach(viewEntity -> {
                Entity entity = model.getEntity(viewEntity);
                if (entity == null) {
                    throw new RuntimeException("Entity '" + viewEntity + "' in view '" + view.getName() + "' does not exist");
                }
                entities.add(entity);
            });

            entities.forEach(entity -> entity.getFields().stream().filter(field -> field.getType() instanceof XrefField).forEach(field -> {
                Entity referenced;
                referenced = field.getXrefEntity();
                entities.stream().filter(other -> !other.getName().equals(entity.getName())).filter(other -> other.getName().equals(referenced.getName())).forEach(other -> references.add(new Pair<>(entity, other)));
            }));

            Vector<Entity> viewEntities = new Vector<>();
            references.forEach(p -> {
                if (!viewEntities.contains(p.getA())) viewEntities.add(p.getA());
                if (!viewEntities.contains(p.getB())) viewEntities.add(p.getB());
            });
        });
    }

    /**
     * Validate foreign key relationships: <li>
     * <ul>
     * Do the xref_field and xref_label refer to fields actually exist
     * <ul>
     * Is the entity refered to non-abstract
     * <ul>
     * Does the xref_field refer to a unique field (i.e. foreign key)</li>
     *
     * @param model
     * @throws MolgenisModelException
     * @throws DatabaseException
     */
    public static void validateForeignKeys(Model model) throws MolgenisModelException, DatabaseException {
        logger.debug("validate xref_field and xref_label references...");
        for (Entity entity : model.getEntities()) {
            String entityname = entity.getName();

            for (Field field : entity.getFields()) {
                String fieldname = field.getName();
                if (field.getType() instanceof XrefField || field.getType() instanceof MrefField) {

                    String xref_entity_name = field.getXrefEntityName();
                    String xref_field_name = field.getXrefFieldName();

                    List<String> xref_label_names = field.getXrefLabelNames();

                    if (xref_label_names.size() == 0) {
                        xref_label_names.add(field.getXrefFieldName());
                    }

                    Entity xref_entity = model.getEntity(xref_entity_name);
                    if (xref_entity == null)
                        throw new MolgenisModelException("xref entity '" + xref_entity_name + "' does not exist for field " + entityname + "." + fieldname);

                    if (xref_field_name == null || xref_field_name.equals("")) {
                        xref_field_name = xref_entity.getPrimaryKey().getName();
                        field.setXrefField(xref_field_name);

                        logger.debug("automatically set " + entityname + "." + fieldname + " xref_field=" + xref_field_name);
                    }

                    if (!xref_entity.getName().equals(field.getXrefEntityName()))
                        throw new MolgenisModelException("xref entity '" + xref_entity_name + "' does not exist for field " + entityname + "." + fieldname + " (note: entity names are case-sensitive)");

                    if (xref_entity.isAbstract()) {
                        throw new MolgenisModelException("cannot refer to abstract xref entity '" + xref_entity_name + "' from field " + entityname + "." + fieldname);
                    }
                    if (entity.isAbstract() && field.getType() instanceof MrefField)
                        throw new MolgenisModelException("interfaces cannot have mref therefore remove '" + entityname + "." + fieldname + "'");

                    Field xref_field = xref_entity.getField(xref_field_name, false, true, true);

                    if (xref_field == null)
                        throw new MolgenisModelException("xref field '" + xref_field_name + "' does not exist for field " + entityname + "." + fieldname);

                    for (String xref_label_name : xref_label_names) {
                        Field xref_label;
                        if (xref_label_name.contains(".")) {
                            xref_label = model.findField(xref_label_name);
                        } else {
                            xref_label = xref_entity.getAllField(xref_label_name);
                        }
                        if (xref_label == null) {
                            StringBuilder validFieldsBuilder = new StringBuilder();
                            Map<String, List<Field>> candidates = field.allPossibleXrefLabels();

                            if (candidates.size() == 0) {
                                throw new MolgenisModelException("xref label '" + xref_label_name + "' does not exist for field " + entityname + "." + fieldname + ". \nCouldn't find suitable secondary keys to use as xref_label. \nDid you set a unique=\"true\" or <unique fields=\" ...>?");
                            }
                            for (Entry<String, List<Field>> entry : candidates.entrySet()) {
                                String key = entry.getKey();
                                if (xref_label_name.equals(key)) {
                                    List<Field> value = entry.getValue();
                                    xref_label = value.get(value.size() - 1);
                                }
                                validFieldsBuilder.append(',').append(key);
                            }
                            if (xref_label == null) {
                                throw new MolgenisModelException("xref label '" + xref_label_name + "' does not exist for field " + entityname + "." + fieldname + ". Valid labels include " + validFieldsBuilder);
                            }

                        } else {
                            if (!xref_label_name.equals(xref_field_name) && !field.allPossibleXrefLabels().keySet().contains(xref_label_name)) {
                                String validLabels = StringUtils.join(field.allPossibleXrefLabels().keySet(), ',');
                                throw new MolgenisModelException("xref label '" + xref_label_name + "' for " + entityname + "." + fieldname + " is not part a secondary key. Valid labels are " + validLabels + "\nDid you set a unique=\"true\" or <unique fields=\" ...>?");
                            }
                        }
                    }

                    if (xref_field.getType() instanceof TextField)
                        throw new MolgenisModelException("xref field '" + xref_field_name + "' is of illegal type 'TEXT' for field " + entityname + "." + fieldname);

                    String finalXref_field_name = xref_field_name;
                    boolean isUnique = xref_entity.getAllKeys().stream()
                            .flatMap(unique -> unique.getFields().stream())
                            .anyMatch(keyField -> keyField.getName().equals(finalXref_field_name));
                    if (!isUnique)
                        throw new MolgenisModelException("xref pointer '" + xref_entity_name + "." + xref_field_name + "' is a non-unique field for field " + entityname + "." + fieldname + "\n" + xref_entity.toString());
                }
            }
        }
    }

    /**
     * Validate the unique constraints
     * <ul>
     * <li>Do unique field names refer to existing fields?
     * <li>Is there a unique column id + unique label?
     * </ul>
     *
     * @param model
     * @throws MolgenisModelException
     */
    public static void validateKeys(Model model) throws MolgenisModelException {
        logger.debug("validate the fields used in 'unique' constraints...");
        for (Entity entity : model.getEntities()) {
            String entityName = entity.getName();
            int autoCount = 0;
            for (Field field : entity.getAllFields()) {
                String fieldName = field.getName();
                if (field.isAuto() && field.getType() instanceof IntField) {
                    autoCount++;

                    boolean isKey = false;

                    for (Unique unique : entity.getAllKeys()) {
                        for (Field keyfield : unique.getFields()) {
                            if (keyfield.getName() == null) throw new MolgenisModelException("unique field '"
                                    + fieldName + "' is not known in entity " + entityName);
                            if (keyfield.getName().equals(field.getName())) isKey = true;
                        }
                    }

                    if (!isKey) throw new MolgenisModelException(
                            "there can be only one auto column and it must be the primary key for field '" + entityName
                                    + "." + fieldName + "'");
                }

                if (field.getType() instanceof EnumField) {
                    if (field.getDefaultValue() != null && !"".equals(field.getDefaultValue())) if (!field
                            .getEnumOptions().contains(field.getDefaultValue())) {
                        throw new MolgenisModelException("default value '" + field.getDefaultValue()
                                + "' is not in enum_options for field '" + entityName + "." + fieldName + "'");
                    }
                }
            }

            if (autoCount > 1) throw new MolgenisModelException(
                    "there should be only one auto column and it must be the primary key for entity '" + entityName
                            + "'");

            if (!entity.isAbstract() && autoCount < 1) {
                throw new MolgenisModelException(
                        "there should be one auto column for each root entity and it must be the primary key for entity '"
                                + entityName + "'");
            }
        }
    }

    /**
     * Validate extends and implements relationships:
     * <ul>
     * <li>Do superclasses actually exist
     * <li>Do 'implements' refer to abstract superclasses (interfaces)
     * <li>Do 'extends' refer to non-abstract superclasses
     * <li>Copy primary key to subclass to form parent/child relationships
     * </ul>
     *
     * @param model
     * @throws MolgenisModelException
     */
    public static void validateExtendsAndImplements(Model model) throws MolgenisModelException {
        logger.debug("validate 'extends' and 'implements' relationships...");
        for (Entity entity : model.getEntities()) {

            entity.getAllImplements().stream().filter(iFace -> !iFace.isAbstract()).forEach(iFace -> {
                try {
                    List<Field> pkeyFields = iFace.getKeyFields(Entity.PRIMARY_KEY);

                    if (pkeyFields.size() == 1) {
                        Field pkeyField = pkeyFields.get(0);

                        if (entity.getField(pkeyField.getName()) == null) {
                            Field field = new Field(pkeyField);
                            field.setEntity(entity);
                            field.setAuto(pkeyField.isAuto());
                            field.setNillable(pkeyField.isNillable());
                            field.setReadonly(pkeyField.isReadOnly());
                            field.setXRefVariables(iFace.getName(), pkeyField.getName(), null);
                            field.setHidden(true);

                            logger.debug("copy primary key " + field.getName() + " from interface " + iFace.getName() + " to " + entity.getName());
                            entity.addField(field);
                        }
                    }
                } catch (MolgenisModelException e) {
                    e.getMessage();
                }
            });

            List<String> parents = entity.getParents();
            parents.stream().map(parentName -> {
                Entity parent = model.getEntity(parentName);
                try {
                    if (parent == null) {
                        throw new MolgenisModelException("'" + parentName + "''" + entity.getName() + "'");
                    } else if (parent.isAbstract()) {
                        throw new MolgenisModelException(entity.getName() + "" + parentName + "" + parentName + "'");
                    } else if (entity.isAbstract()) {
                        throw new MolgenisModelException(entity.getName() + "" + parentName + "" + entity.getName() + "'");
                    }
                } catch (MolgenisModelException e) {
                    throw new RuntimeException(e);
                }

                if (parent.getKeys().size() == 0) {
                    return null;
                } else {
                    try {
                        List<String> keys = parent.getKeyFields(Entity.PRIMARY_KEY).stream().filter(key -> entity.getField(key.getName()) == null).map(key -> {
                            Field field = new Field(key);
                            field.setEntity(entity);
                            field.setAuto(key.isAuto());
                            field.setNillable(key.isNillable());
                            field.setReadonly(key.isReadOnly());

                            field.setSystem(true);
                            field.setXRefVariables(parent.getName(), key.getName(), null);
                            field.setHidden(true);

                            entity.addField(field);
                            logger.debug("" + field.getName() + "" + parent.getName() + "" + entity.getName());
                            return field.getName();
                        }).collect(Collectors.toList());

                        if (!keys.isEmpty()) {
                            entity.getKeys().add(0, new Unique(entity, keys, false, ""));
                        }
                        return null;
                    } catch (MolgenisModelException e) {
                        return e.getMessage();
                    }
                }
            }).filter(Objects::nonNull);
        }
    }

    /**
     * Add interfaces as artificial entities to the model
     *
     * @param model
     * @throws MolgenisModelException
     * @throws Exception
     */
    public static void addInterfaces(Model model) throws MolgenisModelException {
        logger.debug("add root entities for interfaces...");
        for (Entity entity : model.getEntities()) {
            if (entity.isRootAncestor()) {
                Entity rootAncestor = entity;
                if (!entity.isAbstract()) {
                    rootAncestor = new Entity("_" + entity.getName() + "Interface", entity.getName(), model.getDatabase());
                    rootAncestor.setDescription("Identity map table for " + entity.getName() + " and all its subclasses. " +
                            "For each row that is added to " + entity.getName() + " or one of its subclasses, first a row must be added to this table to get a valid primary key value.");
                    rootAncestor.setAbstract(true);

                    Entity finalRootAncestor = rootAncestor;
                    entity.getKey(0).getFields().forEach(f -> {
                        Field key_field = new Field(finalRootAncestor, f.getType(), f.getName(), f.getName(), f.isAuto(), f.isNillable(), f.isReadOnly(), f.getDefaultValue());

                        key_field.setDescription("Primary key field unique in " + entity.getName() + " and its subclasses.");

                        if (key_field.getType() instanceof StringField) {
                            key_field.setVarCharLength(key_field.getVarCharLength());
                        }

                        finalRootAncestor.addField(key_field);
                    });

                    List<String> keyFields_copy = entity.getKey(0).getFields().stream()
                            .map(Field::getName)
                            .collect(Collectors.toCollection(Vector::new));

                    rootAncestor.addKey(keyFields_copy, entity.getKey(0).isSubclass(), null);

                    List<String> parents = new ArrayList<>();
                    parents.add(rootAncestor.getName());
                    entity.setParents(parents);
                }

                List<Entity> subclasses = entity.getAllDescendants();
                List<String> enumOptions = new ArrayList<>();
                enumOptions.add(entity.getName());
                subclasses.forEach(subclass -> enumOptions.add(subclass.getName()));

                Field typeField = new Field(rootAncestor, new EnumField(), Field.TYPE_FIELD, Field.TYPE_FIELD, true, false, false, null);
                typeField.setDescription("Subtypes of " + entity.getName() + ". Have to be set to allow searching");
                typeField.setEnumOptions(enumOptions);
                typeField.setHidden(true);
                rootAncestor.addField(0, typeField);

            }
        }
    }

    public static void validateNamesAndReservedWords(Model model, MolgenisOptions options) throws MolgenisModelException {
        logger.debug("check for JAVA and SQL reserved words...");

        List<String> keywords = new ArrayList<>();
        keywords.addAll(Arrays.asList(MOLGENIS_KEYWORDS));
        keywords.addAll(Arrays.asList(JAVA_KEYWORDS));
        keywords.addAll(Arrays.asList(JAVASCRIPT_KEYWORDS));
        keywords.addAll(Arrays.asList(ORACLE_KEYWORDS));

        if (options.db_driver.contains("mysql")) {
            keywords.addAll(Arrays.asList(MYSQL_KEYWORDS));
        }
        if (options.db_driver.contains("hsql")) {
            keywords.addAll(Arrays.asList(HSQL_KEYWORDS));
        }

        validateNameFormat(model.getName(), "model name");
        model.getModules().forEach(module -> validateNameFormat(module.getName(), "module name"));

        for (Entity e : model.getEntities()) {
            if (e.getName().contains(" ")) {
                throw new MolgenisModelException("entity name '" + e.getName() + "' cannot contain spaces. Use 'label' if you want to show a name with spaces.");
            }

            if (keywords.contains(e.getName().toUpperCase()) || keywords.contains(e.getName().toLowerCase())) {
                e.setName(e.getName() + "_");
                logger.warn("entity name '" + e.getName() + "' illegal:" + e.getName() + " is a reserved word");
                throw new MolgenisModelException("entity name '" + e.getName() + "' illegal:" + e.getName() + " is a reserved JAVA and/or SQL word and cannot be used for entity name");
            }
            for (Field f : e.getFields()) {
                if (f.getName().contains(" ")) {
                    throw new MolgenisModelException("field name '" + e.getName() + "." + f.getName() + "' cannot contain spaces. Use 'label' if you want to show a name with spaces.");
                }

                if (keywords.contains(f.getName().toUpperCase()) || keywords.contains(f.getName().toLowerCase())) {
                    f.setName(f.getName() + "_");
                    logger.warn("field name '" + f.getName() + "' illegal:" + f.getName() + " is a reserved word");
                    throw new MolgenisModelException("field name '" + e.getName() + "." + f.getName() + "' illegal: " + f.getName() + " is a reserved JAVA and/or SQL word");
                }

                if (f.getType() instanceof XrefField || f.getType() instanceof MrefField) {
                    String xref_entity = f.getXrefEntityName();
                    if (xref_entity != null && (keywords.contains(xref_entity.toUpperCase()) || keywords.contains(xref_entity.toLowerCase()))) {
                        throw new MolgenisModelException("xref_entity reference from field '" + e.getName() + "." + f.getName() + "' illegal: " + xref_entity + " is a reserved JAVA and/or SQL word");
                    }

                    if (f.getType() instanceof MrefField) {
                        if (f.getMrefName() == null) {
                            String mrefEntityName = f.getEntity().getName() + "_" + f.getName();
                            if (mrefEntityName.length() > 30) {
                                mrefEntityName = mrefEntityName.substring(0, 25) + Integer.toString(mrefEntityName.hashCode()).substring(0, 5);
                            }
                            Entity mrefEntity;
                            try {
                                mrefEntity = model.getEntity(mrefEntityName);
                            } catch (Exception exc) {
                                throw new MolgenisModelException("mref name for " + f.getEntity().getName() + "." + f.getName() + " not unique. Please use explicit mref_name=name setting");
                            }

                            if (mrefEntity != null) {
                                mrefEntityName += "_mref";
                                if (model.getEntity(mrefEntityName) != null) {
                                    mrefEntityName += "_" + Math.random();
                                }
                            }

                            f.setMrefName(mrefEntityName);
                        }
                        if (f.getMrefLocalid() == null) {
                            f.setMrefLocalid(f.getEntity().getName());
                        }
                        if (f.getMrefRemoteid() == null) {
                            f.setMrefRemoteid(f.getName());
                        }
                    }
                }
            }
        }

        model.getUserinterface().getAllChildren().stream()
                .filter(screen -> screen.getName().contains(" "))
                .forEach(screen -> {
                    try {
                        throw new MolgenisModelException("ui element '" + screen.getName() + "illegal: it cannot contain spaces. If you want to have a name with spaces use the 'label' attribute");
                    } catch (MolgenisModelException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static void validateNameFormat(String name, String type) {
        if (name.contains(" ")) {
            try {
                throw new MolgenisModelException(type + " '" + name + "' illegal: it cannot contain spaces. Use 'label' if you want to show a name with spaces.");
            } catch (MolgenisModelException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * test for case sensitivity
     */
    public static void correctXrefCaseSensitivity(Model model) {
        logger.debug("correct case of names in xrefs...");

        model.getEntities().forEach(entity -> entity.getFields().forEach(field -> {
            field.setName(field.getName().toLowerCase());

            if (field.getType() instanceof XrefField || field.getType() instanceof MrefField) {
                try {
                    Entity xrefEntity = field.getXrefEntity();
                    field.setXRefEntity(xrefEntity.getName());

                    String xrefField = field.getXrefField().getName();

                    List<String> xrefLabels = field.getXrefLabelsTemp();
                    List<String> correctedXrefLabels = xrefLabels.stream().map(xrefLabel -> xrefEntity.getAllField(xrefLabel).getName()).collect(Collectors.toList());
                    field.setXRefVariables(xrefEntity.getName(), xrefField, correctedXrefLabels);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error(e);
                }
            }
        }));
    }

    /**
     * @param model
     * @throws MolgenisModelException
     */
    public static void copyDecoratorsToSubclass(Model model) throws MolgenisModelException {
        logger.debug("copying decorators to subclasses...");
        for (Entity e : model.getEntities()) {
            if (e.getDecorator() == null) {
                for (Entity superClass : e.getImplements()) {
                    if (superClass.getDecorator() != null) {
                        e.setDecorator(superClass.getDecorator());
                    }
                }
                for (Entity superClass : e.getAllAncestors()) {
                    if (superClass.getDecorator() != null) {
                        e.setDecorator(superClass.getDecorator());
                    }
                }
            }
        }
    }

    /**
     * Copy fields to subclasses (redundantly) so this field can be part of an
     * extra constraint. E.g. a superclass has non-unique field 'name'; in the
     * subclass it is said to be unique and a copy is made to capture this
     * constraint in the table for the subclass.
     *
     * @param model
     * @throws MolgenisModelException
     */
    public static void copyFieldsToSubclassToEnforceConstraints(Model model) {
        logger.debug("copy fields to subclass for constrain checking...");

        model.getEntities().stream()
                .filter(Entity::hasAncestor)
                .forEach(e -> e.getKeys().forEach(key -> key.getFields().forEach(f -> {
                    if (e.getField(f.getName()) == null) {
                        Field copy = new Field(f);
                        copy.setEntity(e);
                        copy.setAuto(f.isAuto());
                        e.addField(copy);
                        logger.debug(key + " cannot be enforced on " + e.getName() + ", copying " + f.getEntity().getName() + "." + f.getName() + " to subclass as " + copy.getName());
                    }
                })));
    }

    private static final String[] MOLGENIS_KEYWORDS = {"entity", "field", "form", "menu", "screen", "plugin"};

    private static final String[] HSQL_KEYWORDS = {"ALIAS", "ALTER", "AUTOCOMMIT", "CALL", "CHECKPOINT", "COMMIT", "CONNECT", "CREATE", "COLLATION", "COUNT", "DATABASE", "DEFRAG", "DELAY", "DELETE", "DISCONNECT", "DROP", "END", "EXPLAIN", "EXTRACT", "GRANT", "IGNORECASE", "INDEX", "INSERT", "INTEGRITY", "LOGSIZE", "PASSWORD", "POSITION", "PLAN", "PROPERTY", "READONLY", "REFERENTIAl", "REVOKE", "ROLE", "ROLLBACK", "SAVEPOINT", "SCHEMA", "SCRIPT", "SCRIPTFORMAT", "SELECT", "SEQUENCE", "SET", "SHUTDOWN", "SOURCE", "TABLE", "TRIGGER", "UPDATE", "USER", "VIEW", "WRITE"};
    /**
     * http://dev.mysql.com/doc/refman/5.0/en/reserved-words.html
     */
    private static final String[] MYSQL_KEYWORDS = {"Type", "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN", "CONDITION", "CONNECTION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH", "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FLOAT", "FLOAT4", "FLOAT8", "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT", "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER", "INTERVAL", "INTO", "IS", "ITERATE", "JOIN", "KEY", "KEYS", "KILL", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY", "MATCH", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON", "OPTIMIZE", "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE", "RAID0", "READ", "READS", "REAL", "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SMALLINT", "SONAME", "SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STRAIGHT_JOIN", "TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "WHEN", "WHERE", "WHILE", "WITH", "WRITE", "X509", "XOR", "YEAR_MONTH", "ZEROFILL"};
    /**
     * https://cis.med.ucalgary.ca/http/java.sun.com/docs/books/tutorial/java/
     * nutsandbolts/_keywords.html
     */
    private static final String[] JAVA_KEYWORDS = {"abstract", "continue", "for", "new", "switch", "assert", "default", "goto", "package", "synchronized", "boolean", "do", "if", "private", "this", "break", "double", "implements", "protected", "throw", "byte", "else", "import", "public", "throws", "case", "enum", "instanceof", "return", "transient", "catch", "extends", "int", "short", "try", "char", "final", "interface", "static", "void", "class", "finally", "long", "strictfp", "volatile", "const", "float", "native", "super", "while"};

    private static final String[] JAVASCRIPT_KEYWORDS = {"function"};

    private static final String[] ORACLE_KEYWORDS = {

            "ACCESS", "ELSE", "MODIFY", "START", "ADD", "EXCLUSIVE", "NOAUDIT", "SELECT", "ALL", "EXISTS", "NOCOMPRESS", "SESSION", "ALTER", "FILE", "NOT", "SET", "AND", "FLOAT", "NOTFOUND", "SHARE", "ANY", "FOR", "NOWAIT", "SIZE", "ARRAYLEN", "FROM", "NULL", "SMALLINT", "AS", "GRANT", "NUMBER", "SQLBUF", "ASC", "GROUP", "OF", "SUCCESSFUL", "AUDIT", "HAVING", "OFFLINE", "SYNONYM", "BETWEEN", "IDENTIFIED", "ON", "SYSDATE", "BY", "IMMEDIATE", "ONLINE", "TABLE", "CHAR", "IN", "OPTION", "THEN", "CHECK", "INCREMENT", "OR", "TO", "CLUSTER", "INDEX", "ORDER", "TRIGGER", "COLUMN", "INITIAL", "PCTFREE", "UID", "COMMENT", "INSERT", "PRIOR", "UNION", "COMPRESS", "INTEGER", "PRIVILEGES", "UNIQUE", "CONNECT", "INTERSECT", "PUBLIC", "UPDATE", "CREATE", "INTO", "RAW", "USER", "CURRENT", "IS", "RENAME", "VALIDATE", "DATE", "LEVEL", "RESOURCE", "VALUES", "DECIMAL", "LIKE", "REVOKE", "VARCHAR", "DEFAULT", "LOCK", "ROW", "VARCHAR2", "DELETE", "LONG", "ROWID", "VIEW", "DESC", "MAXEXTENTS", "ROWLABEL", "WHENEVER", "DISTINCT", "MINUS", "ROWNUM", "WHERE", "DROP", "MODE", "ROWS", "WITH"};

    private static String firstToUpper(String string) {
        if (string == null) return " NULL ";
        if (string.length() > 0) return string.substring(0, 1).toUpperCase() + string.substring(1);
        else return " ERROR[STRING EMPTY] ";
    }

    private static void accept(Entity e) {
        try {
            throw new MolgenisModelException("entity '" + e.getName() + "' doesn't have a primary key defined");
        } catch (MolgenisModelException ex) {
            throw new RuntimeException(ex);
        }
    }
}