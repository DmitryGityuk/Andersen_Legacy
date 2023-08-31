package org.molgenis.model;

import org.apache.log4j.Logger;
import org.molgenis.MolgenisOptions;
import org.molgenis.fieldtypes.MrefField;
import org.molgenis.fieldtypes.XrefField;
import org.molgenis.model.elements.Entity;
import org.molgenis.model.elements.Model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.stream.IntStream;

public class MolgenisModel {
    private static final Logger logger = Logger.getLogger(MolgenisModel.class.getSimpleName());

    public static Model parse(MolgenisOptions options) throws Exception {
        Model model;

        try {
            logger.info("parsing db-schema from " + options.model_database);

            model = MolgenisModelParser.parseDbSchema(options.model_database);

            Model finalModel = model;
            options.authorizable.stream()
                    .map(String::trim)
                    .forEach(eName -> {
                        Vector<String> implNames = finalModel.getEntity(eName).getImplementsNames();
                        if (!implNames.contains("Authorizable")) {
                            implNames.add("Authorizable");
                            finalModel.getEntity(eName).setImplements(implNames);
                        }
                    });

            logger.debug("read: " + model);

            MolgenisModelValidator.validate(model, options);

            logger.info("parsing ui-schema");
            model = MolgenisModelParser.parseUiSchema(options.path + options.model_userinterface, model);
            MolgenisModelValidator.validateUI(model);

            logger.debug("validated: " + model);
        } catch (MolgenisModelException e) {
            logger.error("Parsing failed: " + e.getMessage());
            throw e;
        }
        return model;
    }

    public static Model parse(Properties p) throws Exception {
        MolgenisOptions options = new MolgenisOptions(p);
        return parse(options);
    }

    public static List<Entity> sortEntitiesByDependency(List<Entity> entityList, final Model model) throws MolgenisModelException {
        List<Entity> result = new ArrayList<>();

        boolean found = true;
        List<Entity> toBeMoved = new ArrayList<>();
        while (entityList.size() > 0 && found) {
            found = false;
            for (Entity entity : entityList) {
                List<String> deps = getDependencies(entity, model);

                boolean missing = deps.stream()
                        .map(dep -> indexOf(result, dep))
                        .noneMatch(index -> index < 0);

                if (!missing) {
                    toBeMoved.add(entity);
                    result.add(entity);
                    found = true;
                    break;
                }
            }

            toBeMoved.forEach(entityList::remove);
            toBeMoved.clear();
        }

        for (Entity entity : entityList) {
            logger.error(String.format("cyclic relations to '%s' depends on %s", entity.getName(), getDependencies(entity, model)));
            result.add(entity);
        }

        result.forEach(e -> logger.info(e.getName()));

        return result;
    }

    private static int indexOf(List<Entity> entityList, String entityName) {
        return IntStream.range(0, entityList.size())
                .filter(i -> entityList.get(i).getName().equals(entityName))
                .findFirst()
                .orElse(-1);
    }

    private static List<String> getDependencies(Entity currentEntity, Model model) throws MolgenisModelException {
        Set<String> dependencies = new HashSet<>();

        currentEntity.getAllFields().stream()
                .filter(field -> field.getType() instanceof XrefField)
                .forEach(field -> {
                    dependencies.add(getEntityName(field.getXrefEntityName(), model));
                    field.getXrefEntity().getAllDescendants().stream()
                            .map(Entity::getName)
                            .filter(name -> !dependencies.contains(name))
                            .forEach(dependencies::add);
                });

        currentEntity.getAllFields().stream()
                .filter(field -> field.getType() instanceof MrefField)
                .forEach(field -> {
                    dependencies.add(getEntityName(field.getXrefEntity().getName(), model));
                    dependencies.addAll(model.getEntity(field.getXrefEntity().getName()).getParents());
                });

        dependencies.remove(currentEntity.getName());
        return new ArrayList<>(dependencies);
    }

    private static String getEntityName(String entityName, Model model) {
        return model.getEntity(entityName).getName();
    }
}
