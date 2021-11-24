package org.globsframework.persistence.file;

import com.google.gson.Gson;
import org.globsframework.json.GlobsGson;
import org.globsframework.json.helper.LoadingGlobTypeResolver;
import org.globsframework.metamodel.Annotations;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.GlobArrayField;
import org.globsframework.metamodel.fields.GlobArrayUnionField;
import org.globsframework.metamodel.fields.GlobField;
import org.globsframework.metamodel.fields.GlobUnionField;
import org.globsframework.model.Glob;
import org.globsframework.persistence.FileGlobTypeAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Annotation;
import java.util.*;
import java.util.stream.Stream;

public class DefaultFileGlobTypeAccess implements FileGlobTypeAccess {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFileGlobTypeAccess.class);
    private final Path dir;
    private final Map<String, GlobType> schema = new HashMap<>();

    public DefaultFileGlobTypeAccess(Path dir) {
        this.dir = dir;
        load();
    }

    public GlobType find(String name) {
        return schema.get(name);
    }

    public void declare(GlobType globType) {
        if (schema.containsKey(globType.getName())) {
            return;
        }
        File file = new File(dir.toFile(), globType.getName() + ".json");
        if (file.exists()) {
            LOGGER.error("globType " + globType.getName() + " already declared.");
        }
        try {
            Gson gson = GlobsGson.create(null);
            String s = gson.toJson(globType);
            Writer writer = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8);
            writer.append(s);
            writer.close();
            schema.put(globType.getName(), globType);

            globType.streamAnnotations().map(Glob::getType).forEach(this::declare);
            globType.streamFields().flatMap(Annotations::streamAnnotations).map(Glob::getType).forEach(this::declare);
            globType.streamFields().flatMap(field -> field instanceof GlobField ? Stream.of(((GlobField) field).getTargetType()) :
                    field instanceof GlobUnionField ? ((GlobUnionField) field).getTargetTypes().stream() :
                            field instanceof GlobArrayField ? Stream.of(((GlobArrayField) field).getTargetType()) :
                                    field instanceof GlobArrayUnionField ? ((GlobArrayUnionField) field).getTargetTypes().stream() :
                                            Stream.empty()
            ).forEach(this::declare);

        } catch (IOException e) {
            String s = "fail to wirte globType " + globType.getName();
            LOGGER.error(s);
            throw new RuntimeException(s, e);
        }
    }

    public void shutdown() {
    }

    private void load() {
        try {
            LoadingGlobTypeResolver.Builder builder = LoadingGlobTypeResolver.builder(schema::get); // return null is not found
            DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
            stream.forEach(path -> {
                try {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toFile()), StandardCharsets.UTF_8))) {
                        builder.read(reader);
                    }
                } catch (IOException e) {
                    String s = "Fail to read " + path.toString();
                    LOGGER.error(s, e);
                    throw new RuntimeException(s, e);
                }
            });
            for (GlobType type : builder.read()) {
                schema.put(type.getName(), type);
            }
        } catch (IOException e) {
            String s = "Fail to read directory " + dir.toString();
            LOGGER.error(s, e);
            throw new RuntimeException(s, e);
        }
    }
}
