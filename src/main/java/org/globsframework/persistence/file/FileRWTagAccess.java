package org.globsframework.persistence.file;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.globsframework.json.GSonUtils;
import org.globsframework.json.GlobGSonDeserializer;
import org.globsframework.json.GlobTypeResolver;
import org.globsframework.model.Glob;
import org.globsframework.persistence.RWTagAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

public class FileRWTagAccess implements RWTagAccess {
    public static final int MAX_GLOB_COUNT = 10000;
    public static final int MAX_DAY = 2;
    private static final Logger LOGGER = LoggerFactory.getLogger(FileRWTagAccess.class);
    private final Path dir;
    private final GlobTypeResolver typeAccess;
    private Writer writer;
    private boolean first = true;
    private int size = 0;
    private Instant last = Instant.now();

    public FileRWTagAccess(Path dir, GlobTypeResolver typeAccess) {
        this.dir = dir;
        this.typeAccess = typeAccess;
        createNewOutputFile();
    }

    private void createNewOutputFile() {
        if (writer != null) {
            try {
                writer.append("]");
                writer.close();
            } catch (IOException e) {
                String s = "Can not write or close file ";
                LOGGER.error(s, e);
                throw new RuntimeException(s);
            }
        }
        File file;
        int loop = 0;
        do {
            Instant instant = Instant.now();
            ZonedDateTime zonedDateTime = instant.atZone(ZoneOffset.UTC);
            file = new File(dir.toFile(), zonedDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + ".json");
            if (file.exists()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
            }
        } while (file.exists() && loop++ < 5);

        if (file.exists()) {
            String s = file.getName() + " exist after 5 repeat";
            LOGGER.error(s);
            throw new RuntimeException(s);
        }
        try {
            size = 0;
            first = true;
            last = Instant.now();
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8));
            writer.append("[");
        } catch (Exception e) {
            String s = "Can not create or write to file " + file.getAbsolutePath();
            LOGGER.error(s, e);
            throw new RuntimeException(s);
        }
    }

    public synchronized void save(String uuid, Glob[] tags) {
        try {
            if (!first) {
                writer.append(",");
            }
            first = false;
            writer.append("{\"uuid\":\"").append(uuid).append("\",")
                    .append("\"data\":[");
            for (int i = 0, tagsLength = tags.length; i < tagsLength; i++) {
                Glob tag = tags[i];
                String s = GSonUtils.encode(tag, true);
                writer.append(s);
                if (i < tagsLength - 1) {
                    writer.append(",");
                }
                size++;
            }
            writer.append("]}");
            writer.flush();

            if (size > MAX_GLOB_COUNT || Duration.between(last, Instant.now()).compareTo(Duration.ofDays(MAX_DAY)) > 0) {
                createNewOutputFile();
            }
        } catch (Exception e) {
            throw new RuntimeException("Bug", e);
        }
    }

    public void listAll(TagListener consumer) {
        try {
            DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
            stream.forEach(path -> {
                try {
                    File file = path.toFile();
                    if (file.length() > 0) {
                        JsonReader jsonReader = new JsonReader(new BufferedReader(new FileReader(file)));
                        jsonReader.beginArray();
                        do {
                            jsonReader.beginObject();
                            String name = jsonReader.nextName();
                            if (!name.equals("uuid")) {

                            }
                            String uuid = jsonReader.nextString();
                            name = jsonReader.nextName();
                            if (!name.equals("data")) {

                            }
                            jsonReader.beginArray();
                            Iterator<Glob> it = new Iterator<Glob>() {
                                public boolean hasNext() {
                                    try {
                                        return jsonReader.peek() != JsonToken.END_ARRAY;
                                    } catch (Exception e) {
                                        throw new RuntimeException("EOF", e);
                                    }
                                }

                                public Glob next() {
                                    try {
                                        return GlobGSonDeserializer.read(jsonReader, typeAccess);
                                    } catch (IOException e) {
                                        throw new RuntimeException("EOF", e);
                                    }
                                }
                            };
                            consumer.accept(uuid, it);
                            // be sure iterator was fully read.
                            while (it.hasNext()) {
                                it.next();
                            }
                            jsonReader.endArray();
                            jsonReader.endObject();
                        } while (jsonReader.peek() != JsonToken.END_ARRAY);
                    }
                } catch (Exception e) {
                    LOGGER.error("While reading " + path.toString(), e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("For " + dir.toString(), e);
        }
    }

    public synchronized void shutdown() {
        LOGGER.info("closing tags");
        try {
            writer.append("]");
            writer.close();
        } catch (IOException e) {
            LOGGER.error("Fail to close tags", e);
        }
        writer = null;

    }

    static class Data {
        String uuid;
        Glob[] tags;
    }
}
