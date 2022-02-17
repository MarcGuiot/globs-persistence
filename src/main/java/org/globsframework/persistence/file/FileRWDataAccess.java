package org.globsframework.persistence.file;

import com.google.gson.stream.JsonReader;
import org.globsframework.json.GSonUtils;
import org.globsframework.json.GlobGSonDeserializer;
import org.globsframework.metamodel.GlobTypeResolver;
import org.globsframework.model.Glob;
import org.globsframework.persistence.RWDataAccess;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class FileRWDataAccess implements RWDataAccess {
    private File dir;
    private GlobTypeResolver typeAccess;

    public FileRWDataAccess(File dir, GlobTypeResolver typeAccess) {
        this.dir = dir;
        this.typeAccess = typeAccess;
    }

    public Glob getData(String uuid) {
        File file = new File(dir, uuid + ".json");
        Glob glob = null;
        try {
            Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
            JsonReader jsonReader = new JsonReader(reader);
            jsonReader.beginObject();
            String uuidName = jsonReader.nextName();
            String readUUID = jsonReader.nextString();
            if (!readUUID.equals(uuid)) {
                throw new RuntimeException("Bad UUID");
            }
            String dataName = jsonReader.nextName();
            glob = GlobGSonDeserializer.read(jsonReader, typeAccess);
            jsonReader.endObject();
        } catch (IOException e) {
            throw new RuntimeException("Fail to read " + file.getAbsolutePath(), e);
        }
        return glob;
    }

    public String save(Glob glob) {
        File file;
        String uuid;
        do {
            uuid = UUID.randomUUID().toString();
            file = new File(dir, uuid + ".json");
        } while (file.exists());

        try {
            Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8));
            writer.append("{")
            .append("\"uuid\":\"").append(uuid).append("\",\"data\":");
            GSonUtils.encode(writer, glob, true);
            writer.append("}");
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException("Fail to write data for " + uuid, e);
        }
        return uuid;
    }

    public void shutdown() {
    }
}
