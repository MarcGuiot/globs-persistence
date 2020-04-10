package org.globsframework.persistence.http;

import org.globsframework.remote.rpc.ExportMethod;
import org.globsframework.utils.Ref;

import java.util.List;

public interface RpcPersistence {

    @ExportMethod
    void declareType(String type, String typesAsJson);

    @ExportMethod
    String pushData(String type, String data, String tags);

    @ExportMethod
    String list(String typeName, String name, String constraintAsJson);

    @ExportMethod
    void unregister(String listenerUUID, String uuid);

    @ExportMethod
    String getData(String typeName, String tag);

    @ExportMethod
    String updateTag(String typeName, String refTag, String tags);

    // return uuid for this listener
    @ExportMethod
    String register(String listenerUUID, String typeName, String listenerTypeName, String constraintAsJson, String[] additionalTypes);

    @ExportMethod
    long getNextEvents(String listenerUUID, long lastId, Ref<List<DataEvent>> events);

}
