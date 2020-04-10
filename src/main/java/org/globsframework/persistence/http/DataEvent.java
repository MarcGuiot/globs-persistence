package org.globsframework.persistence.http;

import org.globsframework.directory.Directory;
import org.globsframework.remote.Serializer;
import org.globsframework.utils.serialization.SerializedInput;
import org.globsframework.utils.serialization.SerializedOutput;

public class DataEvent {
    final public String listenerUUID;
    final public String oldValue;
    final public String newValue;
    final public String[] additional;

    public DataEvent(String listenerUUID, String oldValue, String newValue, String[] additional) {
        this.listenerUUID = listenerUUID;
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.additional = additional;
    }

    static public class EventsSerializer implements Serializer {
        public Class getClassType() {
            return DataEvent.class;
        }

        public Object read(SerializedInput serializedInput, Directory directory) {
            String listenerUUID = serializedInput.readUtf8String();
            String oldValue = serializedInput.readUtf8String();
            String newValue = serializedInput.readUtf8String();
            String[] additional = serializedInput.readStringArray();
            return new DataEvent(listenerUUID, oldValue, newValue, additional);
        }

        public void write(Object object, SerializedOutput serializedOutput) {
            DataEvent dataEvent = (DataEvent) object;
            serializedOutput.writeUtf8String(dataEvent.listenerUUID);
            serializedOutput.writeUtf8String(dataEvent.oldValue);
            serializedOutput.writeUtf8String(dataEvent.newValue);
            serializedOutput.write(dataEvent.additional);
        }
    }
}
