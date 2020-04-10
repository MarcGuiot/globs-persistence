package org.globsframework.persistence.http.client;

import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.model.MutableGlob;
import org.globsframework.sqlstreams.constraints.Constraint;

import java.util.List;
import java.util.Map;

public interface ClientPersistence {
    void pushData(Glob data, MutableGlob[] tags);

    List<Glob> list(GlobType type, Constraint constraint);

    Glob getData(Glob tag);

    Listener listen(GlobType type, Constraint constraint, OnChange consumer, GlobType[] additionalWantedTags);

    void updateTag(Glob tag, MutableGlob[] globs);

    interface OnChange {
        void change(Glob oldValue, Glob newValue, Map<GlobType, Glob> additionalTags); //if newValue == null => delete; if oldValue == null => create else update

        void reset();
    }

    interface Listener {
        void unregister();
    }

    void stop();

}
