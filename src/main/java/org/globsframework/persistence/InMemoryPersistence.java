package org.globsframework.persistence;

import org.globsframework.json.GSonUtils;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.type.DataType;
import org.globsframework.model.Glob;
import org.globsframework.model.MutableGlob;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;
import org.globsframework.sqlstreams.constraints.OperandVisitor;
import org.globsframework.sqlstreams.constraints.impl.*;
import org.globsframework.utils.collections.ConcurrentMapOfMaps;
import org.globsframework.utils.collections.MapOfMaps;
import org.globsframework.utils.collections.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.*;

public class InMemoryPersistence implements Persistence {
    private static Logger LOGGER = LoggerFactory.getLogger(InMemoryPersistence.class);
    private final RWDataAccess data;
    private final RWTagAccess tagAccess;
    private MapOfMaps<String, GlobType, Glob> tagsByUUID = new ConcurrentMapOfMaps<>();
    private MultiMap<GlobType, ChangeDetector> changeDetectors = new MultiMap<>();

    public InMemoryPersistence(RWDataAccess data, RWTagAccess tagAccess) {
        this.data = data;
        this.tagAccess = tagAccess;

        tagAccess.listAll((uuid, tags) -> {
            Map<GlobType, Glob> globTypeGlobMap = tagsByUUID.getModifiable(uuid);
            while (tags.hasNext()) {
                Glob glob = tags.next();
                globTypeGlobMap.put(glob.getType(), glob);
            }
        });
    }

    public String pushData(Glob data, MutableGlob[] tags) {
        String key = this.data.save(data);
        updateTags(tags, key);
        return key;
    }

    private void updateTags(MutableGlob[] tags, String key) {
        for (MutableGlob tag : tags) {
            tag.set(getUUIDField(tag), key);
        }

        tagAccess.save(key, tags);

        for (Glob tag : tags) {
            GlobType type = tag.getType();
            tagsByUUID.put(key, type, tag);
        }

        for (Glob tag : tags) {
            List<ChangeDetector> changeDetectors = this.changeDetectors.get(tag.getType());
            for (ChangeDetector changeDetector : changeDetectors) {
                changeDetector.callChange(tag.getType(), null, tag, tagsByUUID.get(key));
            }
        }
    }

    public List<Glob> list(GlobType type, Constraint constraint) {
        List<Glob> result = new ArrayList<>();
        Filter filter = constraint.visit(new FilterConstraintVisitor()).filter;
        for (Map.Entry<String, Map<GlobType, Glob>> stringMapEntry : tagsByUUID.entry()) {
            Map<GlobType, Glob> map = stringMapEntry.getValue();
            if (filter.isEligible(map)) {
                Glob e = map.get(type);
                if (e != null) {
                    result.add(e);
                }
            }
        }
        return result;
    }

    public Glob getData(Glob tag) {
        String uuid = tag.get(getUUIDField(tag));
        return data.getData(uuid);
    }

    public Listener listen(GlobType type, Constraint constraint, Persistence.OnChange consumer, GlobType[] additionalWantedTags) {
        ChangeDetector value = new ChangeDetector(type, constraint == null ?
                d -> true : constraint.visit(new FilterConstraintVisitor()).filter, consumer, additionalWantedTags);
        changeDetectors.put(type, value);
        return new Listener() {
            public void unregister() {
                changeDetectors.get(type).remove(value);
            }
        };
    }

    public String updateTag(Glob refTag, MutableGlob[] tags) {
        String uuid = refTag.get(getUUIDField(refTag));
        if (uuid == null) {
            String s = "Missing uuid " + GSonUtils.encode(refTag, true);
            LOGGER.error(s);
            throw new RuntimeException(s);
        }
        updateTags(tags, uuid);
        return uuid;
    }

    public static StringField getUUIDField(Glob glob) {
        GlobType type = glob.getType();
        Field[] keyFields = type.getKeyFields();
        if (keyFields.length != 1) {
            String message = "One keyField of type string for uuid is expected for " + type.getName();
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
        Field field = keyFields[0];
        if (field.getDataType() != DataType.String) {
            String message = "keyField must be a string " + type.getName();
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
        return field.asStringField();
    }

    public void shutdown() {
        data.shutdown();
        tagAccess.shutdown();
    }

    interface Filter {
        boolean isEligible(Map<GlobType, Glob> data);
    }

    interface DataAccess {
        Object getData(Map<GlobType, Glob> data);
    }

    static class ChangeDetector {
        final GlobType observedType;
        final Filter filter;
        final Persistence.OnChange consumer;
        final private GlobType[] additionalWantedTags;

        public ChangeDetector(GlobType type, Filter filter, OnChange consumer, GlobType[] additionalWantedTags) {
            this.observedType = type;
            this.filter = filter;
            this.consumer = consumer;
            this.additionalWantedTags = additionalWantedTags == null || additionalWantedTags.length == 0? null : additionalWantedTags;
        }

        void callChange(GlobType globType, Glob oldTag, Glob newTag, Map<GlobType, Glob> data) {
            if (globType == observedType && filter.isEligible(data)) {
                List<Glob> other;
                if (additionalWantedTags != null) {
                    other = new ArrayList<>(additionalWantedTags.length);
                    for (GlobType additionalWantedTag : additionalWantedTags) {
                        Glob e = data.get(additionalWantedTag);
                        if (e != null) {
                            other.add(e);
                        }
                    }
                }
                else {
                    other = Collections.EMPTY_LIST;
                }
                consumer.change(globType, oldTag, newTag, other);
            }
        }
    }

//    public void addTag(Glob refTag, Glob newTag) {
//
//    }

    private static class FilterConstraintVisitor implements ConstraintVisitor {
        Filter filter;

        private FilterConstraintVisitor() {
        }

        public void visitEqual(EqualConstraint constraint) {
            DataAccess leftAccess = constraint.getLeftOperand().visitOperand(new ValueOperandVisitor()).value;
            DataAccess rightAccess = constraint.getRightOperand().visitOperand(new ValueOperandVisitor()).value;
            filter = data -> Objects.equals(leftAccess.getData(data), rightAccess.getData(data));
        }

        public void visitNotEqual(NotEqualConstraint constraint) {
            DataAccess leftAccess = constraint.getLeftOperand().visitOperand(new ValueOperandVisitor()).value;
            DataAccess rightAccess = constraint.getRightOperand().visitOperand(new ValueOperandVisitor()).value;
            filter = data -> !Objects.equals(leftAccess.getData(data), rightAccess.getData(data));

        }

        public void visitAnd(AndConstraint constraint) {
            Filter leftFilter = constraint.getLeftConstraint().visit(new FilterConstraintVisitor()).filter;
            Filter rightFilter = constraint.getRightConstraint().visit(new FilterConstraintVisitor()).filter;
            filter = data -> leftFilter.isEligible(data) && rightFilter.isEligible(data);
        }

        public void visitOr(OrConstraint constraint) {
            Filter leftFilter = constraint.getLeftConstraint().visit(new FilterConstraintVisitor()).filter;
            Filter rightFilter = constraint.getRightConstraint().visit(new FilterConstraintVisitor()).filter;
            filter = data -> leftFilter.isEligible(data) || rightFilter.isEligible(data);
        }

        public void visitLessThan(LessThanConstraint constraint) {
            ValueOperandVisitor leftVisitor = new ValueOperandVisitor();
            constraint.getLeftOperand().visitOperand(leftVisitor);
            DataAccess leftAccess = leftVisitor.value;
            ValueOperandVisitor rightVisitor = new ValueOperandVisitor();
            constraint.getRightOperand().visitOperand(rightVisitor);
            DataAccess rightAccess = rightVisitor.value;
            Field field = leftVisitor.field != null ? leftVisitor.field : rightVisitor.field;
            if (field != null) {
                DataType dataType = field.getDataType();
                if (dataType == DataType.DateTime) {
                    filter = data -> {
                        ZonedDateTime l = (ZonedDateTime) leftAccess.getData(data);
                        ZonedDateTime r = (ZonedDateTime) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isBefore(r) || l.isEqual(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Date) {
                    filter = data -> {
                        LocalDate l = (LocalDate) leftAccess.getData(data);
                        LocalDate r = (LocalDate) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isBefore(r) || l.isEqual(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Long || dataType == DataType.Double || dataType == DataType.Integer || dataType == DataType.BigDecimal) {
                    filter = data -> {
                        Comparable l = (Comparable) leftAccess.getData(data);
                        Comparable r = (Comparable) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.compareTo(r) <= 0;
                        }
                        return false;
                    };
                } else {
                    filter = data -> false;
                }
            }
        }

        public void visitBiggerThan(BiggerThanConstraint constraint) {
            ValueOperandVisitor leftVisitor = new ValueOperandVisitor();
            constraint.getLeftOperand().visitOperand(leftVisitor);
            DataAccess leftAccess = leftVisitor.value;
            ValueOperandVisitor rightVisitor = new ValueOperandVisitor();
            constraint.getRightOperand().visitOperand(rightVisitor);
            DataAccess rightAccess = rightVisitor.value;
            Field field = leftVisitor.field != null ? leftVisitor.field : rightVisitor.field;
            if (field != null) {
                DataType dataType = field.getDataType();
                if (dataType == DataType.DateTime) {
                    filter = data -> {
                        ZonedDateTime l = (ZonedDateTime) leftAccess.getData(data);
                        ZonedDateTime r = (ZonedDateTime) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isAfter(r) || l.isEqual(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Date) {
                    filter = data -> {
                        LocalDate l = (LocalDate) leftAccess.getData(data);
                        LocalDate r = (LocalDate) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isAfter(r) || l.isEqual(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Long || dataType == DataType.Double || dataType == DataType.Integer || dataType == DataType.BigDecimal) {
                    filter = data -> {
                        Comparable l = (Comparable) leftAccess.getData(data);
                        Comparable r = (Comparable) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.compareTo(r) >= 0;
                        }
                        return false;
                    };
                } else {
                    filter = data -> false;
                }
            }

        }

        public void visitStrictlyBiggerThan(StrictlyBiggerThanConstraint constraint) {
            ValueOperandVisitor leftVisitor = new ValueOperandVisitor();
            constraint.getLeftOperand().visitOperand(leftVisitor);
            DataAccess leftAccess = leftVisitor.value;
            ValueOperandVisitor rightVisitor = new ValueOperandVisitor();
            constraint.getRightOperand().visitOperand(rightVisitor);
            DataAccess rightAccess = rightVisitor.value;
            Field field = leftVisitor.field != null ? leftVisitor.field : rightVisitor.field;
            if (field != null) {
                DataType dataType = field.getDataType();
                if (dataType == DataType.DateTime) {
                    filter = data -> {
                        ZonedDateTime l = (ZonedDateTime) leftAccess.getData(data);
                        ZonedDateTime r = (ZonedDateTime) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isAfter(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Date) {
                    filter = data -> {
                        LocalDate l = (LocalDate) leftAccess.getData(data);
                        LocalDate r = (LocalDate) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isAfter(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Long || dataType == DataType.Double || dataType == DataType.Integer || dataType == DataType.BigDecimal) {
                    filter = data -> {
                        Comparable l = (Comparable) leftAccess.getData(data);
                        Comparable r = (Comparable) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.compareTo(r) > 0;
                        }
                        return false;
                    };
                } else {
                    filter = data -> false;
                }
            }

        }

        public void visitStrictlyLesserThan(StrictlyLesserThanConstraint constraint) {
            ValueOperandVisitor leftVisitor = new ValueOperandVisitor();
            constraint.getLeftOperand().visitOperand(leftVisitor);
            DataAccess leftAccess = leftVisitor.value;
            ValueOperandVisitor rightVisitor = new ValueOperandVisitor();
            constraint.getRightOperand().visitOperand(rightVisitor);
            DataAccess rightAccess = rightVisitor.value;
            Field field = leftVisitor.field != null ? leftVisitor.field : rightVisitor.field;
            if (field != null) {
                DataType dataType = field.getDataType();
                if (dataType == DataType.DateTime) {
                    filter = data -> {
                        ZonedDateTime l = (ZonedDateTime) leftAccess.getData(data);
                        ZonedDateTime r = (ZonedDateTime) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isBefore(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Date) {
                    filter = data -> {
                        LocalDate l = (LocalDate) leftAccess.getData(data);
                        LocalDate r = (LocalDate) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.isBefore(r);
                        }
                        return false;
                    };
                } else if (dataType == DataType.Long || dataType == DataType.Double || dataType == DataType.Integer || dataType == DataType.BigDecimal) {
                    filter = data -> {
                        Comparable l = (Comparable) leftAccess.getData(data);
                        Comparable r = (Comparable) rightAccess.getData(data);
                        if (l != null && r != null) {
                            return l.compareTo(r) < 0;
                        }
                        return false;
                    };
                } else {
                    filter = data -> false;
                }
            }

        }

        public void visitIn(InConstraint constraint) {
            String message = "Not implemented";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }

        public void visitIsOrNotNull(NullOrNotConstraint constraint) {
            String message = "Not implemented";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }

        public void visitNotIn(NotInConstraint constraint) {
            String message = "Not implemented";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }

        public void visitContains(Field field, String value, boolean contains) {
            String message = "Not implemented";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }

        private static class ValueOperandVisitor implements OperandVisitor {
            DataAccess value;
            Field field;

            public void visitValueOperand(ValueOperand value) {
                this.value = data -> value.getValue();
                this.field = value.getField();
            }

            public void visitAccessorOperand(AccessorOperand accessorOperand) {
                this.value = data -> accessorOperand.getAccessor().getObjectValue();
                this.field = accessorOperand.getField();
            }

            public void visitFieldOperand(Field field) {
                this.field = field;
                this.value = data -> {
                    Glob glob = data.get(field.getGlobType());
                    if (glob != null) {
                        return glob.getValue(field);
                    } else {
                        return null;
                    }
                };
            }
        }
    }
}
