package ve.zlab.k;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import ve.zlab.k.functions.KRowFunction;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.response.DynamicObject;

public class KCollection {
    
    private List<KRow> collection;
    private final Map<String, Object> extra;
    private final Map<String, Integer> ref;
    private String table;
    private final List<String> exclude;

    public KCollection(final List<Object> objects, final Map<String, Integer> ref, final String table) {
        this.collection = new ArrayList();
        this.ref = ref;
        this.extra = new HashMap<>();
        this.table = table;
        this.exclude = new ArrayList<>();
        
        if (objects == null || objects.isEmpty()) {
            return;
        }
        
        final boolean isArrayObject = objects.get(0) instanceof Object[];
        
        if (isArrayObject) {
            for (Object object : objects) {
                this.collection.add(new KRow((Object[]) object, ref, this.table));
            }
        } else {
            for (Object object : objects) {
                this.collection.add(new KRow(new Object[] {
                    object
                }, ref, this.table));
            }
        }
    }
    
    public KCollection(final List<KRow> collection, final Map<String, Integer> ref, final Map<String, Object> extra, final String table, final List<String> exclude) {
        this.collection = collection;
        this.ref = ref;
        this.extra = extra;
        this.table = table;
        this.exclude = exclude;
    }
    
    public KCollection exclude(final String... excludes) {
        if (excludes == null) {
            return null;
        }

        for (final String exclude_ : excludes) {
            if (exclude_ == null) {
                continue;
            }
            
            this.exclude.add(exclude_);
        }

        return this;
    }
    
    public List<Object> pluck(final String c) {
        final List<Object> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.get(c));
        }
        
        return list;
    }
    
    public List<Object> pluck(final String c, final KRowFunction kRowFunction) throws KException {
        final List<Object> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.get(c));
            }
        }
        
        return list;
    }
    
    public List<Object> pluck(final int n) {
        final List<Object> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.get(n));
        }
        
        return list;
    }
    
    public List<Object> pluck(final int n, final KRowFunction kRowFunction) throws KException {
        final List<Object> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.get(n));
            }
        }
        
        return list;
    }
    
    public List<String> pluckString(final String c) {
        final List<String> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getString(c));
        }
        
        return list;
    }
    
    public List<String> pluckString(final String c, final KRowFunction kRowFunction) throws KException {
        final List<String> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getString(c));
            }
        }
        
        return list;
    }
    
    public List<String> pluckString(final int n) {
        final List<String> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getString(n));
        }
        
        return list;
    }
    
    public List<String> pluckString(final int n, final KRowFunction kRowFunction) throws KException {
        final List<String> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getString(n));
            }
        }
        
        return list;
    }
    
    public List<Character> pluckCharacter(final String c) {
        final List<Character> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getCharacter(c));
        }
        
        return list;
    }
    
    public List<Character> pluckCharacter(final String c, final KRowFunction kRowFunction) throws KException {
        final List<Character> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getCharacter(c));
            }
        }
        
        return list;
    }
    
    public List<Character> pluckCharacter(final int n) {
        final List<Character> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getCharacter(n));
        }
        
        return list;
    }
    
    public List<Character> pluckCharacter(final int n, final KRowFunction kRowFunction) throws KException {
        final List<Character> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getCharacter(n));
            }
        }
        
        return list;
    }
    
    public List<BigDecimal> pluckBigDecimal(final String c) {
        final List<BigDecimal> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getBigDecimal(c));
        }
        
        return list;
    }
    
    public List<BigDecimal> pluckBigDecimal(final String c, final KRowFunction kRowFunction) throws KException {
        final List<BigDecimal> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getBigDecimal(c));
            }
        }
        
        return list;
    }
    
    public List<BigDecimal> pluckBigDecimal(final int n) {
        final List<BigDecimal> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getBigDecimal(n));
        }
        
        return list;
    }
    
    public List<BigDecimal> pluckBigDecimal(final int n, final KRowFunction kRowFunction) throws KException {
        final List<BigDecimal> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getBigDecimal(n));
            }
        }
        
        return list;
    }
    
    public List<BigInteger> pluckBigInteger(final String c) {
        final List<BigInteger> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getBigInteger(c));
        }
        
        return list;
    }
    
    public List<BigInteger> pluckBigInteger(final String c, final KRowFunction kRowFunction) throws KException {
        final List<BigInteger> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getBigInteger(c));
            }
        }
        
        return list;
    }
    
    public List<BigInteger> pluckBigInteger(final int n) {
        final List<BigInteger> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getBigInteger(n));
        }
        
        return list;
    }
    
    public List<BigInteger> pluckBigInteger(final int n, final KRowFunction kRowFunction) throws KException {
        final List<BigInteger> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getBigInteger(n));
            }
        }
        
        return list;
    }
    
    public List<Long> pluckLong(final String c) {
        final List<Long> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getLong(c));
        }
        
        return list;
    }

    public List<Long> pluckLong(final String c, final KRowFunction kRowFunction) throws KException {
        final List<Long> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getLong(c));
            }
        }
        
        return list;
    }
    
    public List<Long> pluckLong(final int n) {
        final List<Long> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getLong(n));
        }
        
        return list;
    }
    
    public List<Long> pluckLong(final int n, final KRowFunction kRowFunction) throws KException {
        final List<Long> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getLong(n));
            }
        }
        
        return list;
    }
    
    public List<Integer> pluckInteger(final String c) {
        final List<Integer> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getInteger(c));
        }
        
        return list;
    }
    
    public List<Integer> pluckInteger(final String c, final KRowFunction kRowFunction) throws KException {
        final List<Integer> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getInteger(c));
            }
        }
        
        return list;
    }
    
    public List<Integer> pluckInteger(final int n) {
        final List<Integer> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getInteger(n));
        }
        
        return list;
    }
    
    public List<Integer> pluckInteger(final int n, final KRowFunction kRowFunction) throws KException {
        final List<Integer> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getInteger(n));
            }
        }
        
        return list;
    }
    
    public List<Boolean> pluckBoolean(final String c) {
        final List<Boolean> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getBoolean(c));
        }
        
        return list;
    }
    
    public List<Boolean> pluckBoolean(final String c, final KRowFunction kRowFunction) throws KException {
        final List<Boolean> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getBoolean(c));
            }
        }
        
        return list;
    }
    
    public List<Boolean> pluckBoolean(final int n) {
        final List<Boolean> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getBoolean(n));
        }
        
        return list;
    }
    
    public List<Boolean> pluckBoolean(final int n, final KRowFunction kRowFunction) throws KException {
        final List<Boolean> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getBoolean(n));
            }
        }
        
        return list;
    }
    
    public List<Double> pluckDouble(final String c) {
        final List<Double> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getDouble(c));
        }
        
        return list;
    }
    
    public List<Double> pluckDouble(final String c, final KRowFunction kRowFunction) throws KException {
        final List<Double> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getDouble(c));
            }
        }
        
        return list;
    }
    
    public List<Double> pluckDouble(final int n) {
        final List<Double> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getDouble(n));
        }
        
        return list;
    }
    
    public List<Double> pluckDouble(final int n, final KRowFunction kRowFunction) throws KException {
        final List<Double> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getDouble(n));
            }
        }
        
        return list;
    }
    
    public List<UUID> pluckUUID(final String c) {
        final List<UUID> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getUUID(c));
        }
        
        return list;
    }

    public List<UUID> pluckUUID(final String c, final KRowFunction kRowFunction) throws KException {
        final List<UUID> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getUUID(c));
            }
        }
        
        return list;
    }
    
    public List<UUID> pluckUUID(final int n) {
        final List<UUID> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getUUID(n));
        }
        
        return list;
    }
    
    public List<UUID> pluckUUID(final int n, final KRowFunction kRowFunction) throws KException {
        final List<UUID> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getUUID(n));
            }
        }
        
        return list;
    }
    
    public List<LocalDateTime> pluckLocalDateTime(final String c) {
        final List<LocalDateTime> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getLocalDateTime(c));
        }
        
        return list;
    }

    public List<LocalDateTime> pluckLocalDateTime(final String c, final KRowFunction kRowFunction) throws KException {
        final List<LocalDateTime> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getLocalDateTime(c));
            }
        }
        
        return list;
    }
    
    public List<LocalDateTime> pluckLocalDateTime(final int n) {
        final List<LocalDateTime> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getLocalDateTime(n));
        }
        
        return list;
    }
    
    public List<LocalDateTime> pluckLocalDateTime(final int n, final KRowFunction kRowFunction) throws KException {
        final List<LocalDateTime> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getLocalDateTime(n));
            }
        }
        
        return list;
    }
    
    public List<LocalDate> pluckLocalDate(final String c) {
        final List<LocalDate> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getLocalDate(c));
        }
        
        return list;
    }

    public List<LocalDate> pluckLocalDate(final String c, final KRowFunction kRowFunction) throws KException {
        final List<LocalDate> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getLocalDate(c));
            }
        }
        
        return list;
    }
    
    public List<LocalDate> pluckLocalDate(final int n) {
        final List<LocalDate> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getLocalDate(n));
        }
        
        return list;
    }
    
    public List<LocalDate> pluckLocalDate(final int n, final KRowFunction kRowFunction) throws KException {
        final List<LocalDate> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getLocalDate(n));
            }
        }
        
        return list;
    }
    
    public List<Date> pluckDate(final String c) {
        final List<Date> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getDate(c));
        }
        
        return list;
    }

    public List<Date> pluckDate(final String c, final KRowFunction kRowFunction) throws KException {
        final List<Date> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getDate(c));
            }
        }
        
        return list;
    }
    
    public List<Date> pluckDate(final int n) {
        final List<Date> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getDate(n));
        }
        
        return list;
    }
    
    public List<Date> pluckDate(final int n, final KRowFunction kRowFunction) throws KException {
        final List<Date> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getDate(n));
            }
        }
        
        return list;
    }
    
    public List<Timestamp> pluckTimestamp(final String c) {
        final List<Timestamp> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getTimestamp(c));
        }
        
        return list;
    }

    public List<Timestamp> pluckTimestamp(final String c, final KRowFunction kRowFunction) throws KException {
        final List<Timestamp> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getTimestamp(c));
            }
        }
        
        return list;
    }
    
    public List<Timestamp> pluckTimestamp(final int n) {
        final List<Timestamp> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            list.add(kRow.getTimestamp(n));
        }
        
        return list;
    }
    
    public List<Timestamp> pluckTimestamp(final int n, final KRowFunction kRowFunction) throws KException {
        final List<Timestamp> list = new ArrayList<>();
        
        for (KRow kRow : collection) {
            if ((Boolean) kRowFunction.run(kRow)) {
                list.add(kRow.getTimestamp(n));
            }
        }
        
        return list;
    }

    public List<KRow> getCollection() {
        return collection;
    }

    public void setCollection(List<KRow> collection) {
        this.collection = collection;
    }
    
    public KCollection addExtra(final String key, final Object value) {
        this.extra.put(key, value);
        
        return this;
    }
    
    public void addChildren(final String name, final String parentConnectionColumn, final String childConnectionColumn, final KCollection child) throws KException {
        this.addChildren(name, parentConnectionColumn, childConnectionColumn, child, false);
    }
    
    public void addChildren(final String name, final String parentConnectionColumn, final String childConnectionColumn, final KCollection child, final boolean keepConnection) throws KException {
        final Map<Object, List<KRow>> groups = new HashMap<>();
        
        for (final KRow kRow : child.getCollection()) {
            final Object parentValue = kRow.get(childConnectionColumn);
            
            if (parentValue == null) {
                continue;
            }
            
            final KRow clone = kRow.clone();
                
            if (!keepConnection) {
                clone.removeColumn(childConnectionColumn);
            }
            
            if (groups.containsKey(parentValue)) {
                groups.get(parentValue).add(clone);
            } else {
                groups.put(parentValue, new ArrayList<KRow>() {{ 
                    add(clone);
                }});
            }
        }
        
        for (final KRow kRow : collection) {
            final Object parentValue = kRow.get(parentConnectionColumn);
            
            final List<KRow> children;
            
            if (groups.containsKey(parentValue)) {
                children = groups.get(parentValue);
            } else {
                children = new ArrayList<>();
            }
            
            kRow.addColumn(name, children);
        }
    }
    
    private String toResponse(final String nameItems) {
        final List<Map<String, Object>> items = new ArrayList<>();
        
        for (final KRow kRow : collection) {
            items.add(kRow.toMap(this.exclude));
        }
        
        final DynamicObject response = DynamicObject.create().add(nameItems, items);
        
        if (!extra.isEmpty()) {
            response.add("extra", extra);
        }
        
        return response.toJSON();
    }
    
    public List<Map<String, Object>> toList() {
        final List<Map<String, Object>> items = new ArrayList<>();
        
        for (final KRow kRow : collection) {
            items.add(kRow.toMap(this.exclude));
        }
        
        return items;
    }
    
    public ResponseEntity buildResponse() {
        return buildResponse("items");
    }
    
    public ResponseEntity buildResponse(final String nameItems) {
        return ResponseEntity.ok(this.toResponse(nameItems));
    }
    
    public int size() {
        return this.collection.size();
    }
    
    public boolean isEmpty() {
        return this.collection.isEmpty();
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
    
    public void remove(final int i) {
        this.collection.remove(i);
    }
    
    public void removeColumn(final String column) throws KException {
        if (!this.isPresent(column)) {
            throw KExceptionHelper.internalServerError("The column in KCollection [" + column + "] to be removed does not exist. Ref? [" + ref + "]");
        }
        
        for (final KRow kRow : this.getCollection()) {
            kRow.removeColumn(column);
        }
        
        final Integer deleted = this.ref.get(column);
        
        this.ref.remove(column);
        
        for (final Map.Entry<String, Integer> entry : this.ref.entrySet()) {
            final Integer currentValue = entry.getValue();
            
            if (currentValue > deleted) {
                this.ref.put(entry.getKey(), currentValue - 1);
            }
        }
    }
    
    public Boolean isPresent(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return false;
        }
        
        return true;
    }
    
    public KRow get(final int i) {
        return this.collection.get(i);
    }
    
    public KCollection transform(final String property, final KRowFunction kRowFunction) throws KException {
        for (final KRow kRow : collection) {
            kRow.set(property, kRowFunction.run(kRow));
        }
        
        return this;
    }
    
    public KCollection filter(final KRowFunction kRowFunction) throws KException {
        final KCollection kCollectionCloned = new KCollection(new ArrayList<>(), new HashMap(this.ref), new HashMap(this.extra), table, new ArrayList<>(this.exclude));
        
        for (final KRow kRow : this.collection) {
            try {
                final boolean added = (boolean) kRowFunction.run(kRow);
                
                if (added) {
                    kCollectionCloned.getCollection().add(kRow.clone());
                }
            } catch (Exception e) {
                //Nothing to do!
            }
        }
        
        return kCollectionCloned;
    }
    
    public KCollection addProperty(final String property, final KRowFunction kRowFunction) throws KException {
        for (final KRow kRow : collection) {
            kRow.addColumn(property, kRowFunction.run(kRow));
        }
        
        this.ref.put(property, this.ref.entrySet().size());
        
        return this;
    }
    
    public KCollection addProperty(final String property, final Object value) throws KException {
        for (final KRow kRow : collection) {
            kRow.addColumn(property, value);
        }
        
        this.ref.put(property, this.ref.entrySet().size());
        
        return this;
    }
    
    public <T, V> Map<T, V> twoColumnsToMap(final String key, final String value, final Class<T> clazzT, final Class<V> clazzV) throws KException {
        final Map<T, V> map = new HashMap<>();
        
        for (final KRow kRow : collection) {
            map.put(this.getByClass(kRow, key, clazzT), this.getByClass(kRow, value, clazzV));
        }
        
        return map;
    }
    
    private <T> T getByClass(final KRow kRow, final String column, final Class<T> clazz) throws KException {
        if (clazz == Object.class) {
            return (T) kRow.get(column);
        }
        
        if (clazz == String.class) {
            return (T) kRow.getString(column);
        }
        
        if (clazz == Character.class) {
            return (T) kRow.getCharacter(column);
        }
        
        if (clazz == UUID.class) {
            return (T) kRow.getUUID(column);
        }
        
        if (clazz == BigDecimal.class) {
            return (T) kRow.getBigDecimal(column);
        }
        
        if (clazz == BigInteger.class) {
            return (T) kRow.getBigInteger(column);
        }
        
        if (clazz == Long.class) {
            return (T) kRow.getLong(column);
        }
        
        if (clazz == Integer.class) {
            return (T) kRow.getInteger(column);
        }
        
        if (clazz == Boolean.class) {
            return (T) kRow.getBoolean(column);
        }
        
        if (clazz == Double.class) {
            return (T) kRow.getDouble(column);
        }
        
        if (clazz == LocalDateTime.class) {
            return (T) kRow.getLocalDateTime(column);
        }
        
        if (clazz == LocalDate.class) {
            return (T) kRow.getLocalDate(column);
        }
        
        if (clazz == Date.class) {
            return (T) kRow.getDate(column);
        }
        
        if (clazz == Timestamp.class) {
            return (T) kRow.getTimestamp(column);
        }
        
        throw KExceptionHelper.internalServerError("Class not supported.");
    }
    
    public <T> Map<T, KCollection> groupKCollectionByColumn(final String column, final boolean removeColumn, final Class<T> clazzT) throws KException {
        final Map<T, KCollection> map = new HashMap<>();
        
        for (final KRow kRow : this.collection) {
            if (kRow.isNull(column)) {
                continue;
            }
            
            final T key = this.getByClass(kRow, column, clazzT);
            
            if (map.containsKey(key)) {
                final KCollection kCollection = map.get(key);
                
                kCollection.getCollection().add((removeColumn) ? kRow.clone().removeColumn(column) : kRow.clone());

                map.put(key, kCollection);
            } else {
                map.put(key, new KCollection(new ArrayList() {{
                    add((removeColumn) ? kRow.clone().removeColumn(column) : kRow.clone());
                }}, new HashMap(this.ref), new HashMap(this.extra), table, new ArrayList<>(this.exclude)));
            }
        }
        
        return map;
    }
    
    public Map<Object, KCollection> groupKCollectionByObjectColumn(final String column) throws KException {
        return this.groupKCollectionByObjectColumn(column, false);
    }
    
    public Map<Object, KCollection> groupKCollectionByObjectColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, Object.class);
    }
    
    public Map<String, KCollection> groupKCollectionByStringColumn(final String column) throws KException {
        return this.groupKCollectionByStringColumn(column, false);
    }
    
    public Map<String, KCollection> groupKCollectionByStringColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, String.class);
    }
    
    public Map<Character, KCollection> groupKCollectionByCharacterColumn(final String column) throws KException {
        return this.groupKCollectionByCharacterColumn(column, false);
    }
    
    public Map<Character, KCollection> groupKCollectionByCharacterColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, Character.class);
    }
    
    public Map<UUID, KCollection> groupKCollectionByUUIDColumn(final String column) throws KException {
        return this.groupKCollectionByUUIDColumn(column, false);
    }
    
    public Map<UUID, KCollection> groupKCollectionByUUIDColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, UUID.class);
    }
    
    public Map<BigDecimal, KCollection> groupKCollectionByBigDecimalColumn(final String column) throws KException {
        return this.groupKCollectionByBigDecimalColumn(column, false);
    }
    
    public Map<BigDecimal, KCollection> groupKCollectionByBigDecimalColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, BigDecimal.class);
    }
    
    public Map<BigInteger, KCollection> groupKCollectionByBigIntegerColumn(final String column) throws KException {
        return this.groupKCollectionByBigIntegerColumn(column, false);
    }
    
    public Map<BigInteger, KCollection> groupKCollectionByBigIntegerColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, BigInteger.class);
    }
    
    public Map<Long, KCollection> groupKCollectionByLongColumn(final String column) throws KException {
        return this.groupKCollectionByLongColumn(column, false);
    }
    
    public Map<Long, KCollection> groupKCollectionByLongColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, Long.class);
    }
    
    public Map<Integer, KCollection> groupKCollectionByIntegerColumn(final String column) throws KException {
        return this.groupKCollectionByIntegerColumn(column, false);
    }
    
    public Map<Integer, KCollection> groupKCollectionByIntegerColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, Integer.class);
    }
    
    public Map<Boolean, KCollection> groupKCollectionByBooleanColumn(final String column) throws KException {
        return this.groupKCollectionByBooleanColumn(column, false);
    }
    
    public Map<Boolean, KCollection> groupKCollectionByBooleanColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, Boolean.class);
    }
    
    public Map<Double, KCollection> groupKCollectionByDoubleColumn(final String column) throws KException {
        return this.groupKCollectionByDoubleColumn(column, false);
    }
    
    public Map<Double, KCollection> groupKCollectionByDoubleColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, Double.class);
    }
    
    public Map<LocalDateTime, KCollection> groupKCollectionByLocalDateTimeColumn(final String column) throws KException {
        return this.groupKCollectionByLocalDateTimeColumn(column, false);
    }
    
    public Map<LocalDateTime, KCollection> groupKCollectionByLocalDateTimeColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, LocalDateTime.class);
    }
    
    public Map<LocalDate, KCollection> groupKCollectionByLocalDateColumn(final String column) throws KException {
        return this.groupKCollectionByLocalDateColumn(column, false);
    }
    
    public Map<LocalDate, KCollection> groupKCollectionByLocalDateColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, LocalDate.class);
    }
    
    public Map<Date, KCollection> groupKCollectionByDateColumn(final String column) throws KException {
        return this.groupKCollectionByDateColumn(column, false);
    }
    
    public Map<Date, KCollection> groupKCollectionByDateColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, Date.class);
    }
    
    public Map<Timestamp, KCollection> groupKCollectionByTimestampColumn(final String column) throws KException {
        return this.groupKCollectionByTimestampColumn(column, false);
    }
    
    public Map<Timestamp, KCollection> groupKCollectionByTimestampColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupKCollectionByColumn(column, removeColumn, Timestamp.class);
    }
    
    private <T> Map<T, List<Map<String, Object>>> groupListByColumn(final String column, final boolean removeColumn, final Class<T> clazzT) throws KException {
        final Map<T, List<Map<String, Object>>> map = new HashMap<>();
        
        for (final KRow kRow : this.collection) {
            if (kRow.isNull(column)) {
                continue;
            }
            
            final T key = this.getByClass(kRow, column, clazzT);
            
            if (map.containsKey(key)) {
                final List<Map<String, Object>> list = map.get(key);
                
                list.add((removeColumn) ? kRow.clone().removeColumn(column).toMap() : kRow.toMap());
                
                map.put(key, list);
            } else {
                map.put(key, new ArrayList<Map<String, Object>>() {{
                    add((removeColumn) ? kRow.clone().removeColumn(column).toMap() : kRow.toMap());
                }});
            }
        }
        
        return map;
    }
    
    public Map<Object, List<Map<String, Object>>> groupListByObjectColumn(final String column) throws KException {
        return this.groupListByObjectColumn(column, false);
    }
    
    public Map<Object, List<Map<String, Object>>> groupListByObjectColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, Object.class);
    }
    
    public Map<String, List<Map<String, Object>>> groupListByStringColumn(final String column) throws KException {
        return this.groupListByStringColumn(column, false);
    }
    
    public Map<String, List<Map<String, Object>>> groupListByStringColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, String.class);
    }
    
    public Map<Character, List<Map<String, Object>>> groupListByCharacterColumn(final String column) throws KException {
        return this.groupListByCharacterColumn(column, false);
    }
    
    public Map<Character, List<Map<String, Object>>> groupListByCharacterColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, Character.class);
    }
    
    public Map<UUID, List<Map<String, Object>>> groupListByUUIDColumn(final String column) throws KException {
        return this.groupListByUUIDColumn(column, false);
    }
    
    public Map<UUID, List<Map<String, Object>>> groupListByUUIDColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, UUID.class);
    }
    
    public Map<BigDecimal, List<Map<String, Object>>> groupListByBigDecimalColumn(final String column) throws KException {
        return this.groupListByBigDecimalColumn(column, false);
    }
    
    public Map<BigDecimal, List<Map<String, Object>>> groupListByBigDecimalColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, BigDecimal.class);
    }
    
    public Map<BigInteger, List<Map<String, Object>>> groupListByBigIntegerColumn(final String column) throws KException {
        return this.groupListByBigIntegerColumn(column, false);
    }
    
    public Map<BigInteger, List<Map<String, Object>>> groupListByBigIntegerColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, BigInteger.class);
    }
    
    public Map<Long, List<Map<String, Object>>> groupListByLongColumn(final String column) throws KException {
        return this.groupListByLongColumn(column, false);
    }
    
    public Map<Long, List<Map<String, Object>>> groupListByLongColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, Long.class);
    }
    
    public Map<Integer, List<Map<String, Object>>> groupListByIntegerColumn(final String column) throws KException {
        return this.groupListByIntegerColumn(column, false);
    }
    
    public Map<Integer, List<Map<String, Object>>> groupListByIntegerColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, Integer.class);
    }
    
    public Map<Boolean, List<Map<String, Object>>> groupListByBooleanColumn(final String column) throws KException {
        return this.groupListByBooleanColumn(column, false);
    }
    
    public Map<Boolean, List<Map<String, Object>>> groupListByBooleanColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, Boolean.class);
    }
    
    public Map<Double, List<Map<String, Object>>> groupListByDoubleColumn(final String column) throws KException {
        return this.groupListByDoubleColumn(column, false);
    }
    
    public Map<Double, List<Map<String, Object>>> groupListByDoubleColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, Double.class);
    }
    
    public Map<LocalDateTime, List<Map<String, Object>>> groupListByLocalDateTimeColumn(final String column) throws KException {
        return this.groupListByLocalDateTimeColumn(column, false);
    }
    
    public Map<LocalDateTime, List<Map<String, Object>>> groupListByLocalDateTimeColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, LocalDateTime.class);
    }
    
    public Map<LocalDate, List<Map<String, Object>>> groupListByLocalDateColumn(final String column) throws KException {
        return this.groupListByLocalDateColumn(column, false);
    }
    
    public Map<LocalDate, List<Map<String, Object>>> groupListByLocalDateColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, LocalDate.class);
    }
    
    public Map<Date, List<Map<String, Object>>> groupListByDateColumn(final String column) throws KException {
        return this.groupListByDateColumn(column, false);
    }
    
    public Map<Date, List<Map<String, Object>>> groupListByDateColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, Date.class);
    }
    
    public Map<Timestamp, List<Map<String, Object>>> groupListByTimestampColumn(final String column) throws KException {
        return this.groupListByTimestampColumn(column, false);
    }
    
    public Map<Timestamp, List<Map<String, Object>>> groupListByTimestampColumn(final String column, final boolean removeColumn) throws KException {
        return this.groupListByColumn(column, removeColumn, Timestamp.class);
    }
    
    public List<Object[]> values() {
        final List<Object[]> items = new ArrayList<>();
        
        for (final KRow kRow : collection) {
            items.add(kRow.values());
        }
        
        return items;
    }
    
    public Map<String, Object> getExtra() {
        return this.extra;
    }
    
    public Object getExtra(final String key) {
        return this.extra.get(key);
    }
    
    public Object getLongExtra(final String key) {
        final Object v = this.extra.get(key);
        
        if (v == null) {
            return null;
        }
        
        if (v instanceof BigInteger) {
            return ((BigInteger) v).longValue();
        }
        
        return (Long) v;
    }
    
    public void logs() {
        System.out.println("KCollection Ref?: [" + ref + "]");
        
        for (final KRow kRow : this.getCollection()) {
            kRow.logs();
        }
    }
}