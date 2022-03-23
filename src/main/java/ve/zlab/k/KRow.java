package ve.zlab.k;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import ve.zlab.k.functions.KRowFunction;
import ve.zlab.k.helper.KExceptionHelper;

public class KRow {
    
    private Object[] o;
    final private Map<String, Integer> ref;
    private boolean isNull;
    private String table;
    private List<String> exclude;
    
    public boolean isNull() {
        return isNull;
    }
    
    public KRow() {
        super();
        
        this.o = null;
        this.ref = null;
        this.isNull = true;
        this.table = null;
        this.exclude = new ArrayList<>();
    }

    public KRow(final Object[] o, final String table) {
        super();
        
        this.o = o;
        this.ref = new HashMap<>();
        this.isNull = false;
        this.table = table;
        this.exclude = new ArrayList<>();
    }
    
    public KRow(final Object[] o, final Map<String, Integer> ref, final String table) {
        super();
        
        this.o = o;
        this.ref = (ref == null) ? null : new HashMap<>(ref);
        this.table = table;
        this.exclude = new ArrayList<>();
    }
    
    public KRow(final Object[] o, final Map<String, Integer> ref, final boolean isNull, final String table) {
        this(o ,ref, table);
        
        this.isNull = isNull;
        this.exclude = new ArrayList<>();
    }
    
    public KRow exclude(final String exclude) {
        if (exclude == null) {
            return null;
        }

        this.exclude.add(exclude);

        return this;
    }
    
    public KRow exclude(final String... excludes) {
        if (excludes == null) {
            return null;
        }

        for (final String exclude : excludes) {
            if (exclude == null) {
                continue;
            }
            
            this.exclude.add(exclude);
        }

        return this;
    }
    
    public Object get(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.get(n);
    }
    
    public Object get(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        return o[n];
    }
    
    public String getString(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getString(n);
    }
    
    public String getString(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        return (String) o[n];
    }
    
    public Character getCharacter(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getCharacter(n);
    }
    
    public Character getCharacter(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        return (Character) o[n];
    }
    
    public UUID getUUID(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getUUID(n);
    }
    
    public UUID getUUID(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        return (UUID) o[n];
    }
    
    public BigDecimal getBigDecimal(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getBigDecimal(n);
    }
    
    public BigDecimal getBigDecimal(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        return (BigDecimal) o[n];
    }
    
    public BigInteger getBigInteger(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getBigInteger(n);
    }
    
    public BigInteger getBigInteger(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        return (BigInteger) o[n];
    }
    
    public Long getLong(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getLong(n);
    }
    
    public Long getLong(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        if (o[n] instanceof BigInteger) {
            return ((BigInteger) o[n]).longValue();
        }
        
        return (Long) o[n];
    }
    
    public Integer getInteger(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getInteger(n);
    }
    
    public Integer getInteger(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        if (o[n] instanceof Short) {
            return ((Short) o[n]).intValue();
        }
        
        if (o[n] instanceof BigInteger) {
            return ((BigInteger) o[n]).intValue();
        }
        
        return (Integer) o[n];
    }
    
    public Boolean getBoolean(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getBoolean(n);
    }
    
    public Boolean getBoolean(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        return (Boolean) o[n];
    }
    
    public Double getDouble(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getDouble(n);
    }
    
    public Double getDouble(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        return (Double) o[n];
    }
    
    public byte[] getBytea(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getBytea(n);
    }
    
    public byte[] getBytea(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        return (byte[]) o[n];
    }
    
    public Boolean isPresent(final String c) {
        if (o == null) {
            return false;
        }
        
        final Integer n = ref.get(c);
        
        if (n == null) {
            return false;
        }
        
        return true;
    }
    
    public boolean isNull(final String c) {
        if (o == null) {
            return true;
        }
        
        final Integer n = ref.get(c);
        
        if (n == null) {
            return true;
        }
        
        return o[n] == null;
    }
    
    public boolean isNull(final int n) {
        if (o == null) {
            return true;
        }
        
        if (n >= o.length) {
            return true;
        }
        
        return o[n] == null;
    }
    
    public Boolean isNullOrEmpty(final String c) {
        if (o == null) {
            return true;
        }
        
        final Integer n = ref.get(c);
        
        if (n == null) {
            return true;
        }
        
        final String v = (String) o[n];
        
        if (v == null) {
            return true;
        }
        
        if (v.trim().isEmpty()) {
            return true;
        }
        
        return false;
    }
    
    public Boolean isNullOrEmpty(final int n) {
        if (o == null) {
            return true;
        }
        
        if (n >= o.length) {
            return true;
        }
        
        final String v = (String) o[n];
        
        if (v == null) {
            return true;
        }
        
        if (v.trim().isEmpty()) {
            return true;
        }
        
        return false;
    }
    
    public LocalDateTime getLocalDateTime(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getLocalDateTime(n);
    }
    
    public LocalDateTime getLocalDateTime(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        final Object value = o[n];
        
        if (value instanceof Timestamp) {
            return ((Timestamp) o[n]).toLocalDateTime();
        }
        
        return (LocalDateTime) o[n];
    }
    
    public LocalDate getLocalDate(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getLocalDate(n);
    }
    
    public LocalDate getLocalDate(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        final Object value = o[n];
        
        if (value instanceof Date) {
            return ((Date) o[n]).toLocalDate();
        }
        
        return (LocalDate) o[n];
    }
    
    public Date getDate(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getDate(n);
    }
    
    public Date getDate(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        return (Date) o[n];
    }

    public Timestamp getTimestamp(final String c) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getTimestamp(n);
    }
    
    public Timestamp getTimestamp(final int n) {
        if (n >= o.length) {
            return null;
        }
        
        return (Timestamp) o[n];
    }
    
    public KRow assertNotNull(final String c, final HttpStatus status, final String message) throws KException {
        if (this.isNull) {
            throw new KException(status, message);
        }
        
        final Integer n = ref.get(c);
        
        if (n == null) {
            throw new KException(status, message);
        }

        if (n >= o.length) {
            throw new KException(status, message);
        }
        
        if (o[n] == null) {
            throw new KException(status, message);
        }
        
        return this;
    }
    
    public KRow assertNotNull(final int n, final HttpStatus status, final String message) throws KException {
        if (this.isNull) {
            throw new KException(status, message);
        }
        
        if (n >= o.length) {
            throw new KException(status, message);
        }
        
        if (o[n] == null) {
            throw new KException(status, message);
        }
        
        return this;
    }
    
    public KRow assertNotNull(final String c, final HttpStatus status) throws KException {
        return assertNotNull(c, status, null);
    }
    
    public KRow assertNotNull(final int n, final HttpStatus status) throws KException {
        return assertNotNull(n, status, null);
    }
    
    public Object[] values() {
       return o; 
    }
    
    public Object[] getO() {
        return o;
    }

    public void setO(Object[] o) {
        this.o = o;
    }
    
    public void set(final String property, Object o) {
        this.o[ref.get(property)] = o;
    }
    
    public void set(final String property, KRowFunction kRowFunction) throws KException {
        if (kRowFunction == null) {
            this.o[ref.get(property)] = null;
            
            return;
        }
        
        this.o[ref.get(property)] = kRowFunction.run(this);
    }
    
    public Map<String, Object> toMap() {
        return toMap(new ArrayList<>());
    }
    
    public Map<String, Object> toMap(final String property) {
        if (property == null) {
            return toMap();
        }
        
        return toMap(new ArrayList() {{ add(property); }});
    }
    
    public Map<String, Object> toMap(final List<String> exclude) {
        if (this.isNull) {
            return null;
        }
        
        final List<String> listExclude = new ArrayList<>(this.exclude);
        
        if (exclude != null && !exclude.isEmpty()) {
            listExclude.addAll(exclude);
        }
        
        final Map<String, Object> map = new HashMap<>();
        
        for (final Map.Entry<String, Integer> entry : this.ref.entrySet()) {
            final String key = entry.getKey();
            
            if (listExclude.contains(key)) {
                continue;
            }
            
            if (o[entry.getValue()] == null) {
                continue;
            }
            
            final Object value = o[entry.getValue()];

            if (value instanceof List) {
                final List list = (List) value;

                if (list.isEmpty()) {
                    map.put(key, value);
                    
                    continue;
                }
                
                if (list.get(0) instanceof KRow) {
                    final List<Map<String, Object>> kRows = new ArrayList<>();

                    for (Object o : list) {
                        kRows.add(((KRow) o).toMap());
                    }

                    map.put(key, kRows);
                } else {
                    map.put(key, value);
                }
                
                continue;
            }
                
            map.put(key, value);
        }
        
        return map;
    }
    
    private String toResponse() {
        if (this.isNull) {
            return null;
        }
        
        final Map<String, Object> map = new HashMap<>();
        
        for (final Map.Entry<String, Integer> entry : this.ref.entrySet()) {
            if (this.exclude.contains(entry.getKey())) {
                continue;
            }
            
            if (o[entry.getValue()] != null) {
                map.put(entry.getKey(), o[entry.getValue()]);   
            }
        }
        
        return new JSONObject(map).toString();
    }
    
    public KRow assertNotNull(final HttpStatus status, final String message) throws KException {
        if (this.isNull) {
            throw new KException(status, message);
        }
        
        return this;
    }
    
    public KRow assertNotNull(final HttpStatus status) throws KException {
        return assertNotNull(status, null);
    }
    
    public ResponseEntity buildResponse() {
        return ResponseEntity.ok(this.toResponse());
    }
    
    public void addColumn(final String name, Object value) {
        this.o = Arrays.copyOf(o, o.length + 1);
        this.o[o.length - 1] = value;
        
        this.ref.put(name, o.length - 1);
    }
    
    public KRow removeColumn(final String name) throws KException {
        if (!this.isPresent(name)) {
            throw KExceptionHelper.internalServerError("The column in KRow [" + name + "] to be removed does not exist. Ref? [" + ref + "]");
        }
        
        final Object[] previous = this.o.clone();
        this.o = new Object[this.o.length - 1];
        
        final Integer deleted = this.ref.get(name);
        int current = 0;
        
        for (int i = 0; i < previous.length; i++) {
            if (i != deleted) {
                this.o[current++] = previous[i];
            }
        }
        
        this.ref.remove(name);
        
        for (final Map.Entry<String, Integer> entry : this.ref.entrySet()) {
            final Integer currentValue = entry.getValue();
            
            if (currentValue > deleted) {
                this.ref.put(entry.getKey(), currentValue - 1);
            }
        }
        
        return this;
    }
    
    @Override
    public KRow clone() {
        return new KRow((this.o != null) ? this.o.clone() : null, (this.ref != null ? new HashMap(this.ref) : null), this.isNull, this.table);
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
    
    public KRow transform(final String property, final KRowFunction kRowFunction) throws KException {
        this.set(property, kRowFunction.run(this));
        
        return this;
    }
    
    public KRow addProperty(final String property, final KRowFunction kRowFunction) throws KException {
        this.addColumn(property, kRowFunction.run(this));
        
        return this;
    }

    public Map<String, Integer> getRef() {
        return ref;
    }

    public List<String> getExclude() {
        return exclude;
    }
    
    public void logs() {
        System.out.println("Is null?: [" + isNull + "]");
        
        if (!this.isNull) {
            System.out.println("Ref?: [" + ref + "]");
            System.out.println("o?");   
            
            for (final Object object : this.o) {
                if (object == null) {
                    System.out.println("NULL");
                } else {
                    System.out.println(object);   
                }
            }
        }
    }
}
