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
    
    private String cleanC(final String c) {
        if (c == null) {
            return "";
        }
        
        if (!c.contains(".")) {
            return c;
        }
        
        return c.substring(c.indexOf(".") + 1);
    }
    
    public Object get(final String c) {
        final String cleanC = this.cleanC(c);
                
        final Integer n = ref.get(cleanC);
        
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
        final String cleanC = this.cleanC(c);
        
        final Integer n = ref.get(cleanC);
        
        if (n == null) {
            return null;
        }
        
        return this.getString(n, cleanC);
    }
    
    public String getString(final int n) {
        return this.getString(n, null);
    }
    
    private String getString(final int n, final String nameRef) {
        if (n >= o.length) {
            return null;
        }
        
        try {
            return (String) o[n];
        } catch (Exception e) {
            if (nameRef != null) {
                throw KExceptionHelper.internalServerError("'" + nameRef + "' value is not casteable to \"String\"");
            }
            
            throw KExceptionHelper.internalServerError("Value at position '" + n + "' is not casteable to \"String\"");
        }
    }
    
    public Character getCharacter(final String c) {
        final String cleanC = this.cleanC(c);
        
        final Integer n = ref.get(cleanC);
        
        if (n == null) {
            return null;
        }
        
        return this.getCharacter(n, cleanC);
    }
    
    public Character getCharacter(final int n) {
        return this.getCharacter(n, null);
    }
    
    private Character getCharacter(final int n, final String nameRef) {
        if (n >= o.length) {
            return null;
        }
        
        try {
            return (Character) o[n];
        } catch (Exception e) {
            if (nameRef != null) {
                throw KExceptionHelper.internalServerError("'" + nameRef + "' value is not casteable to \"Character\"");
            }
            
            throw KExceptionHelper.internalServerError("Value at position '" + n + "' is not casteable to \"Character\"");
        }
    }
    
    public UUID getUUID(final String c) {
        final String cleanC = this.cleanC(c);
        
        final Integer n = ref.get(cleanC);
        
        if (n == null) {
            return null;
        }
        
        return this.getUUID(n, cleanC);
    }
    
    public UUID getUUID(final int n) {
        return this.getUUID(n, null);
    }
    
    private UUID getUUID(final int n, final String nameRef) {
        if (n >= o.length) {
            return null;
        }
        
        if (o[n] == null) {
            return null;
        }
        
        if (o[n] instanceof String) {
            return UUID.fromString((String) o[n]);
        }
        
        try {
            return (UUID) o[n];
        } catch (Exception e) {
            if (nameRef != null) {
                throw KExceptionHelper.internalServerError("'" + nameRef + "' value is not casteable to \"UUID\"");
            }
            
            throw KExceptionHelper.internalServerError("Value at position '" + n + "' is not casteable to \"UUID\"");
        }
    }
    
    public BigDecimal getBigDecimal(final String c) {
        final String cleanC = this.cleanC(c);
        
        final Integer n = ref.get(cleanC);
        
        if (n == null) {
            return null;
        }
        
        return this.getBigDecimal(n, cleanC);
    }
    
    public BigDecimal getBigDecimal(final int n) {
        return this.getBigDecimal(n, null);
    }
    
    private BigDecimal getBigDecimal(final int n, final String nameRef) {
        if (n >= o.length) {
            return null;
        }
        
        try {
            return (BigDecimal) o[n];
        } catch (Exception e) {
            if (nameRef != null) {
                throw KExceptionHelper.internalServerError("'" + nameRef + "' value is not casteable to \"BigDecimal\"");
            }
            
            throw KExceptionHelper.internalServerError("Value at position '" + n + "' is not casteable to \"BigDecimal\"");
        }
    }
    
    public BigInteger getBigInteger(final String c) {
        final String cleanC = this.cleanC(c);
        
        final Integer n = ref.get(cleanC);
        
        if (n == null) {
            return null;
        }
        
        return this.getBigInteger(n, cleanC);
    }
    
    public BigInteger getBigInteger(final int n) {
        return this.getBigInteger(n, null);
    }
    
    private BigInteger getBigInteger(final int n, final String nameRef) {
        if (n >= o.length) {
            return null;
        }
        
        try {
            return (BigInteger) o[n];
        } catch (Exception e) {
            if (nameRef != null) {
                throw KExceptionHelper.internalServerError("'" + nameRef + "' value is not casteable to \"BigInteger\"");
            }
            
            throw KExceptionHelper.internalServerError("Value at position '" + n + "' is not casteable to \"BigInteger\"");
        }
    }
    
    public Long getLong(final String c) {
        final String cleanC = this.cleanC(c);
        
        final Integer n = ref.get(cleanC);
        
        if (n == null) {
            return null;
        }
        
        return this.getLong(n, cleanC);
    }
    
    public Long getLong(final int n) {
        return this.getLong(n, null);
    }
    
    private Long getLong(final int n, final String nameRef) {
        if (n >= o.length) {
            return null;
        }
        
        if (o[n] == null) {
            return null;
        }
        
        if (o[n] instanceof BigInteger) {
            return ((BigInteger) o[n]).longValue();
        }
        
        try {
            return (Long) o[n];
        } catch (Exception e) {
            if (nameRef != null) {
                throw KExceptionHelper.internalServerError("'" + nameRef + "' value is not casteable to \"Long\"");
            }
            
            throw KExceptionHelper.internalServerError("Value at position '" + n + "' is not casteable to \"Long\"");
        }
    }
    
    public Integer getInteger(final String c) {
        final String cleanC = this.cleanC(c);
        
        final Integer n = ref.get(cleanC);
        
        if (n == null) {
            return null;
        }
        
        return this.getInteger(n, cleanC);
    }
    
    public Integer getInteger(final int n) {
        return this.getInteger(n, null);
    }
    
    private Integer getInteger(final int n, final String nameRef) {
        if (n >= o.length) {
            return null;
        }
        
        if (o[n] == null) {
            return null;
        }
        
        if (o[n] instanceof Short) {
            return ((Short) o[n]).intValue();
        }
        
        if (o[n] instanceof BigInteger) {
            return ((BigInteger) o[n]).intValue();
        }
        
        try {
            return (Integer) o[n];
        } catch (Exception e) {
            if (nameRef != null) {
                throw KExceptionHelper.internalServerError("'" + nameRef + "' value is not casteable to \"Integer\"");
            }
            
            throw KExceptionHelper.internalServerError("Value at position '" + n + "' is not casteable to \"Integer\"");
        }
    }
    
    public Boolean getBoolean(final String c) {
        final String cleanC = this.cleanC(c);
        
        final Integer n = ref.get(cleanC);
        
        if (n == null) {
            return null;
        }
        
        return this.getBoolean(n, cleanC);
    }
    
    public Boolean getBoolean(final int n) {
        return this.getBoolean(n, null);
    }
    
    private Boolean getBoolean(final int n, final String nameRef) {
        if (n >= o.length) {
            return null;
        }
        
        try {
            return (Boolean) o[n];
        } catch (Exception e) {
            if (nameRef != null) {
                throw KExceptionHelper.internalServerError("'" + nameRef + "' value is not casteable to \"Boolean\"");
            }
            
            throw KExceptionHelper.internalServerError("Value at position '" + n + "' is not casteable to \"Boolean\"");
        }
    }
    
    public Double getDouble(final String c) {
        final String cleanC = this.cleanC(c);
        
        final Integer n = ref.get(cleanC);
        
        if (n == null) {
            return null;
        }
        
        return this.getDouble(n, cleanC);
    }
    
    public Double getDouble(final int n) {
        return this.getDouble(n, null);
    }
    
    private Double getDouble(final int n, final String nameRef) {
        if (n >= o.length) {
            return null;
        }

        try {
            return (Double) o[n];
        } catch (Exception e) {
            if (nameRef != null) {
                throw KExceptionHelper.internalServerError("'" + nameRef + "' value is not casteable to \"Double\"");
            }
            
            throw KExceptionHelper.internalServerError("Value at position '" + n + "' is not casteable to \"Double\"");
        }
    }
    
    public byte[] getBytea(final String c) {
        final String cleanC = this.cleanC(c);
        
        final Integer n = ref.get(cleanC);
        
        if (n == null) {
            return null;
        }
        
        return this.getBytea(n, cleanC);
    }
    
    public byte[] getBytea(final int n) {
        return this.getBytea(n, null);
    }
    
    private byte[] getBytea(final int n, final String nameRef) {
        if (n >= o.length) {
            return null;
        }

        try {
            return (byte[]) o[n];
        } catch (Exception e) {
            if (nameRef != null) {
                throw KExceptionHelper.internalServerError("'" + nameRef + "' value is not casteable to \"byte[]\"");
            }
            
            throw KExceptionHelper.internalServerError("Value at position '" + n + "' is not casteable to \"byte[]\"");
        }
    }
    
    public boolean isPresent(final String c) {
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
    
    public boolean isNullOrEmpty(final String c) {
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
    
    public boolean isNullOrEmpty(final int n) {
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
