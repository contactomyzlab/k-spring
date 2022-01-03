package ve.zlab.k.response;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.springframework.http.ResponseEntity;
import ve.zlab.k.KCollection;
import ve.zlab.k.KRow;
import ve.zlab.k.annotations.Column;
import ve.zlab.k.model.KModelDTO;

public class DynamicObject {
    
    private HashMap<String, Object> data;
    private Boolean addNullValues;

    public DynamicObject() {
        this.data = new HashMap<>();
        this.addNullValues = true;
    }
    
    public DynamicObject(final Boolean addNullValues) {
        this.data = new HashMap<>();
        this.addNullValues = addNullValues;
    }
    
    public static DynamicObject create() {
        return new DynamicObject();
    }
    
    public DynamicObject dontAllowNullValues() {
        this.addNullValues = false;
        
        return this;
    }
    
    public DynamicObject add(final String name, final Object value) {
        if (!addNullValues) {
            if (value == null) {
                return this;
            }
        }
        
        if (value instanceof KRow) {
            this.data.put(name, ((KRow) value).toMap());
                    
            return this;
        }
        
        if (value instanceof KCollection) {
            this.data.put(name, ((KCollection) value).toList());
                    
            return this;
        }
        
        this.data.put(name, value);
        
        return this;
    }
    
    public DynamicObject addKRowProperties(final KRow kRow) {
        if (kRow == null || kRow.isNull()) {
            return this;
        }
        
        
        for (final Map.Entry<String, Integer> entry : kRow.getRef().entrySet()) {
            if (kRow.getExclude().contains(entry.getKey())) {
                continue;
            }
            
            if (kRow.getO()[entry.getValue()] != null) {
                this.add(entry.getKey(), kRow.getO()[entry.getValue()]);
            }
        }
        
        return this;
    }
    
    public DynamicObject addKModelProperties(final KModelDTO kModel) {
        if (kModel == null) {
            return this;
        }
        
        final Class myClass = kModel.getEntityClass();

        final Field[] fields = myClass.getDeclaredFields();

        for (final Field field : fields) {
            final Column column = field.getAnnotation(Column.class);

            if (column == null) {
                continue;
            }
            
            final String name = column.name();
            
            if (!kModel.getFieldsToUpdate().contains(name)) {
                continue;
            }

            try {
                field.setAccessible(true);

                final Object o = field.get(kModel);

                if (!addNullValues) {
                    if (o == null) {
                        continue;
                    }
                }

                this.add(name, o);
            } catch (Exception e) {
                //NOTHING
            }
        }
        
        return this;
    }
    
    public DynamicObject addProperties(final Map<String, ?> map) {
        if (map == null) {
            return this;
        }
        
        
        for (final Map.Entry<String, ?> entry : map.entrySet()) {            
            if (entry.getValue() != null) {
                this.add(entry.getKey(), entry.getValue());
            }
        }
        
        return this;
    }
    
    public ResponseEntity toResponse() {
        return ResponseEntity.ok(this.toJSON());
    }
    
    public String toJSON() {
        final HashMap<String, Object> mapToJson = new HashMap<>();
        
        for (final Map.Entry<String,Object> entry : data.entrySet()) {
            if (entry.getValue() == null) {
                mapToJson.put(entry.getKey(), JSONObject.NULL);
            } else {
                mapToJson.put(entry.getKey(), entry.getValue());
            }
        } 
        
        return new JSONObject(mapToJson).toString();
    }

    public HashMap<String, Object> toMap() {
        return data;
    }
}