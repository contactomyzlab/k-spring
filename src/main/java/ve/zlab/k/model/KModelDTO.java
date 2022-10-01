package ve.zlab.k.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import ve.zlab.k.KExecutor;
import ve.zlab.k.KQuery;
import ve.zlab.k.annotations.KClass;
import ve.zlab.k.response.DynamicObject;
import ve.zlab.k.annotations.KTable;

public abstract class KModelDTO {

    private final List<String> fieldsToUpdate;
    
    public KModelDTO() {
        super();
        
        this.fieldsToUpdate = new ArrayList<>();
    }
    
    public Map<String, Object> toMap() {
        return DynamicObject.create().addKModelProperties(this).toMap();
    }
    
    public KQuery toKQuery(final KExecutor K) {
        return
            K.
            table(this);
    }
    
    /* Field To Update Funcionality */
    public void addFieldToUpdate(final String field) {
        this.fieldsToUpdate.add(field);
    }

    public List<String> getFieldsToUpdate() {
        return fieldsToUpdate;
    }
    
    /* */
    public Class<? extends KModelDTO> getEntityClass() {
        return ((KClass) this.getClass().getAnnotation(KClass.class)).entityClass();
    }
    
    public Class getColumnIdClass() {
        return ((KClass) this.getClass().getAnnotation(KClass.class)).columnIdClass();
    }
    
    public String getDTOTable() {
        return ((KTable) this.getClass().getAnnotation(KTable.class)).name();
    }
    
    public String getDTOAlias() {
        return ((KTable) this.getClass().getAnnotation(KTable.class)).alias();
    }
    
    public String getTableWithAlias(final Class<? extends KModelDTO> clazz) {
        try {
            final KModelDTO kModelDTO = clazz.newInstance();
            
            return StringUtils.join(new String[]{
                kModelDTO.getDTOTable(), kModelDTO.getDTOAlias()
            }, " ");
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    public String getTableWithAlias() {
        return StringUtils.join(new String[]{
            this.getDTOTable(), this.getDTOAlias()
        }, " ");
    }
    
    /* Static Methods */
    public static String table(final Class<? extends KModelDTO> clazz) {
        try {
            return clazz.newInstance().getDTOTable();
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    public static String alias(final Class<? extends KModelDTO> clazz) {
        try {
            return clazz.newInstance().getDTOAlias();
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    public static String tableWithAlias(final Class<? extends KModelDTO> clazz) {
        try {
            final KModelDTO kModelDTO = clazz.newInstance();
            
            return StringUtils.join(new String[]{
                kModelDTO.getDTOTable(), kModelDTO.getDTOAlias()
            }, " ");
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    /* Abstract Methods */
    public abstract String getNameColumnId();
}
