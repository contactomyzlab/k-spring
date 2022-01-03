package ve.zlab.k.model;

import org.apache.commons.lang3.StringUtils;
import ve.zlab.k.annotations.KClass;

public abstract class KModel {
    
    public static String alias(final String column, final String alias) {
        return StringUtils.join(new String[]{
            column, "AS", alias
        }, " ");
    }
    
    public static String c(final Class<? extends KModelDTO> clazz, final String column) {
        try {
            return StringUtils.join(new String[]{
                clazz.newInstance().getDTOAlias(), column
            }, ".");
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    public static JoinData belongsTo(final Class<? extends KModel> clazz, final String through) {
        try {
            final KModel kModelDLL = clazz.newInstance();
            
            return new JoinData(kModelDLL.getEntityClass(), through, kModelDLL.getNameColumnId());
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    public static String tableWithAlias(final Class<? extends KModel> clazz) {
        try {
            final KModel kModel = clazz.newInstance();
            
            final KModelDTO kModelDTO = kModel.getEntityClass().newInstance();
            
            return StringUtils.join(new String[]{
                kModelDTO.getDTOTable(), kModelDTO.getDTOAlias()
            }, " ");
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    /* */
    public Class<? extends KModelDTO> getEntityClass() {
        return ((KClass) this.getClass().getAnnotation(KClass.class)).entityClass();
    }
    
    /* Abstract Methods */
    public abstract String getNameColumnId();
}
