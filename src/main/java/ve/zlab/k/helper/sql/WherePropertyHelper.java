package ve.zlab.k.helper.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import ve.zlab.core.helper.security.validator.model.WhereProperty;
import ve.zlab.k.constant.Status;

public class WherePropertyHelper {
    
    public static List<WhereProperty> group(final WhereProperty... whereProperties) {
        if (whereProperties == null) {
            return new ArrayList<>();
        }
        
        return Arrays.asList(whereProperties);
    }
    
    public static WhereProperty notDeleted() {
        return WherePropertyHelper.whereEqual("deleted", false);
    }
    
    public static WhereProperty.WhereEqual active() {
        return WherePropertyHelper.whereEqual("status_id", Status.ACTIVE);
    }
    
    public static WhereProperty.WhereEqual whereEqual(final String property, final Object value) {
        return new WhereProperty.WhereEqual(property, value);
    }
    
    public static WhereProperty.WhereNull whereNull(final String property) {
        return new WhereProperty.WhereNull(property);
    }
}
