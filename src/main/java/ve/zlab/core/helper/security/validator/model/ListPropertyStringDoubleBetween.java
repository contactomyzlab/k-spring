package ve.zlab.core.helper.security.validator.model;

import java.lang.reflect.Method;
import org.apache.commons.lang3.StringUtils;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class ListPropertyStringDoubleBetween extends KRule {
    
    private String property;
    private Double from;
    private Double to;
    
    public ListPropertyStringDoubleBetween(final String property, final Double from, final Double to, final String message) {
        super(message, KRule.LIST_PROPERTY_STRING_DOUBLE_BETWEEN);
        
        this.property = property;
        this.from = from;
        this.to = to;
    }
    
    public void validate(final Object object) throws KException {
        if (object == null) {
            return;
        }

        final String value = this.extractValue(object);

        try {
            final Double d = Double.parseDouble(value.trim());

            if (d >= from && d <= to) {
                return;
            }
            
            throw new Exception(message);
        } catch (Exception e) {
            throw KExceptionHelper.badRequest(message);
        }
    }
    
    public String extractValue(Object object) throws KException {
        final String[] properties = this.property.split("\\.");

        for (final String p : properties) {

            final String endName;

            if (p.length() > 1) {
                endName = p.substring(1);
            } else {
                endName = "";
            }
            
            try {
                final Method method = object.getClass().getMethod(StringUtils.join(new String[]{
                    "get", p.substring(0, 1).toUpperCase(), endName
                }, ""), new Class<?>[]{});
                
                object = method.invoke(object, new Object[]{});
            } catch (Exception e) {
                throw KExceptionHelper.internalServerError("Property not found: " + p);
            }
        }
        
        return object.toString();
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }
}
