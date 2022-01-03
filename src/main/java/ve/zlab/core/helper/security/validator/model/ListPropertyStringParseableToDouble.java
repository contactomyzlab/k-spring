package ve.zlab.core.helper.security.validator.model;

import java.lang.reflect.Method;
import org.apache.commons.lang3.StringUtils;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class ListPropertyStringParseableToDouble extends KRule {
    
    private String property;
    
    public ListPropertyStringParseableToDouble(final String property, final String message) {
        super(message, KRule.LIST_PROPERTY_STRING_PARSEABLE_TO_DOUBLE);
        
        this.property = property;
    }
    
    public void validate(final Object object) throws KException {
        if (object == null) {
            return;
        }

        final String value = this.extractValue(object);

        try {
            Double.parseDouble(value.trim());
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
