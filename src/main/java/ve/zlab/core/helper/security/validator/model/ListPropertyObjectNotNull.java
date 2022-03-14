package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class ListPropertyObjectNotNull extends KRule {
    
    public ListPropertyObjectNotNull(final String message) {
        super(message, KRule.LIST_PROPERTY_OBJECT_NOT_NULL);
    }
    
    public void validate(final Object value) throws KException {
        if (value == null) {
            throw KExceptionHelper.badRequest(message);
        }
    }
}
