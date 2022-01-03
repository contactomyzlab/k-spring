package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class ObjectNull extends KRule {
    
    public ObjectNull(final String message) {
        super(message, KRule.OBJECT_NULL);
    }
    
    public void validate(final Object value) throws KException {
        if (value != null) {
            throw KExceptionHelper.badRequest(message);
        }
    }
}
