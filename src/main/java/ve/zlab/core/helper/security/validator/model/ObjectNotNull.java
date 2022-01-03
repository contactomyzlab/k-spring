package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class ObjectNotNull extends KRule {
    
    public ObjectNotNull(final String message) {
        super(message, KRule.OBJECT_NOT_NULL);
    }
    
    public void validate(final Object value) throws KException {
        if (value == null) {
            throw KExceptionHelper.badRequest(message);
        }
    }
}
