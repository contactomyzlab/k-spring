package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class LongNotNullNotZero extends KRule {
    
    public LongNotNullNotZero(final String message) {
        super(message, KRule.LONG_NOT_NULL_NOT_ZERO);
    }
    
    public void validate(final Long value) throws KException {
        if (value == null) {
            throw KExceptionHelper.badRequest(message);
        }
        
        if (value.equals(0L)) {
            throw KExceptionHelper.badRequest(message);
        }
    }
}
