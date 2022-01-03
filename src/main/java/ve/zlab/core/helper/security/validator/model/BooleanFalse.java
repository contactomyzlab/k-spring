package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.KException;
import ve.zlab.k.helper.KExceptionHelper;

public class BooleanFalse extends KRule {
    
    public BooleanFalse(final String message) {
        super(message, KRule.BOOLEAN_FALSE);
    }
    
    public void validate(final boolean value) throws KException {
        if (value) {
            throw KExceptionHelper.badRequest(message);
        }
    }
}
