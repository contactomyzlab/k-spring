package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class StringNotNullNotEmpty extends KRule {
    
    public StringNotNullNotEmpty(final String message) {
        super(message, KRule.STRING_NOT_NULL_NOT_EMPTY);
    }
    
    public void validate(final String value) throws KException {
        if (value == null) {
            throw KExceptionHelper.badRequest(message);
        }
        
        if (value.trim().isEmpty()) {
            throw KExceptionHelper.badRequest(message);
        }
    }
}
