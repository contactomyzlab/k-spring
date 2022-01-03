package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class DoubleNotNull extends KRule {
    
    public DoubleNotNull(final String message) {
        super(message, KRule.DOUBLE_NOT_NULL);
    }
    
    public void validate(final Double value) throws KException {
        if (value == null) {
            throw KExceptionHelper.badRequest(message);
        }
    }
}
