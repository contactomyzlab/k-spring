package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class StringParseableToDouble extends KRule {
    
    public StringParseableToDouble(final String message) {
        super(message, KRule.STRING_PARSEABLE_TO_DOUBLE);
    }
    
    public void validate(final String value) throws KException {
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        
        try {
            Double.parseDouble(value.trim());
        } catch (Exception e) {
            throw KExceptionHelper.badRequest(message);
        }
    }
}
