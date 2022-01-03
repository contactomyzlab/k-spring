package ve.zlab.core.helper.security.validator.model;

import java.util.regex.Pattern;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class StringAlphanumeric extends KRule {
    
    private static final String PATTERN = "^[A-Za-z0-9]+$";

    public StringAlphanumeric(final String message) {
        super(message, KRule.STRING_ALPHANUMERIC);
    }
    
    public void validate(final String value) throws KException {
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        
        final Pattern pattern = Pattern.compile(PATTERN);
        
        if (!pattern.matcher(value).find()) {
            throw KExceptionHelper.badRequest(message);
        }
    }
}
