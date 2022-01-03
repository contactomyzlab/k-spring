package ve.zlab.core.helper.security.validator.model;

import java.util.regex.Pattern;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class StringDateISO8601 extends KRule {
    
    private static final String DATE_ISO8601_PATTERN = "^([0-9]{4})-([0-9]{2})-([0-9]{2})$";

    public StringDateISO8601(final String message) {
        super(message, KRule.DATE_ISO8601);
    }
    
    public void validate(final String value) throws KException {
        if (value == null) {
            return;
        }
        
        final Pattern pattern = Pattern.compile(DATE_ISO8601_PATTERN);
        
        if (!pattern.matcher(value).find()) {
            throw KExceptionHelper.badRequest(message);
        }
    }
}
