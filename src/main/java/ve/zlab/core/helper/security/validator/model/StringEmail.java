package ve.zlab.core.helper.security.validator.model;

import java.util.regex.Pattern;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class StringEmail extends KRule {
    
    private static final String EMAIL = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";

    public StringEmail(final String message) {
        super(message, KRule.EMAIL_PATTERN);
    }
    
    public void validate(final String value) throws KException {
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        
        final Pattern pattern = Pattern.compile(EMAIL);
        
        if (!pattern.matcher(value).find()) {
            throw KExceptionHelper.badRequest(message);
        }
    }
    
    public static boolean isEmail(final String value) {
        if (value == null || value.trim().isEmpty()) {
            return false;
        }
        
        final Pattern pattern = Pattern.compile(EMAIL);
        
        if (!pattern.matcher(value).find()) {
            return false;
        }
        
        return true;
    }
}
