package ve.zlab.core.helper.security.validator.model;

import java.util.regex.Pattern;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class StringOnlyNumbers extends KRule {
    
    private static final String PATTERN = "^[0-9]+$";

    public StringOnlyNumbers(final String message) {
        super(message, KRule.STRING_ONLY_NUMBERS);
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
