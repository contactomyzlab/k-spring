package ve.zlab.core.helper.security.validator.model;

import java.util.regex.Pattern;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class StringOnlyUppercaseLetters extends KRule {
    
    private static final String PATTERN = "^[A-Z]+$";

    public StringOnlyUppercaseLetters(final String message) {
        super(message, KRule.STRING_ONLY_UPPERCASE_LETTERS);
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
