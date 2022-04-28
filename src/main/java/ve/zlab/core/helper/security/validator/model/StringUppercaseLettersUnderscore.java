package ve.zlab.core.helper.security.validator.model;

import java.util.regex.Pattern;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class StringUppercaseLettersUnderscore extends KRule {

    private static final String PATTERN = "^[A-Z_]+$";

    public StringUppercaseLettersUnderscore(final String message) {
        super(message, KRule.STRING_UPPERCASE_LETTERS_UNDERSCORE);
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
