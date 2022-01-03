package ve.zlab.core.helper.security.validator.model;

import java.util.List;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class ListNotEmpty extends KRule {
    
    public ListNotEmpty(final String message) {
        super(message, KRule.LIST_NOT_EMPTY);
    }
    
    public void validate(final List value) throws KException {
        if (value == null) {
            return;
        }
        
        if (value.isEmpty()) {
            throw KExceptionHelper.badRequest(message);
        }
    }
}
