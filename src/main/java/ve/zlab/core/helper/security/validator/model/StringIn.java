package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class StringIn extends KRule {
    
    private String[] list;

    public StringIn(final String[] list, final String message) {
        super(message, KRule.STRING_IN);
        
        this.list = list;
    }
    
    public void validate(final String value) throws KException {
        if (value == null) {
            return;
        }
        
        for (final String in : list) {
            if (in.equals(value)) {
                return;
            }
        }
        
        throw KExceptionHelper.badRequest(message);
    }

    public String[] getList() {
        return list;
    }

    public void setList(String[] list) {
        this.list = list;
    }
}
