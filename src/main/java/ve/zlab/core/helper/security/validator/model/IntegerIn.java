package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class IntegerIn extends KRule {
    
    private Integer[] list;

    public IntegerIn(final Integer[] list, final String message) {
        super(message, KRule.INTEGER_IN);
        
        this.list = list;
    }
    
    public void validate(final Integer value) throws KException {
        if (value == null) {
            return;
        }
        
        for (final Integer in : list) {
            if (in.equals(value)) {
                return;
            }
        }
        
        throw KExceptionHelper.badRequest(message);
    }

    public Integer[] getList() {
        return list;
    }

    public void setList(Integer[] list) {
        this.list = list;
    }
}
