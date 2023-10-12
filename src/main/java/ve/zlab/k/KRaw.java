package ve.zlab.k;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KRaw {
    
    private String raw;
    private List<Object> params;
    
    public KRaw(final String raw, final Object... o) {
        super();
        
        this.raw = raw;
        this.params = o != null ? Arrays.asList(o) : new ArrayList();
    }

    public String getRaw() {
        return raw;
    }

    public List<Object> getParams() {
        return params;
    }
    
}
