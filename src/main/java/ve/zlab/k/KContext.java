package ve.zlab.k;

import java.util.ArrayList;
import java.util.List;

public class KContext {
    
    private List<Object> params;

    public KContext() {
        this.params = new ArrayList<>();
    }

    public KContext(List<Object> params) {
        this.params = params;
    }

    public List<Object> getParams() {
        return params;
    }

    public void setParams(List<Object> params) {
        this.params = params;
    }
    
    public void addParam(Object o) {
        this.params.add(o);
    }
    
    public int getParamsCount() {
        return this.params.size();
    }
    
    public void addParam(int i, Object o) {
        this.params.add(i, o);
    }
    
    public void addParamToStart(Object o) {
        this.params.add(0, o);
    }
    
    public Object getParam(final int i) {
        return this.params.get(i);
    }
}
