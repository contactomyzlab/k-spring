package ve.zlab.k;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public abstract class KJoin {
    
    private static String AND = "AND";
    private static String OR = "OR";
    
    private String on;
    private List<String> where;
    private KContext kContext;

    public KJoin() {
        super();
        
        this.init();
    }
    
    public abstract KJoin execute(final KJoin kJoin);
    
    public final void init() {
        this.where = new ArrayList<>();
    }
    
    public KJoin on(final String c1, final String c2) {
        this.on = KLogic.on(c1, c2);
        
        return this;
    }
    
    public KJoin whereRaw(final String c) {
        where.add(KLogic.joinWhere(KLogic.whereRaw(c)));
        
        return this;
    }
    
    public KJoin whereNotEqual(final String c, final Object v) {
        final String w = KLogic.joinWhere(KLogic.whereNotEqual(c, v));
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin where(final String c, final Object v) {
        final String w = KLogic.joinWhere(KLogic.whereEqual(c, v));
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereEqual(final String c, final Object v) {
        final String w = KLogic.joinWhere(KLogic.whereEqual(c, v));
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereBetween(final String c, final Object low, final Object high) {
        final String w = KLogic.joinWhere(KLogic.whereBetween(c, low, high));
        
        if (w != null) {
            this.kContext.addParam(low);
            this.kContext.addParam(high);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereNotBetween(final String c, final Object low, final Object high) {
        final String w = KLogic.joinWhere(KLogic.whereNotBetween(c, low, high));
        
        if (w != null) {
            this.kContext.addParam(low);
            this.kContext.addParam(high);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereIn(final String c, final Object[] v) {
        return whereIn(c, (v == null) ? null : Arrays.asList(v));
    }
    
    public KJoin whereIn(final String c, final Object[] v, final boolean skipWhenEmpty) {
        return whereIn(c, (v == null) ? null : Arrays.asList(v), skipWhenEmpty);
    }
    
    public KJoin whereIn(final String c, final Collection v) {
        return whereIn(c, v, false);
    }
    
    public KJoin whereIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        final String w = KLogic.joinWhere(KLogic.whereIn(c, v, skipWhenEmpty));
        
        if (w != null) {
            for (final Object o : v) {
                this.kContext.addParam(o);
            }
            
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereNotIn(final String c, final Object[] v) {
        return whereNotIn(c, (v == null) ? null : Arrays.asList(v));
    }
    
    public KJoin whereNotIn(final String c, final Object[] v, final boolean skipWhenEmpty) {
        return whereNotIn(c, (v == null) ? null : Arrays.asList(v), skipWhenEmpty);
    }
    
    public KJoin whereNotIn(final String c, final Collection v) {
        return whereNotIn(c, v, false);
    }
    
    public KJoin whereNotIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        final String w = KLogic.joinWhere(KLogic.whereNotIn(c, v, skipWhenEmpty));
        
        if (w != null) {
            for (final Object o : v) {
                this.kContext.addParam(o);
            }
            
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereNull(final String c) {
        final String w = KLogic.joinWhere(KLogic.whereNull(c));
        
        if (w != null) {
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereNotNull(final String c) {
        final String w = KLogic.joinWhere(KLogic.whereNotNull(c));
        
        if (w != null) {
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin where(final String c, final String operation, final Object v) {
        final String w = KLogic.joinWhere(KLogic.where(c, operation, v));
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereDate(final String c, final String v) {
        final String w = KLogic.joinWhere(KLogic.whereDate(c, v));
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereMonth(final String c, final int v) {
        final String w = KLogic.joinWhere(KLogic.whereMonth(c, v));
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereDay(final String c, final int v) {
        final String w = KLogic.joinWhere(KLogic.whereDay(c, v));
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereYear(final String c, final int v) {
        final String w = KLogic.joinWhere(KLogic.whereYear(c, v));
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereEqualColumn(final String c1, final String c2) {
        final String w = KLogic.joinWhere(KLogic.whereEqualColumn(c1, c2));
        
        if (w != null) {   
            where.add(w);
        }
        
        return this;
    }
    
    public KJoin whereExists(final KQuery kQuery) throws KException {
        final String w = KLogic.joinWhere(KLogic.whereExists(kQuery));
        
        if (w != null) {
//            final List<Object> objects = kQuery.getkContext().getParams();
//            
//            for (final Object o : objects) {
//                this.kContext.addParam(o);
//            }

            where.add(w);
        }
        
        return this;
    }

    public String getOn() {
        return on;
    }

    public void setOn(String on) {
        this.on = on;
    }

    public List<String> getWhere() {
        return where;
    }

    public void setWhere(List<String> where) {
        this.where = where;
    }   
    
    public KContext getkContext() {
        return kContext;
    }

    public void setkContext(KContext kContext) {
        this.kContext = kContext;
    }
}
