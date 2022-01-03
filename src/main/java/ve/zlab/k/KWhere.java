package ve.zlab.k;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public abstract class KWhere {
    
    private List<String[]> where;
    private KContext kContext;

    public KWhere() {
        super();
        
        this.init();
    }
    
    public abstract KWhere execute(final KWhere kWhere) throws KException;
    
    public final void init() {
        this.where = new ArrayList<>();
    }
    
    public KWhere whereRaw(final String c) {
        where.add(KLogic.whereRaw(c));
        
        return this;
    }
    
    public KWhere orWhereRaw(final String c) {
        where.add(KLogic.orWhereRaw(c));
        
        return this;
    }
    
    public KWhere whereEqual(final String c, final Object v) {
        final String[] w =  KLogic.whereEqual(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereEqual(final String c, final Object v) {
        final String[] w =  KLogic.orWhereEqual(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereNotEqual(final String c, final Object v) {
        final String w[] = KLogic.whereNotEqual(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereNotEqual(final String c, final Object v) {
        final String[] w =  KLogic.orWhereNotEqual(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereIEqual(final String c, final String v) {
        final String[] w = KLogic.whereIEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v.toUpperCase());

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereIEqual(final String c, final String v) {
        final String[] w = KLogic.orWhereIEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v.toUpperCase());

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereNotIEqual(final String c, final String v) {
        final String[] w = KLogic.whereNotIEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v.toUpperCase());

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotIEqual(final String c, final String v) {
        final String[] w = KLogic.orWhereNotIEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v.toUpperCase());

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereGreaterThan(final String c, final Object v) {
        final String[] w = KLogic.whereGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereGreaterThan(final String c, final Object v) {
        final String[] w = KLogic.orWhereGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereNotGreaterThan(final String c, final Object v) {
        final String[] w = KLogic.whereNotGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotGreaterThan(final String c, final Object v) {
        final String[] w = KLogic.orWhereNotGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereIGreaterThan(final String c, final String v) {
        final String[] w = KLogic.whereIGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereIGreaterThan(final String c, final String v) {
        final String[] w = KLogic.orWhereIGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereNotIGreaterThan(final String c, final String v) {
        final String[] w = KLogic.whereNotIGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotIGreaterThan(final String c, final String v) {
        final String[] w = KLogic.orWhereNotIGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereGreaterThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.whereGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereGreaterThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.orWhereGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereNotGreaterThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.whereNotGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotGreaterThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.orWhereNotGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereIGreaterThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.whereIGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereIGreaterThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.orWhereIGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereNotIGreaterThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.whereNotIGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotIGreaterThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.orWhereNotIGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereLessThan(final String c, final Object v) {
        final String[] w = KLogic.whereLessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereLessThan(final String c, final Object v) {
        final String[] w = KLogic.orWhereLessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotLessThan(final String c, final Object v) {
        final String[] w = KLogic.whereNotLessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotLessThan(final String c, final Object v) {
        final String[] w = KLogic.orWhereNotLessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereILessThan(final String c, final String v) {
        final String[] w = KLogic.whereILessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereILessThan(final String c, final String v) {
        final String[] w = KLogic.orWhereILessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereNotILessThan(final String c, final String v) {
        final String[] w = KLogic.whereNotILessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotILessThan(final String c, final String v) {
        final String[] w = KLogic.orWhereNotILessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereLessThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.whereLessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereLessThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.orWhereLessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotLessThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.whereNotLessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotLessThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.orWhereNotLessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereILessThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.whereILessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereILessThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.orWhereILessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereNotILessThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.whereNotILessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotILessThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.orWhereNotILessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereNotGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereIGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereIGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereIGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereIGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereNotIGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotIGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotIGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotIGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereIGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereIGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereIGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereIGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereNotIGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotIGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotIGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotIGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereLessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereLessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereLessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereLessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotLessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotLessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotLessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotLessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereILessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereILessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereILessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereILessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotILessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotILessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotILessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotILessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereLessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereLessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereLessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereLessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotLessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotLessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotLessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotLessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereILessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereILessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereILessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereILessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotILessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotILessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotILessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotILessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere where(final String c, final Object v) {
        final String[] w =  KLogic.whereEqual(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhere(final String c, final Object v) {
        final String[] w =  KLogic.orWhereEqual(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere where(final String c, final String operation, final Object v) {
        final String[] w =  KLogic.where(c, operation, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhere(final String c, final String operation, final Object v) {
        final String[] w =  KLogic.orWhere(c, operation, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere where(final KWhere kWhere) throws KException {
        if (kWhere == null) {
            return null;
        }
        
        kWhere.setkContext(kContext);
        
        kWhere.execute(kWhere);
        
        final String[] w =  KLogic.where(kWhere);
        
        if (w != null) {
//            final List<Object> objects = kWhere.getkContext().getParams();
//            
//            for (final Object o : objects) {
//                this.kContext.addParam(o);
//            }

            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhere(final KWhere kWhere) throws KException {
        if (kWhere == null) {
            return null;
        }
        
        kWhere.setkContext(kContext);
        
        kWhere.execute(kWhere);
        
        final String[] w =  KLogic.orWhere(kWhere);
        
        if (w != null) {
//            final List<Object> objects = kWhere.getkContext().getParams();
//            
//            for (final Object o : objects) {
//                this.kContext.addParam(o);
//            }

            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereLikeStartWith(final String c, final String v) {
        final String[] w = KLogic.whereLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v, "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereLikeStartWith(final String c, final String v) {
        final String[] w = KLogic.orWhereLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v, "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereNotLikeStartWith(final String c, final String v) {
        final String[] w = KLogic.whereNotLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v, "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereNotLikeStartWith(final String c, final String v) {
        final String[] w = KLogic.orWhereNotLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v, "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereILikeStartWith(final String c, final String v) {
        final String[] w = KLogic.whereILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereILikeStartWith(final String c, final String v) {
        final String[] w = KLogic.orWhereILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotILikeStartWith(final String c, final String v) {
        final String[] w = KLogic.whereNotILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereNotILikeStartWith(final String c, final String v) {
        final String[] w = KLogic.orWhereNotILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }

    public KWhere whereLikeEndWith(final String c, final String v) {
        final String[] w = KLogic.whereLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereLikeEndWith(final String c, final String v) {
        final String[] w = KLogic.orWhereLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v
            }, ""));

            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotLikeEndWith(final String c, final String v) {
        final String[] w = KLogic.whereNotLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotLikeEndWith(final String c, final String v) {
        final String[] w = KLogic.orWhereNotLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereILikeEndWith(final String c, final String v) {
        final String[] w = KLogic.whereILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase()
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereILikeEndWith(final String c, final String v) {
        final String[] w = KLogic.orWhereILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase()
            }, ""));

            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotILikeEndWith(final String c, final String v) {
        final String[] w = KLogic.whereNotILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase()
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotILikeEndWith(final String c, final String v) {
        final String[] w = KLogic.orWhereNotILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase()
            }, ""));

            this.where.add(w);
        }

        return this;
    }

    public KWhere whereLikeAny(final String c, final String v) {
        final String[] w = KLogic.whereLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v, "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereLikeAny(final String c, final String v) {
        final String[] w = KLogic.orWhereLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v, "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotLikeAny(final String c, final String v) {
        final String[] w = KLogic.whereNotLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v, "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotLikeAny(final String c, final String v) {
        final String[] w = KLogic.orWhereNotLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v, "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }

    public KWhere whereILikeAny(final String c, final String v) {
        final String[] w = KLogic.whereILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereILikeAny(final String c, final String v) {
        final String[] w = KLogic.orWhereILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotILikeAny(final String c, final String v) {
        final String[] w = KLogic.whereNotILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotILikeAny(final String c, final String v) {
        final String[] w = KLogic.orWhereNotILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereBetween(final String c, final Object low, final Object high) {
        final String[] w =  KLogic.whereBetween(c, low, high);
        
        if (w != null) {
            this.kContext.addParam(low);
            this.kContext.addParam(high);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereBetween(final String c, final Object low, final Object high) {
        final String[] w =  KLogic.orWhereBetween(c, low, high);
        
        if (w != null) {
            this.kContext.addParam(low);
            this.kContext.addParam(high);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereNotBetween(final String c, final Object low, final Object high) {
        final String[] w =  KLogic.whereNotBetween(c, low, high);
        
        if (w != null) {
            this.kContext.addParam(low);
            this.kContext.addParam(high);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereNotBetween(final String c, final Object low, final Object high) {
        final String[] w =  KLogic.orWhereNotBetween(c, low, high);
        
        if (w != null) {
            this.kContext.addParam(low);
            this.kContext.addParam(high);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereIBetween(final String c, final String low, final String high) {
        final String[] w = KLogic.whereIBetween(c, low, high);

        if (w != null) {
            this.kContext.addParam(low.toUpperCase());
            this.kContext.addParam(high.toUpperCase());

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereIBetween(final String c, final String low, final String high) {
        final String[] w = KLogic.orWhereIBetween(c, low, high);

        if (w != null) {
            this.kContext.addParam(low.toUpperCase());
            this.kContext.addParam(high.toUpperCase());

            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotIBetween(final String c, final String low, final String high) {
        final String[] w = KLogic.whereNotIBetween(c, low, high);

        if (w != null) {
            this.kContext.addParam(low.toUpperCase());
            this.kContext.addParam(high.toUpperCase());

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotIBetween(final String c, final String low, final String high) {
        final String[] w = KLogic.orWhereNotIBetween(c, low, high);

        if (w != null) {
            this.kContext.addParam(low.toUpperCase());
            this.kContext.addParam(high.toUpperCase());

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereIn(final String c, final Object[] v) {
        return whereIn(c, (v == null) ? null : Arrays.asList(v));
    }
    
    public KWhere whereIn(final String c, final Object[] v, final boolean skipWhenEmpty) {
        return whereIn(c, (v == null) ? null : Arrays.asList(v), skipWhenEmpty);
    }
    
    public KWhere whereIn(final String c, final Collection v) {
        return whereIn(c, v, false);
    }
    
    public KWhere whereIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        final String[] w =  KLogic.whereIn(c, v, skipWhenEmpty);
        
        if (w != null) {
            if (v != null) {
                for (final Object o : v) {
                    this.kContext.addParam(o);
                }
            }
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereIn(final String c, final Object[] v) {
        return orWhereIn(c, (v == null) ? null : Arrays.asList(v));
    }
    
    public KWhere orWhereIn(final String c, final Object[] v, final boolean skipWhenEmpty) {
        return orWhereIn(c, (v == null) ? null : Arrays.asList(v), skipWhenEmpty);
    }
    
    public KWhere orWhereIn(final String c, final Collection v) {
        return orWhereIn(c, v, false);
    }
    
    public KWhere orWhereIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        final String[] w =  KLogic.orWhereIn(c, v, skipWhenEmpty);
        
        if (w != null) {
            if (v != null) {
                for (final Object o : v) {
                    this.kContext.addParam(o);
                } 
            }
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereNotIn(final String c, final Object[] v) {
        return whereNotIn(c, (v == null) ? null : Arrays.asList(v));
    }
    
    public KWhere whereNotIn(final String c, final Object[] v, final boolean skipWhenEmpty) {
        return whereNotIn(c, (v == null) ? null : Arrays.asList(v), skipWhenEmpty);
    }
    
    public KWhere whereNotIn(final String c, final Collection v) {
        return whereNotIn(c, v, false);
    }
    
    public KWhere whereNotIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        final String[] w =  KLogic.whereNotIn(c, v, skipWhenEmpty);
        
        if (w != null) {
            if (v != null) {
                for (final Object o : v) {
                    this.kContext.addParam(o);
                } 
            }
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereNotIn(final String c, final Object[] v) {
        return orWhereNotIn(c, (v == null) ? null : Arrays.asList(v));
    }
    
    public KWhere orWhereNotIn(final String c, final Object[] v, final boolean skipWhenEmpty) {
        return orWhereNotIn(c, (v == null) ? null : Arrays.asList(v), skipWhenEmpty);
    }
    
    public KWhere orWhereNotIn(final String c, final Collection v) {
        return orWhereNotIn(c, v, false);
    }
    
    public KWhere orWhereNotIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        final String[] w =  KLogic.orWhereNotIn(c, v, skipWhenEmpty);
        
        if (w != null) {
            if (v != null) {
                for (final Object o : v) {
                    this.kContext.addParam(o);
                }
            }
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereNull(final String c) {
        final String[] w =  KLogic.whereNull(c);
        
        if (w != null) {
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereNull(final String c) {
        final String[] w =  KLogic.orWhereNull(c);
        
        if (w != null) {
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereNotNull(final String c) {
        final String[] w =  KLogic.whereNotNull(c);
        
        if (w != null) {
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereNotNull(final String c) {
        final String[] w =  KLogic.orWhereNotNull(c);
        
        if (w != null) {
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereDate(final String c, final String v) {
        final String[] w =  KLogic.whereDate(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereDate(final String c, final String v) {
        final String[] w =  KLogic.orWhereDate(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereNotDate(final String c, final String v) {
        final String[] w = KLogic.whereNotDate(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotDate(final String c, final String v) {
        final String[] w = KLogic.orWhereNotDate(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereMonth(final String c, final int v) {
        final String[] w =  KLogic.whereMonth(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereMonth(final String c, final int v) {
        final String[] w =  KLogic.orWhereMonth(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereNotMonth(final String c, final int v) {
        final String[] w = KLogic.whereNotMonth(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotMonth(final String c, final int v) {
        final String[] w = KLogic.orWhereNotMonth(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereDay(final String c, final int v) {
        final String[] w =  KLogic.whereDay(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereDay(final String c, final int v) {
        final String[] w =  KLogic.orWhereDay(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereNotDay(final String c, final int v) {
        final String[] w = KLogic.whereNotDay(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotDay(final String c, final int v) {
        final String[] w = KLogic.orWhereNotDay(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereYear(final String c, final int v) {
        final String[] w =  KLogic.whereYear(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereYear(final String c, final int v) {
        final String[] w =  KLogic.orWhereYear(c, v);
        
        if (w != null) {
            this.kContext.addParam(v);
            
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereNotYear(final String c, final int v) {
        final String[] w = KLogic.whereNotYear(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotYear(final String c, final int v) {
        final String[] w = KLogic.orWhereNotYear(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereEqualColumn(final String c1, final String c2) {
        final String[] w =  KLogic.whereEqualColumn(c1, c2);
        
        if (w != null) {   
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereEqualColumn(final String c1, final String c2) {
        final String[] w =  KLogic.orWhereEqualColumn(c1, c2);
        
        if (w != null) {   
            where.add(w);
        }
        
        return this;
    }
    
    public KWhere whereNotEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere whereIEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereIEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereIEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereIEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereNotIEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotIEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere orWhereNotIEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotIEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereExists(final KQuery kQuery) throws KException {
        final String[] w =  KLogic.whereExists(kQuery);
        
        if (w != null) {
            final List<Object> objects = kQuery.getkContext().getParams();
            
            for (final Object o : objects) {
                this.kContext.addParam(o);
            }

            where.add(w);
        }
        
        return this;
    }
    
    public KWhere orWhereExists(final KQuery kQuery) throws KException {
        final String[] w =  KLogic.orWhereExists(kQuery);
        
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
    
    public KWhere whereTrue(final String c) {
        final String[] w = KLogic.whereTrue(c);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereTrue(final String c) {
        final String[] w = KLogic.orWhereTrue(c);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotTrue(final String c) {
        final String[] w = KLogic.whereNotTrue(c);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotTrue(final String c) {
        final String[] w = KLogic.orWhereNotTrue(c);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereFalse(final String c) {
        final String[] w = KLogic.whereFalse(c);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereFalse(final String c) {
        final String[] w = KLogic.orWhereFalse(c);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotFalse(final String c) {
        final String[] w = KLogic.whereNotFalse(c);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotFalse(final String c) {
        final String[] w = KLogic.orWhereNotFalse(c);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere whereUnknown(final String c) {
        final String[] w = KLogic.whereUnknown(c);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereUnknown(final String c) {
        final String[] w = KLogic.orWhereUnknown(c);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }

    public KWhere whereNotUnknown(final String c) {
        final String[] w = KLogic.whereNotUnknown(c);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    public KWhere orWhereNotUnknown(final String c) {
        final String[] w = KLogic.orWhereNotUnknown(c);

        if (w != null) {
            this.where.add(w);
        }

        return this;
    }
    
    /* Subquery */
    public String toSubquery() {
        return KLogic.generate(this);
    }
    
    public List<String[]> getWhere() {
        return where;
    }

    public void setWhere(List<String[]> where) {
        this.where = where;
    }

    public KContext getkContext() {
        return kContext;
    }

    public void setkContext(KContext kContext) {
        this.kContext = kContext;
    }
    
    public boolean isEmpty() {
        return this.where.isEmpty();
    }
}
