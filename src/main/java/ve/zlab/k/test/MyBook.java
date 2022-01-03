package ve.zlab.k.test;

import ve.zlab.k.annotations.KClass;
import ve.zlab.k.model.JoinData;
import ve.zlab.k.model.KModelDTO;
import ve.zlab.k.model.KModel;

@KClass(entityClass = MyBookDTO.class, columnIdClass = Long.class)
public class MyBook extends KModel {
    
    private static final Class<? extends KModelDTO> E = MyBookDTO.class;
    
    public static final String ID = c(E, MyBookDTO.ID);
    
    public static String ID(final String alias) {
        return alias(ID, alias);
    }

    public static final String NAME = c(E, MyBookDTO.NAME);
    
    public static String NAME(final String alias) {
        return alias(NAME, alias);
    }
    
    public static final String MY_CUSTOMER_ID = c(E, MyBookDTO.MY_CUSTOMER_ID);
    
    public static String MY_CUSTOMER_ID(final String alias) {
        return alias(MY_CUSTOMER_ID, alias);
    }
    
    /* Joins */
    public static JoinData withMyCustomer() {
        return belongsTo(MyCustomer.class, MY_CUSTOMER_ID);
    }
    /* Joins (END) */

    @Override
    public String getNameColumnId() {
        return ID;
    }
    
    public static JoinData hasMany(final Class<? extends KModelDTO> clazz, final String through) {
        return new JoinData(clazz, ID, through);
    }
}
