package ve.zlab.k.test;

import ve.zlab.k.annotations.KClass;
import ve.zlab.k.model.JoinData;
import ve.zlab.k.model.KModelDTO;
import ve.zlab.k.model.KModel;

@KClass(entityClass = MyCustomerDTO.class, columnIdClass = Long.class)
public class MyCustomer extends KModel {
    
    private static final Class<? extends KModelDTO> E = MyCustomerDTO.class;
    
    public static final String ID = c(E, MyCustomerDTO.ID);
    
    public static String ID(final String alias) {
        return alias(ID, alias);
    }

    public static final String NAME = c(E, MyCustomerDTO.NAME);
    
    public static String NAME(final String alias) {
        return alias(NAME, alias);
    }
    
    public static final String LAST_NAME = c(E, MyCustomerDTO.LAST_NAME);
    
    public static String LAST_NAME(final String alias) {
        return alias(LAST_NAME, alias);
    }
    
    /* Joins */
    public static JoinData withMyBook() {
        return hasMany(MyBookDTO.class, MyBook.MY_CUSTOMER_ID);
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
