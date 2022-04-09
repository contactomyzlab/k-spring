package ve.zlab.k.dialect;

import org.hibernate.dialect.PostgreSQL10Dialect;
import org.hibernate.type.descriptor.sql.SqlTypeDescriptor;
import org.hibernate.type.descriptor.sql.VarcharTypeDescriptor;

public class KPostgreSQL10Dialect extends PostgreSQL10Dialect {
    
    public KPostgreSQL10Dialect() {
        super();
        
        registerHibernateType(1111, "string");
    }
    
    @Override
    public SqlTypeDescriptor remapSqlTypeDescriptor(SqlTypeDescriptor sqlTypeDescriptor) {
        switch (sqlTypeDescriptor.getSqlType()) {
            case 1111:
                return VarcharTypeDescriptor.INSTANCE;
            }
        
        return super.remapSqlTypeDescriptor(sqlTypeDescriptor);
    }
}
