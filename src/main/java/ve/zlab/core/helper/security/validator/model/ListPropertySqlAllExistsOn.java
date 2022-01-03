package ve.zlab.core.helper.security.validator.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.springframework.http.HttpStatus;
import ve.zlab.k.KException;
import ve.zlab.k.KExecutor;
import ve.zlab.k.KQuery;

public class ListPropertySqlAllExistsOn extends KRule {
    
    private String table;
    private String column;
    private final List<WhereProperty> whereProperties;

    public ListPropertySqlAllExistsOn(final String table, final String column, final String message, final WhereProperty... whereProperties) {
        super(message, KRule.LIST_ALL_EXISTS_ON);
        
        this.table = table;
        this.column = column;
        
        if (whereProperties == null) {
            this.whereProperties = new ArrayList<>();
        } else {
            this.whereProperties = Arrays.asList(whereProperties);
        }
    }
    
    public void validate(final KExecutor K, final List value) throws KException {
        if (value == null) {
            return;
        }
        
        if (value.isEmpty()) {
            return;
        }
        
        final KQuery kQuery = 
            K.
            table(this.table);
        
        for (final WhereProperty whereProperty : whereProperties) {
            whereProperty.apply(kQuery);
        }
        
        kQuery.assertAllExistOn(this.column, new ArrayList<>(value), HttpStatus.BAD_REQUEST, message);
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }
}
