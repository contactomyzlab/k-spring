package ve.zlab.core.helper.security.validator.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.core.Response;
import org.springframework.http.HttpStatus;
import ve.zlab.k.KException;
import ve.zlab.k.KExecutor;
import ve.zlab.k.KQuery;

public class SqlExistsByColumn extends KRule {
    
    private String table;
    private String column;
    private final List<WhereProperty> whereProperties;

    public SqlExistsByColumn(final String table, final String column, final String message, final WhereProperty... whereProperties) {
        super(message, KRule.SQL_EXISTS_BY_COLUMN);
        
        this.table = table;
        this.column = column;
        
        if (whereProperties == null) {
            this.whereProperties = new ArrayList<>();
        } else {
            this.whereProperties = Arrays.asList(whereProperties);
        }
    }
    
    public void validate(final KExecutor K, final Object value) throws KException {
        if (K == null) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "K reference is mandatory -- SqlExistsByColumn");
        }
        
        if (value == null) {
            return;
        }
        
        final KQuery kQuery = 
            K.
            table(this.table).
            where(this.column, value);
        
        for (final WhereProperty whereProperty : whereProperties) {
            whereProperty.apply(kQuery);
        }
        
        kQuery.assertExists(HttpStatus.BAD_REQUEST, message);
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
