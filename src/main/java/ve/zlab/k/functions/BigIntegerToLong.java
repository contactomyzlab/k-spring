package ve.zlab.k.functions;

import java.math.BigInteger;
import ve.zlab.k.KException;
import ve.zlab.k.KRow;

public class BigIntegerToLong implements KRowFunction<KRow> {
    
    private String column;

    public BigIntegerToLong(String column) {
        this.column = column;
    }

    @Override
    public Object run(KRow kRow) throws KException {
        final Object o = kRow.get(column);
					
        if (o instanceof BigInteger) {
            return ((BigInteger) o).longValue();
        }

        return o;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }
}
