package ve.zlab.k.helper.sql;

public class ProcessorHelper {
    public static String toJson() {
        return "CAST(? AS JSON)";
    }
}
