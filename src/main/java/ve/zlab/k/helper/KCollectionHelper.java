package ve.zlab.k.helper;

import java.util.ArrayList;
import java.util.HashMap;
import ve.zlab.k.KCollection;

public class KCollectionHelper {
    
    public static KCollection emptyKCollection() {
        return new KCollection(new ArrayList<>(), new HashMap<>(), "");
    }
}
