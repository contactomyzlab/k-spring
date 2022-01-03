package ve.zlab.k.functions;

import ve.zlab.k.KException;

@FunctionalInterface
public interface KRowFunction<KRow> {
    Object run(KRow kRow) throws KException;
}
