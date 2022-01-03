package ve.zlab.k.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import ve.zlab.k.model.KModelDTO;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface KClass {
    public Class<? extends KModelDTO> entityClass();
    public Class columnIdClass();
}