package ve.zlab.k;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.persistence.NoResultException;
import javax.persistence.NonUniqueResultException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.model.JoinData;
import ve.zlab.k.model.KModel;
import ve.zlab.k.model.KModelDTO;
import ve.zlab.k.model.SelectDataExtracted;
import ve.zlab.k.query.IQuery;
import ve.zlab.k.transaction.ITransaction;

/**
 * This class is responsible for storing all the values that will be included in your sql query to be generated, therefore, 
 * it provides through different methods, the way to supply all the necessary parameters to generate the desired sql query.<br>
 * 
 * The methods that build and execute the sql query according to the type of query are the following:<br><br>
 * 
 * <b>SELECT</b><br>
 * 
 * <ol>
 *      <li><code>single</code> (1 available) </li>
 *      <li><code>multiple</code> (2 availables) </li>
 * </ol><br>
 * 
 * <b>INSERT</b><br>
 * 
 * <ol>
 *      <li><code>insert</code> (8 availables) </li>
 *      <li><code>uInsert</code> (4 availables) </li>
 *      <li><code>nInsert</code> (6 availables) </li>
 *      <li><code>sInsert</code> (4 availables) </li>
 *      <li><code>insertIntoFrom</code> (1 available) </li>
 * </ol><br>
 * 
 * <b>UPDATE</b><br>
 * 
 * <ol>
 *      <li><code>update</code> (4 availables)</li>
 * </ol><br>
 * 
 * <b>DELETE</b><br>
 * 
 * <ol>
 *      <li><code>delete</code> (1 available) </li>
 * </ol><br>
 * 
 * @author contacto@myzlab.com
 * 
 */
public class KQuery {

    private Class<? extends KModelDTO> clazz;
    private KModelDTO kModel;
    private boolean function;
    private String table;
    private List<String> select;
    private List<String> insertInto;
    private List<String> join;
    private List<String> from;
    private List<String> using;
    private List<String[]> where;
    private KContext kContext;
    private boolean distinct;
    private List<String> orderBy;
    private List<String> groupBy;
    private List<String[]> having;
    private Long offset;
    private Long limit;
    private Long page;
    private ITransaction transaction;
    private List<String> distinctOn;
    private Integer whereNullsCount;
    private Integer indexColumn;
    private Map<String, Integer> columns;
    private Map<String, String> processors;
    private Map<String, String> extraColumnsToInsert;
    private boolean onConflictDoNothing;
    
    public KQuery(final String table, final boolean function, final ITransaction transaction) {
        super();

        this.table = table;
        this.function = function;
        this.transaction = transaction;
        this.init();
    }
    
    public KQuery(final String table, final ITransaction transaction) {
        this(table, false, transaction);
    }
    
    public KQuery(final ITransaction transaction, final Class<? extends KModelDTO> clazz) {
        this(null, false, transaction);
        this.clazz = clazz;
    }
    
    public KQuery(final String table) {
        this(table, false, null);
    }
    
    public KQuery(final ITransaction transaction, final KModelDTO kModel) {
        this(null, false, transaction);
        this.kModel = kModel;
    }

    public final void init() {
        this.select = new ArrayList<>();
        this.insertInto = new ArrayList<>();
        this.join = new ArrayList<>();
        this.from = new ArrayList<>();
        this.using = new ArrayList<>();
        this.kContext = new KContext();
        this.where = new ArrayList<>();
        this.groupBy = new ArrayList<>();
        this.orderBy = new ArrayList<>();
        this.having = new ArrayList<>();
        this.distinct = false;
        this.distinctOn = new ArrayList<>();
        this.columns = new HashMap<>();
        this.indexColumn = 0;
        this.whereNullsCount = 0;
        this.processors = new HashMap<>();
        this.extraColumnsToInsert = new HashMap<>();
        this.onConflictDoNothing = false;
    }
    
    public KQuery addProcessor(final String key, final String value) throws KException {
        if (key == null || value == null) {
            return this;
        }
        
        this.processors.put(key, value);

        return this;
    }
    
    public KQuery addExtraColumnsToInsert(final String key, final String value) throws KException {
        if (key == null || value == null) {
            return this;
        }
        
        this.extraColumnsToInsert.put(key, value);

        return this;
    }

    public KQuery distinctOn(final String... cs) throws KException {
        if (cs == null || cs.length == 0) {
            return this;
        }
        
        this.distinct = true;
        this.distinctOn.addAll(Arrays.asList(cs));

        return this;
    }
    
    public KQuery select(final String... cs) throws KException {
        if (cs == null || cs.length == 0) {
            return this;
        }
        
        for (final String c : cs) {
            final SelectDataExtracted extracted = KLogic.extractSelectData(c);
            
            this.columns.put(extracted.getColumnName(), indexColumn++);
        }

        this.select.addAll(Arrays.asList(cs));

        return this;
    }

    public KQuery select(final List<String> cs) throws KException {
        if (cs == null || cs.isEmpty()) {
            return this;
        }
        
        for (final String c : cs) {
            final SelectDataExtracted extracted = KLogic.extractSelectData(c);
            
            this.columns.put(extracted.getColumnName(), indexColumn++);
        }

        this.select.addAll(cs);

        return this;
    }

    public KQuery insertInto(final String... cs) {
        if (cs == null || cs.length == 0) {
            return this;
        }

        this.insertInto.addAll(Arrays.asList(cs));

        return this;
    }

    public KQuery insertInto(final List<String> cs) {
        if (cs == null || cs.isEmpty()) {
            return this;
        }

        this.insertInto.addAll(cs);

        return this;
    }

//    public KQuery union(final KQuery kQuery) {
//        return this;
//    }
    
    public KQuery from(final String from) {
        if (from == null) {
            return null;
        }

        this.from.add(from);

        return this;
    }

    public KQuery from(final Class<? extends KModel> fromClazz) {
        if (fromClazz == null) {
            return null;
        }

        this.from.add(KModel.tableWithAlias(fromClazz));

        return this;
    }

    public KQuery using(final String using) {
        if (using == null) {
            return null;
        }

        this.using.add(using);

        return this;
    }
    
    public KQuery using(final Class<? extends KModel> usingClazz) {
        if (usingClazz == null) {
            return null;
        }

        this.using.add(KModel.tableWithAlias(usingClazz));

        return this;
    }

    public KQuery innerJoinSub(final KQuery kQuery, final String alias, final KJoin kJoin) throws KException {
        if (kQuery == null) {
            return null;
        }

        if (kJoin == null) {
            return null;
        }

        kJoin.setkContext(new KContext());

        kJoin.execute(kJoin);

        final String s = KLogic.innerJoin(kQuery, alias, kJoin);

        if (s == null) {
            return this;
        }
        
        for (int i = 0 ; i < kQuery.getkContext().getParamsCount(); i++) {
            this.kContext.addParam(kQuery.getkContext().getParam(i));
        }
        
        for (int i = 0 ; i < kJoin.getkContext().getParamsCount(); i++) {
            this.kContext.addParam(kJoin.getkContext().getParam(i));
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery innerJoinSub(final KQuery kQuery, final String alias, final String c1, final String c2) throws KException {
        if (kQuery == null) {
            return null;
        }

        final String s = KLogic.innerJoin(kQuery, alias, c1, c2);

        if (s == null) {
            return this;
        }
        
        for (int i = 0 ; i < kQuery.getkContext().getParamsCount(); i++) {
            this.kContext.addParam(kQuery.getkContext().getParam(i));
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery innerJoin(final Class<? extends KModel> clazz, final String c1, final String c2) {
        try {
            return innerJoin(KModelDTO.tableWithAlias(clazz.newInstance().getEntityClass()), c1, c2);
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    public KQuery innerJoin(final Class<? extends KModel> clazz, final KJoin kJoin) {
        try {
            return innerJoin(KModelDTO.tableWithAlias(clazz.newInstance().getEntityClass()), kJoin);
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    public KQuery innerJoin(final String table, final KJoin kJoin) {
        if (kJoin == null) {
            return null;
        }

        kJoin.setkContext(kContext);

        kJoin.execute(kJoin);

        final String s = KLogic.innerJoin(table, kJoin);

        if (s == null) {
            return this;
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery innerJoin(final JoinData joinData) {
        return this.innerJoin(joinData.getTable(), joinData.getFirstColumn(), joinData.getSecondColumn());
    }

    public KQuery innerJoin(final String table, final String c1, final String c2) {
        final String s = KLogic.innerJoin(table, c1, c2);

        if (s == null) {
            return this;
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery leftJoinSub(final KQuery kQuery, final String alias, final KJoin kJoin) throws KException {
        if (kQuery == null) {
            return null;
        }

        if (kJoin == null) {
            return null;
        }

        kJoin.setkContext(new KContext());

        kJoin.execute(kJoin);

        final String s = KLogic.leftJoin(kQuery, alias, kJoin);

        if (s == null) {
            return this;
        }
        
        for (int i = 0 ; i < kQuery.getkContext().getParamsCount(); i++) {
            this.kContext.addParam(kQuery.getkContext().getParam(i));
        }
        
        for (int i = 0 ; i < kJoin.getkContext().getParamsCount(); i++) {
            this.kContext.addParam(kJoin.getkContext().getParam(i));
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery leftJoinSub(final KQuery kQuery, final String alias, final String c1, final String c2) throws KException {
        if (kQuery == null) {
            return null;
        }

        final String s = KLogic.leftJoin(kQuery, alias, c1, c2);

        if (s == null) {
            return this;
        }
        
        for (int i = 0 ; i < kQuery.getkContext().getParamsCount(); i++) {
            this.kContext.addParam(kQuery.getkContext().getParam(i));
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery leftJoin(final Class<? extends KModel> clazz, final KJoin kJoin) {
        try {
            return leftJoin(KModelDTO.tableWithAlias(clazz.newInstance().getEntityClass()), kJoin);
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }

    public KQuery leftJoin(final Class<? extends KModel> clazz, final String c1, final String c2) {
        try {
            return leftJoin(KModelDTO.tableWithAlias(clazz.newInstance().getEntityClass()), c1, c2);
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    public KQuery leftJoin(final String table, final KJoin kJoin) {
        if (kJoin == null) {
            return null;
        }

        kJoin.setkContext(kContext);

        kJoin.execute(kJoin);

        final String s = KLogic.leftJoin(table, kJoin);

        if (s == null) {
            return this;
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery leftJoin(final JoinData joinData) {
        return this.leftJoin(joinData.getTable(), joinData.getFirstColumn(), joinData.getSecondColumn());
    }

    public KQuery leftJoin(final String table, final String c1, final String c2) {
        final String s = KLogic.leftJoin(table, c1, c2);

        if (s == null) {
            return this;
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery rightJoinSub(final KQuery kQuery, final String alias, final KJoin kJoin) throws KException {
        if (kQuery == null) {
            return null;
        }

        if (kJoin == null) {
            return null;
        }

        kJoin.setkContext(new KContext());

        kJoin.execute(kJoin);

        final String s = KLogic.rightJoin(kQuery, alias, kJoin);

        if (s == null) {
            return this;
        }
        
        for (int i = 0 ; i < kQuery.getkContext().getParamsCount(); i++) {
            this.kContext.addParam(kQuery.getkContext().getParam(i));
        }
        
        for (int i = 0 ; i < kJoin.getkContext().getParamsCount(); i++) {
            this.kContext.addParam(kJoin.getkContext().getParam(i));
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery rightJoinSub(final KQuery kQuery, final String alias, final String c1, final String c2) throws KException {
        if (kQuery == null) {
            return null;
        }

        final String s = KLogic.rightJoin(kQuery, alias, c1, c2);

        if (s == null) {
            return this;
        }
        
        for (int i = 0 ; i < kQuery.getkContext().getParamsCount(); i++) {
            this.kContext.addParam(kQuery.getkContext().getParam(i));
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery rightJoin(final Class<? extends KModel> clazz, final KJoin kJoin) {
        try {
            return rightJoin(KModelDTO.tableWithAlias(clazz.newInstance().getEntityClass()), kJoin);
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    public KQuery rightJoin(final Class<? extends KModel> clazz, final String c1, final String c2) {
        try {
            return rightJoin(KModelDTO.tableWithAlias(clazz.newInstance().getEntityClass()), c1, c2);
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }

    public KQuery rightJoin(final String table, final KJoin kJoin) {
        if (kJoin == null) {
            return null;
        }

        kJoin.setkContext(kContext);

        kJoin.execute(kJoin);

        final String s = KLogic.rightJoin(table, kJoin);

        if (s == null) {
            return this;
        }

        this.join.add(s);

        return this;
    }

    public KQuery rightJoin(final JoinData joinData) {
        return this.rightJoin(joinData.getTable(), joinData.getFirstColumn(), joinData.getSecondColumn());
    }
    
    public KQuery rightJoin(final String table, final String c1, final String c2) {
        final String s = KLogic.rightJoin(table, c1, c2);

        if (s == null) {
            return this;
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery fullJoinSub(final KQuery kQuery, final String alias, final KJoin kJoin) throws KException {
        if (kQuery == null) {
            return null;
        }

        if (kJoin == null) {
            return null;
        }

        kJoin.setkContext(new KContext());

        kJoin.execute(kJoin);

        final String s = KLogic.fullJoin(kQuery, alias, kJoin);

        if (s == null) {
            return this;
        }
        
        for (int i = 0 ; i < kQuery.getkContext().getParamsCount(); i++) {
            this.kContext.addParam(kQuery.getkContext().getParam(i));
        }
        
        for (int i = 0 ; i < kJoin.getkContext().getParamsCount(); i++) {
            this.kContext.addParam(kJoin.getkContext().getParam(i));
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery fullJoinSub(final KQuery kQuery, final String alias, final String c1, final String c2) throws KException {
        if (kQuery == null) {
            return null;
        }

        final String s = KLogic.fullJoin(kQuery, alias, c1, c2);

        if (s == null) {
            return this;
        }
        
        for (int i = 0 ; i < kQuery.getkContext().getParamsCount(); i++) {
            this.kContext.addParam(kQuery.getkContext().getParam(i));
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery fullJoin(final Class<? extends KModel> clazz, final KJoin kJoin) {
        try {
            return fullJoin(KModelDTO.tableWithAlias(clazz.newInstance().getEntityClass()), kJoin);
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    public KQuery fullJoin(final Class<? extends KModel> clazz, final String c1, final String c2) {
        try {
            return fullJoin(KModelDTO.tableWithAlias(clazz.newInstance().getEntityClass()), c1, c2);
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }

    public KQuery fullJoin(final String table, final KJoin kJoin) {
        if (kJoin == null) {
            return null;
        }

        kJoin.setkContext(kContext);

        kJoin.execute(kJoin);

        final String s = KLogic.fullJoin(table, kJoin);

        if (s == null) {
            return this;
        }

        this.join.add(s);

        return this;
    }
    
    public KQuery fullJoin(final JoinData joinData) {
        return this.fullJoin(joinData.getTable(), joinData.getFirstColumn(), joinData.getSecondColumn());
    }

    public KQuery fullJoin(final String table, final String c1, final String c2) {
        final String s = KLogic.fullJoin(table, c1, c2);

        if (s == null) {
            return this;
        }

        this.join.add(s);

        return this;
    }

    public KQuery orderByAsc(final String... fields) {
        if (fields == null || fields.length == 0) {
            return null;
        }

        for (final String field : fields) {
            orderBy(field, 1);
        }

        return this;
    }

    public KQuery orderByDesc(final String... fields) {
        if (fields == null || fields.length == 0) {
            return null;
        }

        for (final String field : fields) {
            orderBy(field, -1);
        }

        return this;
    }

    public KQuery orderBy(final String field, final Integer order) {
        if (field == null || order == null) {
            return this;
        }

        if (field.trim().isEmpty()) {
            return this;
        }

        if (order != -1 && order != 1) {
            return this;
        }

        this.orderBy.add(StringUtils.join(new String[]{
            field.trim(), (order == -1) ? "DESC" : "ASC"
        }, " "));

        return this;
    }

    public KQuery distinct() {
        this.distinct = true;

        return this;
    }

    public KQuery having(final String c, final String operation, final Object v) {
        final String[] w = KLogic.where(c, operation, v);

        if (w != null) {
            this.kContext.addParam(v);

            having.add(w);
        }

        return this;
    }

    public KQuery havingRaw(final String c) {
        having.add(KLogic.whereRaw(c));

        return this;
    }

    public KQuery orHaving(final String c, final String operation, final Object v) {
        final String[] w = KLogic.orWhere(c, operation, v);

        if (w != null) {
            this.kContext.addParam(v);

            having.add(w);
        }

        return this;
    }

    public KQuery orHavingRaw(final String c) {
        having.add(KLogic.orWhereRaw(c));

        return this;
    }

    public KQuery having(final KWhere kWhere) throws KException {
        if (kWhere == null) {
            return null;
        }

        kWhere.setkContext(kContext);

        kWhere.execute(kWhere);

        final String[] w = KLogic.where(kWhere);

        if (w != null) {
            having.add(w);
        }

        return this;
    }

    public KQuery orHaving(final KWhere kWhere) throws KException {
        if (kWhere == null) {
            return null;
        }

        kWhere.setkContext(kContext);

        kWhere.execute(kWhere);

        final String[] w = KLogic.orWhere(kWhere);

        if (w != null) {
            having.add(w);
        }

        return this;
    }

    public KQuery whereRaw(final String c) {
        this.where.add(KLogic.whereRaw(c));

        return this;
    }
    
    public KQuery orWhereRaw(final String c) {
        this.where.add(KLogic.orWhereRaw(c));

        return this;
    }
    
    public KQuery whereEqual(final String c, final Object v) {
        final String[] w = KLogic.whereEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereEqual(final String c, final Object v) {
        final String[] w = KLogic.orWhereEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotEqual(final String c, final Object v) {
        final String[] w = KLogic.whereNotEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotEqual(final String c, final Object v) {
        final String[] w = KLogic.orWhereNotEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereIEqual(final String c, final String v) {
        final String[] w = KLogic.whereIEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v.toUpperCase());

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereIEqual(final String c, final String v) {
        final String[] w = KLogic.orWhereIEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v.toUpperCase());

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotIEqual(final String c, final String v) {
        final String[] w = KLogic.whereNotIEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v.toUpperCase());

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotIEqual(final String c, final String v) {
        final String[] w = KLogic.orWhereNotIEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v.toUpperCase());

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereGreaterThan(final String c, final Object v) {
        final String[] w = KLogic.whereGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereGreaterThan(final String c, final Object v) {
        final String[] w = KLogic.orWhereGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotGreaterThan(final String c, final Object v) {
        final String[] w = KLogic.whereNotGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotGreaterThan(final String c, final Object v) {
        final String[] w = KLogic.orWhereNotGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereIGreaterThan(final String c, final String v) {
        final String[] w = KLogic.whereIGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereIGreaterThan(final String c, final String v) {
        final String[] w = KLogic.orWhereIGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereNotIGreaterThan(final String c, final String v) {
        final String[] w = KLogic.whereNotIGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotIGreaterThan(final String c, final String v) {
        final String[] w = KLogic.orWhereNotIGreaterThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereGreaterThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.whereGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereGreaterThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.orWhereGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereNotGreaterThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.whereNotGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotGreaterThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.orWhereNotGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereIGreaterThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.whereIGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereIGreaterThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.orWhereIGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereNotIGreaterThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.whereNotIGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotIGreaterThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.orWhereNotIGreaterThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereLessThan(final String c, final Object v) {
        final String[] w = KLogic.whereLessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereLessThan(final String c, final Object v) {
        final String[] w = KLogic.orWhereLessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotLessThan(final String c, final Object v) {
        final String[] w = KLogic.whereNotLessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotLessThan(final String c, final Object v) {
        final String[] w = KLogic.orWhereNotLessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereILessThan(final String c, final String v) {
        final String[] w = KLogic.whereILessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereILessThan(final String c, final String v) {
        final String[] w = KLogic.orWhereILessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereNotILessThan(final String c, final String v) {
        final String[] w = KLogic.whereNotILessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotILessThan(final String c, final String v) {
        final String[] w = KLogic.orWhereNotILessThan(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereLessThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.whereLessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereLessThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.orWhereLessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotLessThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.whereNotLessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotLessThanOrEqualTo(final String c, final Object v) {
        final String[] w = KLogic.orWhereNotLessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereILessThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.whereILessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereILessThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.orWhereILessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereNotILessThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.whereNotILessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotILessThanOrEqualTo(final String c, final String v) {
        final String[] w = KLogic.orWhereNotILessThanOrEqualTo(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereNotGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereIGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereIGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereIGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereIGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereNotIGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotIGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotIGreaterThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotIGreaterThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereIGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereIGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereIGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereIGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereNotIGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotIGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotIGreaterThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotIGreaterThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereLessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereLessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereLessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereLessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotLessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotLessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotLessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotLessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereILessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereILessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereILessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereILessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotILessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotILessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotILessThanColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotILessThanColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereLessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereLessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereLessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereLessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotLessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotLessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotLessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotLessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereILessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereILessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereILessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereILessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotILessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotILessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotILessThanOrEqualToColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotILessThanOrEqualToColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery where(final String c, final Object v) {
        final String[] w = KLogic.whereEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhere(final String c, final Object v) {
        final String[] w = KLogic.orWhereEqual(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery where(final String c, final String operation, final Object v) {
        final String[] w = KLogic.where(c, operation, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhere(final String c, final String operation, final Object v) {
        final String[] w = KLogic.orWhere(c, operation, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery where(final KWhere kWhere) throws KException {
        if (kWhere == null) {
            return null;
        }

        kWhere.setkContext(kContext);

        kWhere.execute(kWhere);

        final String[] w = KLogic.where(kWhere);

        if (w != null) {
//            final List<Object> objects = kWhere.getkContext().getParams();
//            
//            for (final Object o : objects) {
//                this.kContext.addParam(o);
//            }

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhere(final KWhere kWhere) throws KException {
        if (kWhere == null) {
            return null;
        }

        kWhere.setkContext(kContext);

        kWhere.execute(kWhere);

        final String[] w = KLogic.orWhere(kWhere);

        if (w != null) {
//            final List<Object> objects = kWhere.getkContext().getParams();

//            for (final Object o : objects) {
//                this.kContext.addParam(o);
//            }
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereLikeStartWith(final String c, final String v) {
        final String[] w = KLogic.whereLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v, "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereLikeStartWith(final String c, final String v) {
        final String[] w = KLogic.orWhereLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v, "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereNotLikeStartWith(final String c, final String v) {
        final String[] w = KLogic.whereNotLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v, "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereNotLikeStartWith(final String c, final String v) {
        final String[] w = KLogic.orWhereNotLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v, "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereILikeStartWith(final String c, final String v) {
        final String[] w = KLogic.whereILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereILikeStartWith(final String c, final String v) {
        final String[] w = KLogic.orWhereILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotILikeStartWith(final String c, final String v) {
        final String[] w = KLogic.whereNotILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereNotILikeStartWith(final String c, final String v) {
        final String[] w = KLogic.orWhereNotILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereLikeEndWith(final String c, final String v) {
        final String[] w = KLogic.whereLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereLikeEndWith(final String c, final String v) {
        final String[] w = KLogic.orWhereLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotLikeEndWith(final String c, final String v) {
        final String[] w = KLogic.whereNotLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotLikeEndWith(final String c, final String v) {
        final String[] w = KLogic.orWhereNotLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereILikeEndWith(final String c, final String v) {
        final String[] w = KLogic.whereILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase()
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereILikeEndWith(final String c, final String v) {
        final String[] w = KLogic.orWhereILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase()
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotILikeEndWith(final String c, final String v) {
        final String[] w = KLogic.whereNotILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase()
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotILikeEndWith(final String c, final String v) {
        final String[] w = KLogic.orWhereNotILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase()
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereLikeAny(final String c, final String v) {
        final String[] w = KLogic.whereLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v, "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereLikeAny(final String c, final String v) {
        final String[] w = KLogic.orWhereLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v, "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotLikeAny(final String c, final String v) {
        final String[] w = KLogic.whereNotLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v, "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotLikeAny(final String c, final String v) {
        final String[] w = KLogic.orWhereNotLike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v, "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereILikeAny(final String c, final String v) {
        final String[] w = KLogic.whereILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereILikeAny(final String c, final String v) {
        final String[] w = KLogic.orWhereILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotILikeAny(final String c, final String v) {
        final String[] w = KLogic.whereNotILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotILikeAny(final String c, final String v) {
        final String[] w = KLogic.orWhereNotILike(c, v);

        if (w != null) {
            this.kContext.addParam(StringUtils.join(new String[]{
                "%", v.toUpperCase(), "%"
            }, ""));

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereBetween(final String c, final Object low, final Object high) {
        final String[] w = KLogic.whereBetween(c, low, high);

        if (w != null) {
            this.kContext.addParam(low);
            this.kContext.addParam(high);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereBetween(final String c, final Object low, final Object high) {
        final String[] w = KLogic.orWhereBetween(c, low, high);

        if (w != null) {
            this.kContext.addParam(low);
            this.kContext.addParam(high);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotBetween(final String c, final Object low, final Object high) {
        final String[] w = KLogic.whereNotBetween(c, low, high);

        if (w != null) {
            this.kContext.addParam(low);
            this.kContext.addParam(high);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotBetween(final String c, final Object low, final Object high) {
        final String[] w = KLogic.orWhereNotBetween(c, low, high);

        if (w != null) {
            this.kContext.addParam(low);
            this.kContext.addParam(high);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereIBetween(final String c, final String low, final String high) {
        final String[] w = KLogic.whereIBetween(c, low, high);

        if (w != null) {
            this.kContext.addParam(low.toUpperCase());
            this.kContext.addParam(high.toUpperCase());

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereIBetween(final String c, final String low, final String high) {
        final String[] w = KLogic.orWhereIBetween(c, low, high);

        if (w != null) {
            this.kContext.addParam(low.toUpperCase());
            this.kContext.addParam(high.toUpperCase());

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotIBetween(final String c, final String low, final String high) {
        final String[] w = KLogic.whereNotIBetween(c, low, high);

        if (w != null) {
            this.kContext.addParam(low.toUpperCase());
            this.kContext.addParam(high.toUpperCase());

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotIBetween(final String c, final String low, final String high) {
        final String[] w = KLogic.orWhereNotIBetween(c, low, high);

        if (w != null) {
            this.kContext.addParam(low.toUpperCase());
            this.kContext.addParam(high.toUpperCase());

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereIn(final String c, final Object[] v) {
        return whereIn(c, (v == null) ? null : Arrays.asList(v));
    }
    
    public KQuery whereIn(final String c, final Object[] v, final boolean skipWhenEmpty) {
        return whereIn(c, (v == null) ? null : Arrays.asList(v), skipWhenEmpty);
    }

    public KQuery whereIn(final String c, final Collection v) {
        return whereIn(c, v, false);
    }
    
    public KQuery whereIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        final String[] w = KLogic.whereIn(c, v, skipWhenEmpty);

        if (w != null) {
            if (v != null) {
                for (final Object o : v) {
                    this.kContext.addParam(o);
                }
            }

            where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereIn(final String c, final Object[] v) {
        return orWhereIn(c, (v == null) ? null : Arrays.asList(v));
    }
    
    public KQuery orWhereIn(final String c, final Object[] v, final boolean skipWhenEmpty) {
        return orWhereIn(c, (v == null) ? null : Arrays.asList(v), skipWhenEmpty);
    }
    
    public KQuery orWhereIn(final String c, final Collection v) {
        return orWhereIn(c, v, false);
    }
    
    public KQuery orWhereIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        final String[] w = KLogic.orWhereIn(c, v, skipWhenEmpty);

        if (w != null) {
            if (v != null) {
                for (final Object o : v) {
                    this.kContext.addParam(o);
                }
            }

            where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereNotIn(final String c, final Object[] v) {
        return whereNotIn(c, (v == null) ? null : Arrays.asList(v));
    }
    
    public KQuery whereNotIn(final String c, final Object[] v, final boolean skipWhenEmpty) {
        return whereNotIn(c, (v == null) ? null : Arrays.asList(v), skipWhenEmpty);
    }
    
    public KQuery whereNotIn(final String c, final Collection v) {
        return whereNotIn(c, v, false);
    }

    public KQuery whereNotIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        final String[] w = KLogic.whereNotIn(c, v, skipWhenEmpty);

        if (w != null) {
            if (v != null) {
                for (final Object o : v) {
                    this.kContext.addParam(o);
                }
            }

            where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotIn(final String c, final Object[] v) {
        return orWhereNotIn(c, (v == null) ? null : Arrays.asList(v));
    }
    
    public KQuery orWhereNotIn(final String c, final Object[] v, final boolean skipWhenEmpty) {
        return orWhereNotIn(c, (v == null) ? null : Arrays.asList(v), skipWhenEmpty);
    }
    
    public KQuery orWhereNotIn(final String c, final Collection v) {
        return orWhereNotIn(c, v, false);
    }
    
    public KQuery orWhereNotIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        final String[] w = KLogic.orWhereNotIn(c, v, skipWhenEmpty);

        if (w != null) {
            for (final Object o : v) {
                this.kContext.addParam(o);
            }

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNull(final String c) {
        final String[] w = KLogic.whereNull(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNull(final String c) {
        final String[] w = KLogic.orWhereNull(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotNull(final String c) {
        final String[] w = KLogic.whereNotNull(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotNull(final String c) {
        final String[] w = KLogic.orWhereNotNull(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    @Deprecated
    public KQuery whereDate(final String c, final String v) {
        final String[] w = KLogic.whereDate(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    @Deprecated
    public KQuery orWhereDate(final String c, final String v) {
        final String[] w = KLogic.orWhereDate(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    @Deprecated
    public KQuery whereNotDate(final String c, final String v) {
        final String[] w = KLogic.whereNotDate(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    @Deprecated
    public KQuery orWhereNotDate(final String c, final String v) {
        final String[] w = KLogic.orWhereNotDate(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereMonth(final String c, final int v) {
        final String[] w = KLogic.whereMonth(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereMonth(final String c, final int v) {
        final String[] w = KLogic.orWhereMonth(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotMonth(final String c, final int v) {
        final String[] w = KLogic.whereNotMonth(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotMonth(final String c, final int v) {
        final String[] w = KLogic.orWhereNotMonth(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereDay(final String c, final int v) {
        final String[] w = KLogic.whereDay(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereDay(final String c, final int v) {
        final String[] w = KLogic.orWhereDay(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotDay(final String c, final int v) {
        final String[] w = KLogic.whereNotDay(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotDay(final String c, final int v) {
        final String[] w = KLogic.orWhereNotDay(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereYear(final String c, final int v) {
        final String[] w = KLogic.whereYear(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereYear(final String c, final int v) {
        final String[] w = KLogic.orWhereYear(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotYear(final String c, final int v) {
        final String[] w = KLogic.whereNotYear(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotYear(final String c, final int v) {
        final String[] w = KLogic.orWhereNotYear(c, v);

        if (w != null) {
            this.kContext.addParam(v);

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereIEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereIEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereIEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereIEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereNotIEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.whereNotIEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery orWhereNotIEqualColumn(final String c1, final String c2) {
        final String[] w = KLogic.orWhereNotIEqualColumn(c1, c2);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereExists(final KQuery kQuery) throws KException {
        final String[] w = KLogic.whereExists(kQuery);

        if (w != null) {
            final List<Object> objects = kQuery.getkContext().getParams();
            
            for (final Object o : objects) {
                this.kContext.addParam(o);
            }

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereNotExists(final KQuery kQuery) throws KException {
        final String[] w = KLogic.whereNotExists(kQuery);

        if (w != null) {
            final List<Object> objects = kQuery.getkContext().getParams();
            
            for (final Object o : objects) {
                this.kContext.addParam(o);
            }

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereExists(final KQuery kQuery) throws KException {
        final String[] w = KLogic.orWhereExists(kQuery);

        if (w != null) {
            final List<Object> objects = kQuery.getkContext().getParams();
            
            for (final Object o : objects) {
                this.kContext.addParam(o);
            }

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotExists(final KQuery kQuery) throws KException {
        final String[] w = KLogic.orWhereNotExists(kQuery);

        if (w != null) {
            final List<Object> objects = kQuery.getkContext().getParams();
            
            for (final Object o : objects) {
                this.kContext.addParam(o);
            }

            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereTrue(final String c) {
        final String[] w = KLogic.whereTrue(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereTrue(final String c) {
        final String[] w = KLogic.orWhereTrue(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotTrue(final String c) {
        final String[] w = KLogic.whereNotTrue(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotTrue(final String c) {
        final String[] w = KLogic.orWhereNotTrue(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereFalse(final String c) {
        final String[] w = KLogic.whereFalse(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereFalse(final String c) {
        final String[] w = KLogic.orWhereFalse(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotFalse(final String c) {
        final String[] w = KLogic.whereNotFalse(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotFalse(final String c) {
        final String[] w = KLogic.orWhereNotFalse(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery whereUnknown(final String c) {
        final String[] w = KLogic.whereUnknown(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereUnknown(final String c) {
        final String[] w = KLogic.orWhereUnknown(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery whereNotUnknown(final String c) {
        final String[] w = KLogic.whereNotUnknown(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }
    
    public KQuery orWhereNotUnknown(final String c) {
        final String[] w = KLogic.orWhereNotUnknown(c);

        if (w != null) {
            this.where.add(w);
        } else {
            whereNullsCount++;
        }

        return this;
    }

    public KQuery offset(final Long offset) {
        this.offset = offset;

        return this;
    }

    public KQuery page(final Long page) {
        if (page == null || page <= 0) {
            return this;
        }

        this.page = page;

        calculateOffset();

        return this;
    }

    public KQuery limit(final Long limit) {
        if (limit == null || limit <= 0) {
            return this;
        }

        this.limit = limit;

        calculateOffset();

        return this;
    }

    public void calculateOffset() {
        if (this.limit == null || this.page == null) {
            return;
        }

        this.offset = (this.page - 1) * limit;
    }

    public KQuery groupBy(final String... cs) {
        if (cs == null || cs.length == 0) {
            return this;
        }

        this.groupBy.addAll(Arrays.asList(cs));

        return this;
    }

    /**
     * Build and execute a SQL SELECT query that will find a record by the <code>id</code> column.<br>
     * This method does not ignore other values added to the WHERE clause.<br><br>
     * 
     * The query must return one and only one result.<br>
     * In case the query does not return a result or it returns more than one, a null Krow will be returned.<br><br>
     * 
     * @param id The id of the record to find
     * @return A Krow with the only record returned by the sql query or a null Krow if the query does not return any result or it returns more than one
     * @throws KException If a syntax error is found in the select columns
     * 
     */
    public KRow find(final Long id) throws KException {
        return this.whereEqual("id", id).single();
    }

    /**
     * Build and run a SQL SELECT query that will count based on the created sql query.<br>
     * This method ignores any column added to the SELECT clause.
     * 
     * @return A Krow with the corresponding value within its attributes called <code>count</code>
     * @throws ve.zlab.k.KException
     * 
     */
    public KRow count() throws KException {
        final String ql = KLogic.generateCount(this);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : this.kContext.getParams()) {
            query.setParameter(i++, o);
        }

        return new KRow(new Object[]{
            query.getSingleResult()
        }, new HashMap() {{
            put("count", 0);
        }}, this.getTable());
    }

    /**
     * Build and execute a SQL SELECT query that will use the MAX function on the column sent as a parameter in the created SQL query.<br>
     * This method ignores any column added to the SELECT clause.
     * 
     * @param c The column on which the MAX function will be applied
     * @return A Krow with the corresponding value within its attributes called <code>max</code>
     * 
     */
    public KRow max(final String c) {
        final String ql = KLogic.generateMax(this, c);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : this.kContext.getParams()) {
            query.setParameter(i++, o);
        }

        return new KRow(new Object[]{
            query.getSingleResult()
        }, new HashMap() {{
            put("max", 0);
        }}, this.getTable());
    }

    /**
     * Build and execute a SQL SELECT query that will use the AVG function on the column sent as a parameter in the created SQL query.<br>
     * This method ignores any column added to the SELECT clause.
     * 
     * @param c The column on which the AVG function will be applied
     * @return A Krow with the corresponding value within its attributes called <code>avg</code>
     * 
     */
    public KRow avg(final String c) {
        final String ql = KLogic.generateAvg(this, c);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : this.kContext.getParams()) {
            query.setParameter(i++, o);
        }

        return new KRow(new Object[]{
            query.getSingleResult()
        }, new HashMap() {{
            put("avg", 0);
        }}, this.getTable());
    }

    /**
     * Build and execute a SQL SELECT query that will use the EXISTS function 
     * to encapsulate the generated SQL query according to the supplied parameters.<br>
     * It is not necessary to add columns to the SELECT clause because through this method, the value 1 is added as a column to the SELECT.<br>
     * This method does not ignore other values added to the SELECT clause.<br><br>
     * 
     * The sql SELECT query has the following syntax:<br><br>
     * 
     * <pre>
     * SELECT EXISTS(
     *     SELECT 1
     *     <b>FROM ...</b>
     * )
     * </pre>
     * 
     * @return The value resulting (boolean) from evaluating the EXISTS function on the generated SQL query
     * 
     */
    public Boolean exists() {
        try {
            this.select("1");
        } catch (Exception e) {
            //Nothing to do!
        }

        final String ql = KLogic.generate(this);

//        final Query query = entityManager.createNativeQuery(String.format("SELECT EXISTS(%s)", ql));
        final IQuery query = transaction.createNativeQuery(String.format("SELECT EXISTS(%s)", ql));

        int i = 1;

        for (final Object o : this.kContext.getParams()) {
            query.setParameter(i++, o);
        }

        try {
            return (boolean) query.getSingleResult();
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Build and execute a SQL SELECT query that will use the EXISTS function 
     * to encapsulate the generated SQL query according to the supplied parameters.<br>
     * If the resulting value (Boolean) of evaluating the EXISTS function in the generated SQL
     * query is false, this method throws an exception with the status supplied and a null message.<br>
     * Otherwise, this method will not throw an error.<br>
     * It is not necessary to add columns to the SELECT clause because through this method, the value 1 is added as a column to the SELECT.<br>
     * This method does not ignore other values added to the SELECT clause.<br><br>
     * 
     * The sql SELECT query has the following syntax:<br><br>
     * 
     * <pre>
     * SELECT EXISTS(
     *     SELECT 1
     *     <b>FROM ...</b>
     * )
     * </pre>
     * 
     * @param status The status with which the KException will be thrown
     * @throws ve.zlab.k.KException If the resulting value (Boolean) of evaluating the EXISTS function in the generated SQL query is false 
     */
    public void assertExists(final HttpStatus status) throws KException {
        assertExists(status, null);
    }

    /**
     * Build and execute a SQL SELECT query that will use the EXISTS function 
     * to encapsulate the generated SQL query according to the supplied parameters.<br>
     * If the resulting value (Boolean) of evaluating the EXISTS function in the generated SQL
     * query is false, this method throws an exception with the status and message supplied.<br>
     * Otherwise, this method will not throw an error.<br>
     * It is not necessary to add columns to the SELECT clause because through this method, the value 1 is added as a column to the SELECT.<br>
     * This method does not ignore other values added to the SELECT clause.<br><br>
     * 
     * The sql SELECT query has the following syntax:<br><br>
     * 
     * <pre>
     * SELECT EXISTS(
     *     SELECT 1
     *     <b>FROM ...</b>
     * )
     * </pre>
     * 
     * @param status The status with which the KException will be thrown
     * @param message The message with which the KException will be thrown
     * @throws ve.zlab.k.KException If the resulting value (Boolean) of evaluating the EXISTS function in the generated SQL query is false 
     */
    public void assertExists(final HttpStatus status, final String message) throws KException {
        this.select("1");

        final String ql = KLogic.generate(this);

//        final Query query = entityManager.createNativeQuery(String.format("SELECT EXISTS(%s)", ql));
        final IQuery query = transaction.createNativeQuery(String.format("SELECT EXISTS(%s)", ql));

        int i = 1;

        for (final Object o : this.kContext.getParams()) {
            query.setParameter(i++, o);
        }

        boolean exists = false;

        try {
            exists = (boolean) query.getSingleResult();
        } catch (Exception e) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        if (!exists) {
            throw new KException(status, message);
        }
    }
    
    /**
     * Build and execute a SQL SELECT query that will use the NOT EXISTS function 
     * to encapsulate the generated SQL query according to the supplied parameters.<br>
     * If the resulting value (Boolean) of evaluating the EXISTS function in the generated SQL
     * query is false, this method throws an exception with the status supplied and a null message.<br>
     * Otherwise, this method will not throw an error.<br>
     * It is not necessary to add columns to the SELECT clause because through this method, the value 1 is added as a column to the SELECT.<br>
     * This method does not ignore other values added to the SELECT clause.<br><br>
     * 
     * The sql SELECT query has the following syntax:<br><br>
     * 
     * <pre>
     * SELECT NOT EXISTS(
     *     SELECT 1
     *     <b>FROM ...</b>
     * )
     * </pre>
     * 
     * @param status The status with which the KException will be thrown
     * @throws ve.zlab.k.KException If the resulting value (Boolean) of evaluating the EXISTS function in the generated SQL query is false 
     */
    public void assertNotExists(final HttpStatus status) throws KException {
        assertNotExists(status, null);
    }

    /**
     * Build and execute a SQL SELECT query that will use the NOT EXISTS function 
     * to encapsulate the generated SQL query according to the supplied parameters.<br>
     * If the resulting value (Boolean) of evaluating the EXISTS function in the generated SQL
     * query is false, this method throws an exception with the status and message supplied.<br>
     * Otherwise, this method will not throw an error.<br>
     * It is not necessary to add columns to the SELECT clause because through this method, the value 1 is added as a column to the SELECT.<br>
     * This method does not ignore other values added to the SELECT clause.<br><br>
     * 
     * The sql SELECT query has the following syntax:<br><br>
     * 
     * <pre>
     * SELECT NOT EXISTS(
     *     SELECT 1
     *     <b>FROM ...</b>
     * )
     * </pre>
     * 
     * @param status The status with which the KException will be thrown
     * @param message The message with which the KException will be thrown
     * @throws ve.zlab.k.KException If the resulting value (Boolean) of evaluating the EXISTS function in the generated SQL query is false 
     */
    public void assertNotExists(final HttpStatus status, final String message) throws KException {
        this.select("1");

        final String ql = KLogic.generate(this);

//        final Query query = entityManager.createNativeQuery(String.format("SELECT NOT EXISTS(%s)", ql));
        final IQuery query = transaction.createNativeQuery(String.format("SELECT NOT EXISTS(%s)", ql));

        int i = 1;

        for (final Object o : this.kContext.getParams()) {
            query.setParameter(i++, o);
        }

        boolean exists = false;

        try {
            exists = (boolean) query.getSingleResult();
        } catch (Exception e) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        if (!exists) {
            throw new KException(status, message);
        }
    }

    /**
     * Build and execute a SQL SELECT query.<br><br>
     * 
     * The query must return one and only one result.<br>
     * In case the query does not return a result or it returns more than one, a null Krow will be returned.<br><br>
     * 
     * @return A Krow with the only record returned by the sql query or a null Krow if the query does not return any result or it returns more than one
     * @throws KException If a syntax error is found in the select columns
     * 
     */
    public KRow single() throws KException {
//        final Map<String, Integer> columns = KLogic.getColumns(this);
        final String ql = KLogic.generate(this);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : this.kContext.getParams()) {
            query.setParameter(i++, o);
        }
        
        return query.getSingleKRow(this.columns, this.getTable());

//        Object result = null;
//
//        try {
//            result = query.getSingleResult();
//
//            return new KRow((Object[]) result, columns, this.getTable());
//        } catch (NoResultException | NonUniqueResultException e) {
////            e.printStackTrace();
//            return new KRow();
//        } catch (ClassCastException e) {
//            return new KRow(new Object[]{
//                result
//            }, columns, this.getTable());
//        } catch (Exception e) {
//            e.printStackTrace();
//
//            return null;
//        }
    }

    /**
     * Build and execute a SQL SELECT query.<br><br>
     * 
     * The query may not return records, one or more than one.<br>
     * 
     * @return A KCollection with the records returned by the sql query or an empty KCollection if the query does not return any result
     * @throws KException If a syntax error is found in the select columns
     * 
     */
    public KCollection multiple() throws KException {
        return this.multiple(false);
    }

    /**
     * Build and execute a SQL SELECT query.<br><br>
     * 
     * The query may not return records, one or more than one.<br>
     * In addition to this, if true is sent as the count parameter,
     * a second query (a SELECT COUNT query) is built and executed to return the number of
     * records in the database for the query built initially without considering the
     * limit, offset and order by clause.<br>
     * This value is stored as an  extra property in KCollection with the name <code>total</code>.<br>
     * Very useful to use in list pagination.<br><br>
     * 
     * @param count true or false if you want that the second query (a SELECT COUNT query) be built and executed
     * @return A KCollection with the records returned by the sql query or an empty KCollection if the query does not return any result
     * @throws KException If a syntax error is found in the select columns
     * 
     */
    public KCollection multiple(final boolean count) throws KException {
//        final Map<String, Integer> columns = KLogic.getColumns(this);
        
        if (this.function) {
            throw KExceptionHelper.internalServerError("Calling multiple the wrong way");
        }
        
        final String ql = KLogic.generate(this);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : this.kContext.getParams()) {
            query.setParameter(i++, o);
        }

        final KCollection kCollection = new KCollection(query.getResultList(), this.columns, this.table);

        if (count) {
            final String qlToCount = KLogic.generateCountToExtra(this);

//            final Query queryToCount = entityManager.createNativeQuery(qlToCount);
            final IQuery queryToCount = transaction.createNativeQuery(qlToCount);

            i = 1;

            for (final Object o : this.kContext.getParams()) {
                queryToCount.setParameter(i++, o);
            }

            return kCollection.addExtra("total", queryToCount.getSingleResult());
        }

        return kCollection;
    }
    
    public KCollection multiple(final Object[] values) throws KException {
        return this.multiple(Arrays.asList(values));
    }
    
    public KCollection multiple(final List<Object> values) throws KException {
//        final Map<String, Integer> columns = KLogic.getColumns(this);
        
        if (!this.function) {
            throw KExceptionHelper.internalServerError("Calling multiple the wrong way");
        }
        
        final String ql = KLogic.generate(this, values);
        
//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;
        
        for (final Object o : this.kContext.getParams()) {
            query.setParameter(i++, o);
        }

        final KCollection kCollection = new KCollection(query.getResultList(), this.columns, this.table);

        return kCollection;
    }

    /**
     * Build and execute a SQL INSERT (Multiple) query with the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...), (<b>?</b>, <b>?</b>, ...), ...
     * RETURNING id
     * </pre>
     * 
     * @param columns Array of columns to be placed in the column section of the sql query
     * @param values List of values to be placed in the value section of the sql query
     * 
     * @return Array of long values of id column that were inserted
     * 
     */
    public Long[] insert(final String[] columns, final List<HashMap<String, Object>> values) {
        return this.insert(Arrays.asList(columns), values, "id", Long.class);
    }

    /**
     * Build and execute a SQL INSERT (Multiple) query with the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...), (<b>?</b>, <b>?</b>, ...), ...
     * RETURNING id
     * </pre>
     * 
     * @param columns List of columns to be placed in the column section of the sql query
     * @param values List of values to be placed in the value section of the sql query
     * 
     * @return Array of long values of id column that were inserted
     * 
     */
    public Long[] insert(final List<String> columns, final List<HashMap<String, Object>> values) {
        return this.insert(columns, values, new HashMap<>(), "id", Long.class);
    }

    /**
     * Build and execute a SQL INSERT (Multiple) query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The sql INSERT query has the following syntax:
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...), (<b>P(?)</b>, <b>P(?)</b>, ...), ...
     * RETURNING id
     * </pre>
     * 
     * @param columns Array of columns to be placed in the column section of the sql query
     * @param values List of values to be placed in the value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * 
     * @return Array of long values of id column that were inserted
     * 
     */
    public Long[] insert(final String[] columns, final List<HashMap<String, Object>> values, final HashMap<String, String> processors) {
        return this.insert(Arrays.asList(columns), values, processors, "id", Long.class);
    }

    /**
     * Build and execute a SQL INSERT (Multiple) query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The sql INSERT query has the following syntax:
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...), (<b>P(?)</b>, <b>P(?)</b>, ...), ...
     * RETURNING id
     * </pre>
     * 
     * @param columns List of columns to be placed in the column section of the sql query
     * @param values List of values to be placed in the value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * @deprecated will be removed in next version.
     * 
     * @return Array of long values of id column that were inserted
     * 
     */
    @Deprecated
    public Long[] insert(final List<String> columns, final List<HashMap<String, Object>> values, final HashMap<String, String> processors) {
        final String ql = KLogic.buildMultipleInsertInto(this, columns, values, processors);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        final List<Object> os = query.getResultList();
        final Long[] ids = new Long[os.size()];

        if (os.get(0) instanceof BigInteger) {
            for (int j = 0; j < ids.length; j++) {
                ids[j] = ((BigInteger) os.get(j)).longValue();
            }
        } else {//os[0] instanceof Long
            for (int j = 0; j < ids.length; j++) {
                ids[j] = (Long) os.get(j);
            }
        }

        return ids;
    }

    /**
     * Build and execute a SQL INSERT query with the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...)
     * RETURNING id
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * 
     * @return The long value of id column that was inserted
     * 
     */
    public Long insert(final HashMap<String, Object> d) {
        return this.insert(d, "id", Long.class);
    }

    /**
     * Build and execute a SQL INSERT query.<br><br>
     * 
     * This method receives the concept of returningField which allows modifying the name of the column that the sql INSERT query should return.<br>
     * This column to return must be numeric.<br><br>
     * 
     * The sql INSERT query has the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...)
     * RETURNING <b>returningField</b> 
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @param returningField The name of the column that the sql INSERT query should return
     * @deprecated will be removed in next version.
     * 
     * @return The long value of the column indicated to return
     * 
     */
    @Deprecated
    public Long insert(final HashMap<String, Object> d, final String returningField) {
        return this.insert(d, new HashMap<>(), returningField);
    }

    /**
     * Build and execute a SQL INSERT query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The sql INSERT query has the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...)
     * RETURNING id
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * 
     * @return The long value of id column that was inserted
     * 
     */
    public Long insert(final HashMap<String, Object> d, final HashMap<String, String> processors) {
        return this.insert(d, processors, "id", Long.class);
    }

    /**
     * Build and execute a SQL INSERT query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * This method receives the concept of returningField which allows modifying the name of the column that the sql INSERT query should return.<br>
     * This column to return must be numeric.<br><br>
     * 
     * The sql INSERT query has the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...)
     * RETURNING <b>returningField</b> 
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * @param returningField The name of the column that the sql INSERT query should return
     * @deprecated will be removed in next version.
     * 
     * @return The long value of the column indicated to return
     * 
     */
    @Deprecated
    public Long insert(final HashMap<String, Object> d, final HashMap<String, String> processors, final String returningField) {
        final String ql = KLogic.buildInsertInto(this, d, processors, true, returningField);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        try {
            final Object id = query.getSingleResult();

            if (id instanceof BigInteger) {
                return ((BigInteger) id).longValue();
            }

            return (Long) id;
        } catch (NoResultException | NonUniqueResultException e) {
            return null;
        }
    }

    /**
     * Build and execute a SQL INSERT query with the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...)
     * RETURNING id
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @deprecated will be removed in next version.
     * 
     * @return The UUID value of id column that was inserted
     * 
     */
    @Deprecated
    public UUID uInsert(final HashMap<String, Object> d) {
        return this.uInsert(d, "id");
    }

    /**
     * Build and execute a SQL INSERT query.<br><br>
     * 
     * This method receives the concept of returningField which allows modifying the name of the column that the sql INSERT query should return.<br>
     * This column to return must be UUID.<br><br>
     * 
     * The sql INSERT query has the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...)
     * RETURNING <b>returningField</b> 
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @param returningField The name of the column that the sql INSERT query should return
     * @deprecated will be removed in next version.
     * 
     * @return The UUID value of the column indicated to return
     * 
     */
    @Deprecated
    public UUID uInsert(final HashMap<String, Object> d, final String returningField) {
        return this.uInsert(d, new HashMap<>(), returningField);
    }

    /**
     * Build and execute a SQL INSERT query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The sql INSERT query has the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...)
     * RETURNING id
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * @deprecated will be removed in next version.
     * 
     * @return The UUID value of id column that was inserted
     * 
     */
    @Deprecated
    public UUID uInsert(final HashMap<String, Object> d, final HashMap<String, String> processors) {
        return this.uInsert(d, processors, "id");
    }

    /**
     * Build and execute a SQL INSERT query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * This method receives the concept of returningField which allows modifying the name of the column that the sql INSERT query should return.<br>
     * This column to return must be UUID.<br><br>
     * 
     * The sql INSERT query has the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...)
     * RETURNING <b>returningField</b> 
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * @param returningField The name of the column that the sql INSERT query should return
     * @deprecated will be removed in next version.
     * 
     * @return The UUID value of the column indicated to return
     * 
     */
    @Deprecated
    public UUID uInsert(final HashMap<String, Object> d, final HashMap<String, String> processors, final String returningField) {
        final String ql = KLogic.buildInsertInto(this, d, processors, true, returningField);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        try {
            final Object id = query.getSingleResult();

            return (UUID) id;
        } catch (NoResultException | NonUniqueResultException e) {
            return null;
        }
    }

    /**
     * Build and execute a SQL INSERT query with the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...)
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @deprecated will be removed in next version, use {@link #insertNoReturn(java.util.HashMap)} instead.
     * 
     */
    @Deprecated
    public void nInsert(final HashMap<String, Object> d) {
        this.nInsert(d, new HashMap<>());
    }

    /**
     * Build and execute a SQL INSERT query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The sql INSERT query has the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...)
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * @deprecated will be removed in next version, use {@link #insertNoReturn(java.util.HashMap, java.util.HashMap)} instead.
     * 
     */
    @Deprecated
    public void nInsert(final HashMap<String, Object> d, final HashMap<String, String> processors) {
        final String ql = KLogic.buildInsertInto(this, d, processors, false);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        query.executeUpdate();
    }
    
    /**
     * Build and execute a SQL INSERT (Multiple) query with the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...), (<b>?</b>, <b>?</b>, ...), ...
     * </pre>
     * 
     * @param columns Array of columns to be placed in the column section of the sql query
     * @param values List of values to be placed in the value section of the sql query
     * @deprecated will be removed in next version, use {@link #insertNoReturn(java.lang.String[], java.util.List)} instead.
     * 
     */
    @Deprecated
    public void nInsert(final String[] columns, final List<HashMap<String, Object>> values) {
        this.nInsert(Arrays.asList(columns), values);
    }

    /**
     * Build and execute a SQL INSERT (Multiple) query with the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...), (<b>?</b>, <b>?</b>, ...), ...
     * </pre>
     * 
     * @param columns List of columns to be placed in the column section of the sql query
     * @param values List of values to be placed in the value section of the sql query
     * @deprecated will be removed in next version, use {@link #insertNoReturn(java.util.List, java.util.List)} instead.
     * 
     */
    @Deprecated
    public void nInsert(final List<String> columns, final List<HashMap<String, Object>> values) {
        this.nInsert(columns, values, new HashMap<>());
    }

    /**
     * Build and execute a SQL INSERT (Multiple) query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The sql INSERT query has the following syntax:
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...), (<b>P(?)</b>, <b>P(?)</b>, ...), ...
     * </pre>
     * 
     * @param columns Array of columns to be placed in the column section of the sql query
     * @param values List of values to be placed in the value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * @deprecated will be removed in next version, use {@link #insertNoReturn(java.lang.String[], java.util.List, java.util.HashMap)} instead.
     * 
     */
    @Deprecated
    public void nInsert(final String[] columns, final List<HashMap<String, Object>> values, final HashMap<String, String> processors) {
        this.nInsert(Arrays.asList(columns), values, processors);
    }

    /**
     * Build and execute a SQL INSERT (Multiple) query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The sql INSERT query has the following syntax:
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...), (<b>P(?)</b>, <b>P(?)</b>, ...), ...
     * </pre>
     * 
     * @param columns List of columns to be placed in the column section of the sql query
     * @param values List of values to be placed in the value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * @deprecated will be removed in next version, use {@link #insertNoReturn(java.util.List, java.util.List, java.util.HashMap)} instead.
     * 
     */
    @Deprecated
    public void nInsert(final List<String> columns, final List<HashMap<String, Object>> values, final HashMap<String, String> processors) {
        final String ql = KLogic.buildMultipleInsertInto(this, columns, values, processors, false);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        query.executeUpdate();
    }
    
    /**
     * Build and execute a SQL INSERT query with the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...)
     * RETURNING id
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @deprecated will be removed in next version.
     * 
     * @return The String value of id column that was inserted
     * 
     */
    @Deprecated
    public String sInsert(final HashMap<String, Object> d) {
        return this.sInsert(d, "id");
    }

    /**
     * Build and execute a SQL INSERT query.<br><br>
     * 
     * This method receives the concept of returningField which allows modifying the name of the column that the sql INSERT query should return.<br>
     * This column to return must be String.<br><br>
     * 
     * The sql INSERT query has the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...)
     * RETURNING <b>returningField</b> 
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @param returningField The name of the column that the sql INSERT query should return
     * @deprecated will be removed in next version.
     * 
     * @return The String value of the column indicated to return
     * 
     */
    @Deprecated
    public String sInsert(final HashMap<String, Object> d, final String returningField) {
        return this.sInsert(d, new HashMap<>(), returningField);
    }

    /**
     * Build and execute a SQL INSERT query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The sql INSERT query has the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...)
     * RETURNING id
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * @deprecated will be removed in next version.
     * 
     * @return The String value of id column that was inserted
     * 
     */
    @Deprecated
    public String sInsert(final HashMap<String, Object> d, final HashMap<String, String> processors) {
        return this.sInsert(d, processors, "id");
    }

    /**
     * Build and execute a SQL INSERT query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * This method receives the concept of returningField which allows modifying the name of the column that the sql INSERT query should return.<br>
     * This column to return must be String.<br><br>
     * 
     * The sql INSERT query has the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...)
     * RETURNING <b>returningField</b> 
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * @param returningField The name of the column that the sql INSERT query should return
     * @deprecated will be removed in next version.
     * 
     * @return The String value of the column indicated to return
     * 
     */
    @Deprecated
    public String sInsert(final HashMap<String, Object> d, final HashMap<String, String> processors, final String returningField) {
        final String ql = KLogic.buildInsertInto(this, d, processors, true, returningField);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        try {
            final Object id = query.getSingleResult();

            return (String) id;
        } catch (NoResultException | NonUniqueResultException e) {
            return null;
        }
    }
    
    /* Insert V2 */
    public <T> T insert(final HashMap<String, Object> d, final String returningField, final Class<T> clazz) {
        return this.insert(d, new HashMap<>(), returningField, clazz);
    }
    
    public <T> T insert(final HashMap<String, Object> d, final HashMap<String, String> processors, final String returningField, final Class<T> clazz) {
        final String ql = KLogic.buildInsertInto(this, d, processors, true, returningField.trim());
        
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        try {
            final Object value = query.getSingleResult();
        
            return KQuery.castTo(value, clazz);
        } catch (NoResultException | NonUniqueResultException e) {
            return null;
        }
    }

    public <T> T[] insert(final String[] columns, final List<HashMap<String, Object>> values, final String returningField, final Class<T> clazz) {
        return this.insert(Arrays.asList(columns), values, returningField, clazz);
    }

    public <T> T[] insert(final List<String> columns, final List<HashMap<String, Object>> values, final String returningField, final Class<T> clazz) {
        return this.insert(columns, values, new HashMap<>(), returningField, clazz);
    }

    public <T> T[] insert(final String[] columns, final List<HashMap<String, Object>> values, final HashMap<String, String> processors, final String returningField, final Class<T> clazz) {
        return this.insert(Arrays.asList(columns), values, processors, returningField, clazz);
    }

    public <T> T[] insert(final List<String> columns, final List<HashMap<String, Object>> values, final HashMap<String, String> processors, final String returningField, final Class<T> clazz) {
        final String ql = KLogic.buildMultipleInsertInto(this, columns, values, processors, true, returningField.trim());

        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        final List<Object> os = query.getResultList();
        final T[] values_ = (T[]) Array.newInstance(clazz, os.size());
        
        for (int j = 0; j < values_.length; j++) {
            values_[j] = KQuery.castTo(os.get(j), clazz);
        }

        return values_;
    }
    
    public <T> T insert() throws KException {
        if (kModel == null) {
            throw KExceptionHelper.internalServerError("The KModel cannot be null");
        }
        
        final Map<String, Object> d = kModel.toMap();
        
        if (!extraColumnsToInsert.isEmpty()) {
            d.putAll(extraColumnsToInsert);
        }

        final String ql = KLogic.buildInsertInto(this, d, this.processors, true, kModel.getNameColumnId());

        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        try {
            final Object value = query.getSingleResult();
        
            return (T) KQuery.castTo(value, kModel.getColumnIdClass());
        } catch (NoResultException | NonUniqueResultException e) {
            return null;
        }
    }
    
    public static <T> T castTo(final Object value, Class<T> clazz) {
        
        if (clazz == Long.class) {
            if (value instanceof BigInteger) {
                return (T) (Long) ((BigInteger) value).longValue();
            }

            return (T) value;
        }

        if (clazz == Integer.class) {
            if (value instanceof Short) {
                return (T) (Integer) ((Short) value).intValue();
            }

            return (T) value;
        }
        
        return (T) value;
    }
    
    /* nInsert V2 -- InsertNoReturn */
    
    /**
     * Build and execute a SQL INSERT query with the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...)
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * 
     */
    public void insertNoReturn(final HashMap<String, Object> d) {
        this.insertNoReturn(d, new HashMap<>());
    }

    /**
     * Build and execute a SQL INSERT query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The sql INSERT query has the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...)
     * </pre>
     * 
     * @param d List of columns and values to be placed in the column and value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * 
     */
    public void insertNoReturn(final HashMap<String, Object> d, final HashMap<String, String> processors) {
        final String ql = KLogic.buildInsertInto(this, d, processors, false);

        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        query.executeUpdate();
    }
    
    /**
     * Build and execute a SQL INSERT (Multiple) query with the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...), (<b>?</b>, <b>?</b>, ...), ...
     * </pre>
     * 
     * @param columns Array of columns to be placed in the column section of the sql query
     * @param values List of values to be placed in the value section of the sql query
     * 
     */
    public void insertNoReturn(final String[] columns, final List<HashMap<String, Object>> values) {
        this.insertNoReturn(Arrays.asList(columns), values);
    }

    /**
     * Build and execute a sql INSERT (Multiple) query with the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>?</b>, <b>?</b>, ...), (<b>?</b>, <b>?</b>, ...), ...
     * </pre>
     * 
     * @param columns List of columns to be placed in the column section of the sql query
     * @param values List of values to be placed in the value section of the sql query
     * 
     */
    public void insertNoReturn(final List<String> columns, final List<HashMap<String, Object>> values) {
        this.insertNoReturn(columns, values, new HashMap<>());
    }

    /**
     * Build and execute a SQL INSERT (Multiple) query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The sql INSERT query has the following syntax:
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...), (<b>P(?)</b>, <b>P(?)</b>, ...), ...
     * </pre>
     * 
     * @param columns Array of columns to be placed in the column section of the sql query
     * @param values List of values to be placed in the value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * 
     */
    public void insertNoReturn(final String[] columns, final List<HashMap<String, Object>> values, final HashMap<String, String> processors) {
        this.insertNoReturn(Arrays.asList(columns), values, processors);
    }

    /**
     * Build and execute a SQL INSERT (Multiple) query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the values section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The sql INSERT query has the following syntax:
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * VALUES (<b>P(?)</b>, <b>P(?)</b>, ...), (<b>P(?)</b>, <b>P(?)</b>, ...), ...
     * </pre>
     * 
     * @param columns List of columns to be placed in the column section of the sql query
     * @param values List of values to be placed in the value section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * 
     */
    public void insertNoReturn(final List<String> columns, final List<HashMap<String, Object>> values, final HashMap<String, String> processors) {
        final String ql = KLogic.buildMultipleInsertInto(this, columns, values, processors, false);

        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        query.executeUpdate();
    }
    
    public void insertNoReturn() throws KException {
        if (kModel == null) {
            throw KExceptionHelper.internalServerError("The KModel cannot be null");
        }
        
        if (whereNullsCount > 0) {
            throw KExceptionHelper.internalServerError("The query cannot be executed for protection. A condition has a null value");
        }

        final String ql = KLogic.buildInsertInto(this, kModel.toMap(), new HashMap<>(), false);

        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        query.executeUpdate();
    }

    /**
     * Build and execute a SQL INSERT query whose data comes from a SQL SELECT query<br><br>
     * 
     * The sql INSERT query has the following syntax:<br><br>
     * 
     * <pre>
     * INSERT INTO <b>table</b> (<b>column1</b>, <b>column2</b>, ...)
     * SELECT <b>select_column1</b>, <b>select_column2</b>, ...
     * FROM <b>table_from f</b>
     * [INNER JOIN <b>table_join_i i</b> ON <b>i.column_i</b> = <b>f.column_f</b>]
     * [LEFT  JOIN <b>table_join_l l</b> ON <b>l.column_l</b> = <b>f.column_f</b>]
     * [WHERE ...]
     * </pre>
     * 
     * @throws KException If not add a table and at least one column for insert into clause
     * 
     */
    public void insertIntoFrom() throws KException {
        final String ql = KLogic.buildInsertIntoFrom(this);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        query.executeUpdate();
    }

    /**
     * Build and execute a SQL UPDATE query with the following syntax:<br><br>
     * 
     * <pre>
     * UPDATE <b>table</b>
     * SET <b>column1</b> = <b>?</b>, <b>column2</b> = <b>?</b>, ...
     * [FROM ...]
     * [WHERE ...]
     * </pre>
     * 
     * @param d List of columns and values to be placed in the set section of the sql query
     * 
     * @throws ve.zlab.k.KException It will be thrown when at least one of the conditions supplied 
     * to the where clause of the query is rejected because it is a null value. This is to 
     * protect against executing a query update without conditionals on a table in the wrong way.
     * 
     */
    public void update(final Map<String, Object> d) throws KException {
        this.update(d, new HashMap<>());
    }

    /**
     * Build and execute a SQL UPDATE query.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the set section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The SQL UPDATE query has the following syntax:<br><br>
     * 
     * <pre>
     * UPDATE <b>table</b>
     * SET <b>column1</b> = <b>P(?)</b>, <b>column2</b> = <b>P(?)</b>, ...
     * [FROM ...]
     * [WHERE ...]
     * </pre>
     * 
     * @param d List of columns and values to be placed in the set section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * 
     * @throws ve.zlab.k.KException It will be thrown when at least one of the conditions supplied 
     * to the where clause of the query is rejected because it is a null value. This is to 
     * protect against executing a query update without conditionals on a table in the wrong way.
     * 
     */
    public void update(final Map<String, Object> d, final HashMap<String, String> processors) throws KException {
        if (whereNullsCount > 0) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "The query cannot be executed for protection. A condition has a null value");
        }

        final String ql = KLogic.buildUpdateSet(this, d, processors);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        query.executeUpdate();
    }

    /**
     * Build and execute a SQL UPDATE query that used in the correct way, allows to execute a multiple update with different values on different records of a table.<br><br>
     * 
     * The SQL UPDATE query has the following syntax:<br><br>
     * 
     * <pre>
     * UPDATE <b>table t</b>
     * SET <b>column1</b> = <b>v.column_value1</b>, <b>column2</b> = <b>?</b>, ...
     * FROM (VALUES
     *  (<b>'VALUE1'</b>, <b>valueId1</b>, ...),
     *  (<b>'VALUE2'</b>, <b>valueId2</b>, ...), ...
     * ) AS v(<b>column_value1</b>, <b>column_id</b>, ...)
     * WHERE t.id = v.column_id ...
     * </pre>
     * 
     * @param fromValues List of values to be placed in the from clause
     * @param fromColumns List of columns to be placed in the from clause
     * @param d List of columns and values to be placed in the set section of the sql query
     * 
     * @throws ve.zlab.k.KException It will be thrown when at least one of the conditions supplied 
     * to the where clause of the query is rejected because it is a null value. This is to 
     * protect against executing a query update without conditionals on a table in the wrong way.
     * 
     */
    public void update(final List<Map<String, Object>> fromValues, final String[] fromColumns, final Map<String, Object> d) throws KException {
        this.update(fromValues, fromColumns, d, new HashMap<>());
    }

    /**
     * Build and execute a SQL UPDATE query that used in the correct way, allows to execute a multiple update with different values on different records of a table.<br><br>
     * 
     * This method receives the concept of processors that allow us to replace 
     * the parameters of the set section with the desired parameter, thus allowing 
     * to embed functions within the sql query, for example, <code>?</code> can be changed by <code>LOWER(TO_JSON(?))</code>.<br>
     * We will define the processor with the letter P.<br><br>
     * 
     * The SQL UPDATE query has the following syntax:<br><br>
     * 
     * <pre>
     * UPDATE <b>table t</b>
     * SET <b>column1</b> = <b>v.column_value1</b>, <b>column2</b> = <b>P(?)</b>, ...
     * FROM (VALUES
     *  (<b>'VALUE1'</b>, <b>valueId1</b>, ...),
     *  (<b>'VALUE2'</b>, <b>valueId2</b>, ...), ...
     * ) AS v(<b>column_value1</b>, <b>column_id</b>, ...)
     * WHERE t.id = v.column_id ...
     * </pre>
     * 
     * @param fromValues List of values to be placed in the from clause
     * @param fromColumns List of columns to be placed in the from clause
     * @param d List of columns and values to be placed in the set section of the sql query
     * @param processors List of processors to replace the list of default parameters
     * 
     * @throws ve.zlab.k.KException It will be thrown when at least one of the conditions supplied 
     * to the where clause of the query is rejected because it is a null value. This is to 
     * protect against executing a query update without conditionals on a table in the wrong way.
     * 
     */
    public void update(final List<Map<String, Object>> fromValues, final String[] fromColumns, final Map<String, Object> d, final HashMap<String, String> processors) throws KException {
        if (whereNullsCount > 0) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "The query cannot be executed for protection. A condition has null value");
        }

        final String ql = KLogic.buildUpdateSet(this, fromValues, fromColumns, d, processors);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        query.executeUpdate();
    }
    
    public void update() throws KException {
        if (kModel == null) {
            throw KExceptionHelper.internalServerError("The KModel cannot be null");
        }
        
        if (whereNullsCount > 0) {
            throw KExceptionHelper.internalServerError("The query cannot be executed for protection. A condition has a null value");
        }
        
        if (where.isEmpty()) {
            throw KExceptionHelper.internalServerError("The query cannot be executed for protection. There are no added conditions");
        }

        final String ql = KLogic.buildUpdateSet(this, kModel.toMap(), this.processors);

        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        query.executeUpdate();
    }

    /**
     * Build and run a SQL DELETE query based on the created sql query.<br>
     * This method ignores any column added to the SELECT clause.
     * 
     * @throws ve.zlab.k.KException It will be thrown when at least one of the conditions supplied 
     * to the where clause of the query is rejected because it is a null value. This is to 
     * protect against executing a query update without conditionals on a table in the wrong way.
     * 
     */
    public void delete() throws KException {
        if (whereNullsCount > 0) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "The query cannot be executed for protection. A condition has null value");
        }
        
        if (kModel != null && where.isEmpty()) {
            throw KExceptionHelper.internalServerError("The query cannot be executed for protection. There are no added conditions");
        }

        final String ql = KLogic.buildDeleteFrom(this);

        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : kContext.getParams()) {
            query.setParameter(i++, o);
        }

        query.executeUpdate();
    }

    /* TRUNCATE */
    public void truncate(final boolean restartIdentity, final boolean cascade) {
        final String ql = KLogic.buildTruncate(this, restartIdentity, cascade);

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        query.executeUpdate();
    }

    /* BOOL AND */
    public void assertAllExistOn(
        final String property,
        final List<Object> values,
        final HttpStatus status,
        final String message
    ) throws KException {

        if (values.isEmpty()) {
            return;
        }

        final String with = KLogic.buildWithClause(values);
        final String boolAnd = KLogic.generateBoolAndClause(this, property);

        final String ql = StringUtils.join(new String[]{
            with, boolAnd
        }, " ");

//        final Query query = entityManager.createNativeQuery(ql);
        final IQuery query = transaction.createNativeQuery(ql);

        int i = 1;

        for (final Object o : values) {
            query.setParameter(i++, o);
        }

        for (final Object o : this.kContext.getParams()) {
            query.setParameter(i++, o);
        }

        final Boolean result = (Boolean) query.getSingleResult();

        if (!result) {
            throw new KException(status, message);
        }
    }
    
    public KRow nextval() {
        final String ql = KLogic.generateNextVal(this);
        
        final IQuery query = transaction.createNativeQuery(ql);

        return new KRow(new Object[]{
            query.getSingleResult()
        }, new HashMap() {{
            put("nextval", 0);
        }}, this.getTable());
    }

    /* DD */
    public void dd() throws KException {
        System.out.println(KLogic.generate(this));

        int i = 1;

        System.out.println("Parámetros: >>>>");
        for (final Object o : this.kContext.getParams()) {
            
            System.out.println(StringUtils.join(new String[]{
                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
            }, " "));

            i++;
        }
        
        System.out.println("Columnas: >>>>");
        for (final Map.Entry<String, Integer> c : this.columns.entrySet()) {
            System.out.println("Columna '" + c.getKey() + "' Indice: " + c.getValue());
        }
    }
    
    public void ddCount() throws KException {
        System.out.println(KLogic.generateCount(this));

        int i = 1;

        System.out.println("Parámetros: >>>>");
        for (final Object o : this.kContext.getParams()) {
            
            System.out.println(StringUtils.join(new String[]{
                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
            }, " "));

            i++;
        }
        
        System.out.println("Columnas: >>>>");
        for (final Map.Entry<String, Integer> c : this.columns.entrySet()) {
            System.out.println("Columna '" + c.getKey() + "' Indice: " + c.getValue());
        }
    }
    
    public void ddInsert() throws KException {
        if (kModel == null) {
            throw KExceptionHelper.internalServerError("The KModel cannot be null");
        }
        
        final Map<String, Object> d = kModel.toMap();
        
        if (!extraColumnsToInsert.isEmpty()) {
            d.putAll(extraColumnsToInsert);
        }
        
        System.out.println(KLogic.buildInsertInto(this, d, new HashMap<>(), true, kModel.getNameColumnId()));

        int i = 1;

        System.out.println("Parámetros: >>>>");
        for (final Object o : this.kContext.getParams()) {
            
            System.out.println(StringUtils.join(new String[]{
                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
            }, " "));

            i++;
        }
    }
    
    public void ddInsertNoReturn() throws KException {
        if (kModel == null) {
            throw KExceptionHelper.internalServerError("The KModel cannot be null");
        }
        
        if (whereNullsCount > 0) {
            throw KExceptionHelper.internalServerError("The query cannot be executed for protection. A condition has a null value");
        }
        
        System.out.println(KLogic.buildInsertInto(this, kModel.toMap(), new HashMap<>(), false));

        int i = 1;

        System.out.println("Parámetros: >>>>");
        for (final Object o : this.kContext.getParams()) {
            
            System.out.println(StringUtils.join(new String[]{
                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
            }, " "));

            i++;
        }
    }
    
    public void ddInsertNoReturn(final HashMap<String, Object> d) {
        this.ddInsertNoReturn(d, new HashMap<>());
    }
    
    public void ddInsertNoReturn(final HashMap<String, Object> d, final HashMap<String, String> processors) {
        System.out.println(KLogic.buildInsertInto(this, d, processors, false));

        int i = 1;

        System.out.println("Parámetros: >>>>");
        for (final Object o : this.kContext.getParams()) {
            
            System.out.println(StringUtils.join(new String[]{
                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
            }, " "));

            i++;
        }
    }
    
    public void ddInsertIntoFrom() throws KException {
        System.out.println(KLogic.buildInsertIntoFrom(this));

        int i = 1;

        System.out.println("Parámetros: >>>>");
        for (final Object o : this.kContext.getParams()) {
            
            System.out.println(StringUtils.join(new String[]{
                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
            }, " "));

            i++;
        }
    }
    
    public void ddUpdate() throws KException {
        if (kModel == null) {
            throw KExceptionHelper.internalServerError("The KModel cannot be null");
        }
        
        if (whereNullsCount > 0) {
            throw KExceptionHelper.internalServerError("The query cannot be executed for protection. A condition has a null value");
        }
        
        if (where.isEmpty()) {
            throw KExceptionHelper.internalServerError("The query cannot be executed for protection. There are no added conditions");
        }
        
        System.out.println(KLogic.buildUpdateSet(this, kModel.toMap(), new HashMap<>()));

        int i = 1;

        System.out.println("Parámetros: >>>>");
        for (final Object o : this.kContext.getParams()) {
            
            System.out.println(StringUtils.join(new String[]{
                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
            }, " "));

            i++;
        }
    }
    
    public void ddUpdate(final Map<String, Object> d) throws KException {
        this.ddUpdate(d, new HashMap<>());
    }
    
    public void ddUpdate(final List<Map<String, Object>> fromValues, final String[] fromColumns, final Map<String, Object> d) throws KException {
        this.ddUpdate(fromValues, fromColumns, d, new HashMap<>());
    }
    
    public void ddUpdate(final List<Map<String, Object>> fromValues, final String[] fromColumns, final Map<String, Object> d, final HashMap<String, String> processors) throws KException {
        if (whereNullsCount > 0) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "The query cannot be executed for protection. A condition has a null value");
        }
        
        System.out.println(KLogic.buildUpdateSet(this, fromValues, fromColumns, d, processors));

        int i = 1;

        System.out.println("Parámetros: >>>>");
        for (final Object o : this.kContext.getParams()) {
            
            System.out.println(StringUtils.join(new String[]{
                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
            }, " "));

            i++;
        }
    }
    
    public void ddUpdate(final Map<String, Object> d, final HashMap<String, String> processors) throws KException {
        if (whereNullsCount > 0) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "The query cannot be executed for protection. A condition has a null value");
        }
        
        System.out.println(KLogic.buildUpdateSet(this, d, processors));

        int i = 1;

        System.out.println("Parámetros: >>>>");
        for (final Object o : this.kContext.getParams()) {
            
            System.out.println(StringUtils.join(new String[]{
                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
            }, " "));

            i++;
        }
    }
    
    public void ddDelete() throws KException {
        if (whereNullsCount > 0) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "The query cannot be executed for protection. A condition has null value");
        }
        
        if (kModel != null && where.isEmpty()) {
            throw KExceptionHelper.internalServerError("The query cannot be executed for protection. There are no added conditions");
        }

        System.out.println(KLogic.buildDeleteFrom(this));

        int i = 1;

        System.out.println("Parámetros: >>>>");
        for (final Object o : this.kContext.getParams()) {
            
            System.out.println(StringUtils.join(new String[]{
                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
            }, " "));

            i++;
        }
    }
    
    public void ddMax(final String c) throws KException {
        final String ql = KLogic.generateMax(this, c);
        
        System.out.println(ql);

        int i = 1;

        System.out.println("Parámetros: >>>>");
        for (final Object o : this.kContext.getParams()) {
            
            System.out.println(StringUtils.join(new String[]{
                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
            }, " "));

            i++;
        }
    }

    /* Subquery */
    public String toSubquery() throws KException {
        return KLogic.generate(this);
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getSelect() {
        return select;
    }

    public void setSelect(List<String> select) {
        this.select = select;
    }

    public List<String> getFrom() {
        return from;
    }

    public void setFrom(List<String> from) {
        this.from = from;
    }

    public List<String> getJoin() {
        return join;
    }

    public void setJoin(List<String> join) {
        this.join = join;
    }

    public List<String[]> getWhere() {
        return where;
    }

    public void setWhere(List<String[]> where) {
        this.where = where;
    }

    public List<String> getUsing() {
        return using;
    }

    public void setUsing(List<String> using) {
        this.using = using;
    }

    public KContext getkContext() {
        return kContext;
    }

    public void setkContext(KContext kContext) {
        this.kContext = kContext;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public List<String> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(List<String> orderBy) {
        this.orderBy = orderBy;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
    }

    public List<String[]> getHaving() {
        return having;
    }

    public void setHaving(List<String[]> having) {
        this.having = having;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Long getLimit() {
        return limit;
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public Long getPage() {
        return page;
    }

    public void setPage(Long page) {
        this.page = page;
    }
    
    public List<String> getDistinctOn() {
        return distinctOn;
    }

    public void setDistinctOn(List<String> distinctOn) {
        this.distinctOn = distinctOn;
    }

    public List<String> getInsertInto() {
        return insertInto;
    }

    public void setInsertInto(List<String> insertInto) {
        this.insertInto = insertInto;
    }
    
    public boolean isFunction() {
        return function;
    }

    public void setFunction(boolean function) {
        this.function = function;
    }
    
    public KModelDTO getKModel() {
        return kModel;
    }

    public KQuery setKModel(KModelDTO kModel) {
        this.kModel = kModel;
        
        return this;
    }
    
    public boolean getOnConflictDoNothing() {
        return onConflictDoNothing;
    }

    public void setOnConflictDoNothing(boolean onConflictDoNothing) {
        this.onConflictDoNothing = onConflictDoNothing;
    }
    
    public void generateNewTableNameWithAlias() {
        if (this.table != null) {
            return;
        }
        
        if (this.clazz != null) {
            this.table = KModelDTO.tableWithAlias(this.clazz);
        }
        
        if (this.kModel != null) {
            this.table = this.kModel.getTableWithAlias();
        }
    }
    
    public void generateNewTableName() {
        if (this.table != null) {
            return;
        }
        
        if (this.clazz != null) {
            this.table = KModelDTO.table(this.clazz);
        }
        
        if (this.kModel != null) {
            this.table = this.kModel.getDTOTable();
        }
    }
}
