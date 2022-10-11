package ve.zlab.k;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.model.SelectDataExtracted;

public class KLogic {

    public final static String AND = "AND";
    public final static String OR = "OR";

    public KLogic() {
    }
    
    public static String generate(final KQuery kQuery, final List<Object> values) {
        kQuery.generateNewTableNameWithAlias();
        
        final String select = KLogic.buildSelect(kQuery);
        final String from =  KLogic.buildFromFunction(kQuery, values);
        //final String where = KLogic.buildWhere(kQuery);
        //final String groupBy = KLogic.buildGroupBy(kQuery);
        //final String having = KLogic.buildHaving(kQuery);
        //final String orderBy = KLogic.buildOrderBy(kQuery, kQuery.getWhiteListOrderBy());
        //final String limit = KLogic.buildLimit(kQuery);
        //final String offset = KLogic.buildOffset(kQuery);

        final StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(select);

        if (from != null && !from.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(from);
        }

//        if (where != null && !where.isEmpty()) {
//            stringBuilder.append(" ");
//            stringBuilder.append(where);
//        }
//
//        if (groupBy != null && !groupBy.isEmpty()) {
//            stringBuilder.append(" ");
//            stringBuilder.append(groupBy);
//        }
//
//        if (having != null && !having.isEmpty()) {
//            stringBuilder.append(" ");
//            stringBuilder.append(having);
//        }
//
//        if (orderBy != null && !orderBy.isEmpty()) {
//            stringBuilder.append(" ");
//            stringBuilder.append(orderBy);
//        }
//
//        if (limit != null && !limit.isEmpty()) {
//            stringBuilder.append(" ");
//            stringBuilder.append(limit);
//        }
//
//        if (offset != null && !offset.isEmpty()) {
//            stringBuilder.append(" ");
//            stringBuilder.append(offset);
//        }

        return stringBuilder.toString();
    }

    public static String generate(final KQuery kQuery) {
        kQuery.generateNewTableNameWithAlias();
        
        final String with = KLogic.buildWithClause(kQuery);
        final String select = KLogic.buildSelect(kQuery);
        final String from = KLogic.buildFrom(kQuery);
        final String where = KLogic.buildWhere(kQuery);
        final String groupBy = KLogic.buildGroupBy(kQuery);
        final String having = KLogic.buildHaving(kQuery);
        final String orderBy = KLogic.buildOrderBy(kQuery);
        final String limit = KLogic.buildLimit(kQuery);
        final String offset = KLogic.buildOffset(kQuery);

        final StringBuilder stringBuilder = new StringBuilder();
        
        if (with != null && !with.isEmpty()) {
            stringBuilder.append(with);
            stringBuilder.append(" ");
        }

        stringBuilder.append(select);

        if (from != null && !from.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(from);
        }

        if (where != null && !where.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(where);
        }

        if (groupBy != null && !groupBy.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(groupBy);
        }

        if (having != null && !having.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(having);
        }

        if (orderBy != null && !orderBy.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(orderBy);
        }

        if (limit != null && !limit.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(limit);
        }

        if (offset != null && !offset.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(offset);
        }

        return stringBuilder.toString();
    }

    public static String generate(final KWhere kWhere) {
        final String where = KLogic.buildWhere(kWhere);

        return where;
    }
    
    public static String generateMax(final KQuery kQuery, final String c) {
        kQuery.generateNewTableNameWithAlias();
        
        final String select = String.format("SELECT MAX(%s)", c);
        final String from = KLogic.buildFrom(kQuery);
        final String where = KLogic.buildWhere(kQuery);
        final String groupBy = KLogic.buildGroupBy(kQuery);
        final String having = KLogic.buildHaving(kQuery);
        
        final StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(select);

        if (from != null && !from.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(from);
        }

        if (where != null && !where.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(where);
        }

        if (groupBy != null && !groupBy.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(groupBy);
        }

        if (having != null && !having.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(having);
        }

        return stringBuilder.toString();
    }
    
    public static String generateNextValOfTable(final KQuery kQuery) {
        kQuery.generateNewTableName();
        
        return String.format("SELECT NEXTVAL('%s%s'\\:\\:REGCLASS)", kQuery.getTable(), "_id_seq");
    }
    
    public static String generateNextValOfSequence(final KQuery kQuery) {
        return String.format("SELECT NEXTVAL('%s'\\:\\:REGCLASS)", kQuery.getTable());
    }
    
    public static String generateCreateSequenceIfNotExists(final KQuery kQuery) {
        return String.format("CREATE SEQUENCE IF NOT EXISTS %s", kQuery.getTable());
    }
    
    public static String generateAvg(final KQuery kQuery, final String c) {
        kQuery.generateNewTableNameWithAlias();
        
        final String select = String.format("SELECT AVG(%s)", c);
        final String from = KLogic.buildFrom(kQuery);
        final String where = KLogic.buildWhere(kQuery);
        final String groupBy = KLogic.buildGroupBy(kQuery);
        final String having = KLogic.buildHaving(kQuery);
        
        final StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(select);

        if (from != null && !from.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(from);
        }

        if (where != null && !where.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(where);
        }

        if (groupBy != null && !groupBy.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(groupBy);
        }

        if (having != null && !having.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(having);
        }

        return stringBuilder.toString();
    }
    
    public static String buildDistinctCountClause(final List<String> distinctOn) throws KException {
        if (distinctOn == null || distinctOn.isEmpty()) {
            return "DISTINCT COUNT(*)";
        }
        
        if (distinctOn.size() > 1) {
            throw KExceptionHelper.internalServerError("Distinct count with multiples columns is not available");
        }
        
        return StringUtils.join(new String[]{
            "COUNT(DISTINCT", distinctOn.get(0) + ")"
        }, " ");
    }
    
    public static String generateCount(final KQuery kQuery) throws KException {
        kQuery.generateNewTableNameWithAlias();
        
        final String select = (!kQuery.isDistinct()) ? "SELECT COUNT(*)" : StringUtils.join(new String[]{
            "SELECT", KLogic.buildDistinctCountClause(kQuery.getDistinctOn())
        }, " ");
        
        final String from = KLogic.buildFrom(kQuery);
        final String where = KLogic.buildWhere(kQuery);
        final String groupBy = KLogic.buildGroupBy(kQuery);
        final String having = KLogic.buildHaving(kQuery);
        
        final StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(select);

        if (from != null && !from.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(from);
        }

        if (where != null && !where.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(where);
        }

        if (groupBy != null && !groupBy.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(groupBy);
        }

        if (having != null && !having.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(having);
        }

        return stringBuilder.toString();
    }
    
    public static String generateCountToExtra(final KQuery kQuery) throws KException {
        kQuery.generateNewTableNameWithAlias();
        
        final String with = KLogic.buildWithClause(kQuery);
        final String from = KLogic.buildFrom(kQuery);
        final String where = KLogic.buildWhere(kQuery);
        final String groupBy = KLogic.buildGroupBy(kQuery);
        final String having = KLogic.buildHaving(kQuery);
        
        final boolean wrapQuery = kQuery.isDistinct() || (groupBy != null && !groupBy.isEmpty());
        final String select = wrapQuery ? KLogic.buildSelect(kQuery) : "SELECT COUNT(*)";

        final StringBuilder stringBuilder = new StringBuilder();
        
        if (with != null && !with.isEmpty()) {
            stringBuilder.append(with);
            stringBuilder.append(" ");
        }

        stringBuilder.append(select);

        if (from != null && !from.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(from);
        }

        if (where != null && !where.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(where);
        }

        if (groupBy != null && !groupBy.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(groupBy);
        }

        if (having != null && !having.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(having);
        }
        
        if (wrapQuery) {
            return StringUtils.join(new String[]{
                "SELECT COUNT(*) FROM (", stringBuilder.toString() + ") AS god_bless_you"
            }, " ");
        }

        return stringBuilder.toString();
    }

    public static String buildDistinctClause(final List<String> distinctOn) {
        if (distinctOn == null || distinctOn.isEmpty()) {
            return "DISTINCT";
        }
        
        final StringBuilder stringBuilder = new StringBuilder("DISTINCT ON (");
        
        boolean first = true;
            
        for (final String column : distinctOn) {
            if (first) {
                first = false;
            } else {
                stringBuilder.append(", ");
            }

            stringBuilder.append(column);
        }
        
        stringBuilder.append(")");
        
        return stringBuilder.toString();
    }

    public static String buildSelect(final KQuery kQuery) {
        if (kQuery == null) {
            return "SELECT *";
        }

        final List<String> select = kQuery.getSelect();

        final String selectSql = (!kQuery.isDistinct()) ? "SELECT" : StringUtils.join(new String[]{
            "SELECT", KLogic.buildDistinctClause(kQuery.getDistinctOn())
        }, " ");

        if (select == null || select.isEmpty()) {
            return StringUtils.join(new String[]{
                selectSql, "*"
            }, " ");
        }

        return StringUtils.join(new String[]{
            selectSql, StringUtils.join(select, ", ")
        }, " ");
    }

    public static String buildFrom(final KQuery kQuery) {
        final String table = kQuery.getTable();

        if (table == null || table.isEmpty()) {
            return "";
        }

        final String join = buildJoin(kQuery);

        if (join == null || join.isEmpty()) {
            return StringUtils.join(new String[]{
                "FROM", table
            }, " ");
        }

        return StringUtils.join(new String[]{
            "FROM", table, join
        }, " ");
    }
    
    public static String buildFromFunction(final KQuery kQuery, final List<Object> values) {
        final String table = kQuery.getTable();

        if (table == null || table.isEmpty()) {
            return "";
        }

        final StringBuilder stringBuilderValues = new StringBuilder();
        
        boolean first = true;
        int i = 0;

        for (final Object value : values) {
            if (first) {
                first = false;
            } else {
                stringBuilderValues.append(", ");
            }

            stringBuilderValues.append((value == null) ? "NULL" : "?");

            if (value != null) {
                kQuery.getkContext().addParam(i++, value);
            }
        }

        return StringUtils.join(new String[]{
            "FROM", kQuery.getTable(), "(", stringBuilderValues.toString(), ")"
        }, " ");
    }

    public static String buildUpdateFrom(final KQuery kQuery) {
        if (kQuery == null) {
            return "";
        }

        final List<String> from = kQuery.getFrom();

        if (from == null || from.isEmpty()) {
            return "";
        }

        final List<String> finalFrom = new ArrayList<>();

        for (int i = 0; i < from.size(); i++) {
            finalFrom.add(from.get(i));
        }

        return StringUtils.join(new String[]{
            "FROM", StringUtils.join(finalFrom, ", ")
        }, " ");
    }
    
    public static String buildUpdateFrom(final KQuery kQuery, final List<Map<String, Object>> fromValues, final String[] fromColumns, int i) {
        if (kQuery == null) {
            return "";
        }
        
        final StringBuilder stringBuilder = new StringBuilder("(VALUES ");
        
        boolean firstValue = true;
        
        for (final Map<String, Object> values : fromValues) {
            
            if (firstValue) {
                firstValue = false;
            } else {
                stringBuilder.append(", ");
            }

            stringBuilder.append("(");
            
            boolean first = true;
            
            for (final String column : fromColumns) {
                if (first) {
                    first = false;
                } else {
                    stringBuilder.append(", ");
                }
                
                if (values.get(column) instanceof KRaw) {
                    stringBuilder.append(((KRaw) values.get(column)).getRaw());
                } else {
                    stringBuilder.append((values.get(column) == null) ? "NULL" : "?");

                    if (values.get(column) != null) {
                        kQuery.getkContext().addParam(i++, values.get(column));
                    }
                }
            }
            
            stringBuilder.append(")");
        }
        
        stringBuilder.append(") AS v (");
        
        boolean first = true;
        
        for (final String column : fromColumns) {
            if (first) {
                first = false;
            } else {
                stringBuilder.append(", ");
            }

            stringBuilder.append(column);
        }
        
        stringBuilder.append(")");

        return StringUtils.join(new String[]{
            "FROM", stringBuilder.toString()
        }, " ");
    }
        
    public static String buildWhere(final KQuery kQuery) {
        if (kQuery == null) {
            return "";
        }

        final List<String[]> where = kQuery.getWhere();

        if (where == null || where.isEmpty()) {
            return "";
        }

        final String first = where.get(0)[1];

        final int size = where.size();

        if (size == 1) {
            return StringUtils.join(new String[]{
                "WHERE", first
            }, " ");
        }

        final List<String> finalWhere = new ArrayList<>();

        for (int i = 1; i < size; i++) {
            finalWhere.add(KLogic.joinWhere(where.get(i)));
        }

        return StringUtils.join(new String[]{
            "WHERE", first, StringUtils.join(finalWhere, " ")
        }, " ");
    }

    public static String buildWhere(final KWhere kWhere) {
        if (kWhere == null) {
            return "";
        }

        final List<String[]> where = kWhere.getWhere();

        if (where == null || where.isEmpty()) {
            return "";
        }

        final String first = where.get(0)[1];

        final int size = where.size();

        if (size == 1) {
            return StringUtils.join(new String[]{
                first
            }, " ");
        }

        final List<String> finalWhere = new ArrayList<>();

        for (int i = 1; i < size; i++) {
            finalWhere.add(KLogic.joinWhere(where.get(i)));
        }

        return StringUtils.join(new String[]{
            first, StringUtils.join(finalWhere, " ")
        }, " ");
    }

    public static String buildWhere(final KJoin kJoin) {
        if (kJoin == null) {
            return "";
        }

        final List<String> where = kJoin.getWhere();

        if (where == null || where.isEmpty()) {
            return "";
        }

        return StringUtils.join(where, " ");
    }
    
    public static String buildUsing(final KQuery kQuery) {
        if (kQuery == null) {
            return "";
        }

        final List<String> using = kQuery.getUsing();

        if (using == null || using.isEmpty()) {
            return "";
        }
        
        return StringUtils.join(new String[]{
            "USING", StringUtils.join(using, ", ")
        }, " ");
    }
    
    public static String buildWithClause(final KQuery kQuery) {
        final List<String> with = kQuery.getWith();

        if (with == null || with.isEmpty()) {
            return "";
        }

        return StringUtils.join(new String[]{
            "WITH", StringUtils.join(with, ", ")
        }, " ");
    }

    public static String buildJoin(final KQuery kQuery) {
        final List<String> join = kQuery.getJoin();

        if (join == null || join.isEmpty()) {
            return "";
        }

        return StringUtils.join(join, " ");
    }

    public static String buildOrderBy(final KQuery kQuery) {
        if (kQuery == null) {
            return "";
        }

        final List<String> orderBys = kQuery.getOrderBy();

        if (orderBys == null || orderBys.isEmpty()) {
            return "";
        }
        
        for (int i = orderBys.size() - 1; i >= 0; i--) {
            final String orderBy = orderBys.get(i);
            final String[] tokens = orderBy.split(" ");

            if (tokens.length != 2) {
                System.out.println("Ksearch>> Removed order by [" + orderBy + "] because syntax not valid");
                orderBys.remove(i);
                
                continue;
            }
            
            final String myself = tokens[0].replaceAll("[^A-Za-z0-9._]", "");
            
            if (!myself.equals(tokens[0].trim())) {
                System.out.println("Ksearch>> Removed order by [" + orderBy + "] because syntax not valid");
                orderBys.remove(i);
            }
//            
//            if (whiteListOrderBy.containsKey(tokens[0])) {
//                orderBys.set(i, StringUtils.join(new String[]{
//                    whiteListOrderBy.get(tokens[0]), tokens[1]
//                }, " "));
//                
//                continue;
//            }
//            
//            if (!kQuery.getNamesAvailablesToOrder().contains(tokens[0])) {
//                orderBys.remove(i);
//            }
        }

        if (orderBys.isEmpty()) {
            return "";
        }

        return StringUtils.join(new String[]{
            "ORDER BY", StringUtils.join(orderBys, ", ")
        }, " ");
    }

    public static String buildGroupBy(final KQuery kQuery) {
        if (kQuery == null) {
            return "";
        }

        final List<String> groupBy = kQuery.getGroupBy();

        if (groupBy == null || groupBy.isEmpty()) {
            return "";
        }

        return StringUtils.join(new String[]{
            "GROUP BY", StringUtils.join(groupBy, ", ")
        }, " ");
    }

    public static String buildHaving(final KQuery kQuery) {
        if (kQuery == null) {
            return "";
        }

        final List<String[]> having = kQuery.getHaving();

        if (having == null || having.isEmpty()) {
            return "";
        }

        final String first = having.get(0)[1];

        final int size = having.size();

        if (size == 1) {
            return StringUtils.join(new String[]{
                "HAVING", first
            }, " ");
        }

        final List<String> finalHaving = new ArrayList<>();

        for (int i = 1; i < size; i++) {
            finalHaving.add(KLogic.joinWhere(having.get(i)));
        }

        return StringUtils.join(new String[]{
            "HAVING", first, StringUtils.join(finalHaving, " ")
        }, " ");
    }

    public static String buildLimit(final KQuery kQuery) {
        if (kQuery == null) {
            return "";
        }

        final Long limit = kQuery.getLimit();

        if (limit == null) {
            return "";
        }

        return StringUtils.join(new String[]{
            "LIMIT", String.valueOf(limit)
        }, " ");
    }

    public static String buildOffset(final KQuery kQuery) {
        if (kQuery == null) {
            return "";
        }

        final Long offset = kQuery.getOffset();

        if (offset == null) {
            return "";
        }

        return StringUtils.join(new String[]{
            "OFFSET", String.valueOf(offset)
        }, " ");
    }
    
    public static String with(final String name, final KQuery kQuery) throws KException {
        if (kQuery == null) {
            return null;
        }

        if (name == null || name.trim().equals("")) {
            return null;
        }

        return StringUtils.join(new String[]{
            name.trim(), "AS (", kQuery.toSubquery(), ")"
        }, " ");
    }

    public static String on(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return StringUtils.join(new String[]{
            c1, "=", c2
        }, " ");
    }

    public static String innerJoin(final String table, final String c1, final String c2) {
        return generalJoin("INNER JOIN", table, c1, c2);
    }

    public static String leftJoin(final String table, final String c1, final String c2) {
        return generalJoin("LEFT JOIN", table, c1, c2);
    }

    public static String rightJoin(final String table, final String c1, final String c2) {
        return generalJoin("RIGHT JOIN", table, c1, c2);
    }
    
    public static String fullJoin(final String table, final String c1, final String c2) {
        return generalJoin("FULL JOIN", table, c1, c2);
    }

    public static String generalJoin(final String joinType, final String table, final String c1, final String c2) {
        if (table == null || table.isEmpty()) {
            return null;
        }

        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return StringUtils.join(new String[]{
            joinType, table, "ON", KLogic.on(c1, c2)
        }, " ");
    }

//    public static String join(final String table, final KJoin kJoin) {
//        return KLogic.generalJoin("INNER JOIN", table, kJoin);
//    }
    
    public static String innerJoin(final String table, final KJoin kJoin) {
        return KLogic.generalJoin("INNER JOIN", table, kJoin);
    }

    public static String leftJoin(final String table, final KJoin kJoin) {
        return KLogic.generalJoin("LEFT JOIN", table, kJoin);
    }

    public static String rightJoin(final String table, final KJoin kJoin) {
        return KLogic.generalJoin("RIGHT JOIN", table, kJoin);
    }
    
    public static String fullJoin(final String table, final KJoin kJoin) {
        return KLogic.generalJoin("FULL JOIN", table, kJoin);
    }

    public static String innerJoin(final KQuery kQuery, final String alias, final KJoin kJoin) throws KException {
        return KLogic.generalJoin("INNER JOIN", kQuery, alias, kJoin);
    }

    public static String leftJoin(final KQuery kQuery, final String alias, final KJoin kJoin) throws KException {
        return KLogic.generalJoin("LEFT JOIN", kQuery, alias, kJoin);
    }

    public static String rightJoin(final KQuery kQuery, final String alias, final KJoin kJoin) throws KException {
        return KLogic.generalJoin("RIGHT JOIN", kQuery, alias, kJoin);
    }
    
    public static String fullJoin(final KQuery kQuery, final String alias, final KJoin kJoin) throws KException {
        return KLogic.generalJoin("FULL JOIN", kQuery, alias, kJoin);
    }

    public static String innerJoin(final KQuery kQuery, final String alias, final String c1, final String c2) throws KException {
        return KLogic.generalJoin("INNER JOIN", kQuery, alias, c1, c2);
    }

    public static String leftJoin(final KQuery kQuery, final String alias, final String c1, final String c2) throws KException {
        return KLogic.generalJoin("LEFT JOIN", kQuery, alias, c1, c2);
    }

    public static String rightJoin(final KQuery kQuery, final String alias, final String c1, final String c2) throws KException {
        return KLogic.generalJoin("RIGHT JOIN", kQuery, alias, c1, c2);
    }
    
    public static String fullJoin(final KQuery kQuery, final String alias, final String c1, final String c2) throws KException {
        return KLogic.generalJoin("FULL JOIN", kQuery, alias, c1, c2);
    }

    public static String generalJoin(final String joinType, final KQuery kQuery, final String alias, final KJoin kJoin) throws KException {
        if (kQuery == null) {
            return null;
        }

        if (alias == null || alias.isEmpty()) {
            return null;
        }

        if (kJoin == null) {
            return null;
        }

        final String on = kJoin.getOn();
        final String where = KLogic.buildWhere(kJoin);

        if (on == null && (where == null || where.isEmpty())) {
            return null;
        }

        if (where == null || where.isEmpty()) {
            return StringUtils.join(new String[]{
                joinType, "(" + kQuery.toSubquery() + ") AS", alias, "ON", on
            }, " ");
        }
        
        if (on == null) {
            if (where.startsWith("AND ")) {
                return StringUtils.join(new String[]{
                    joinType, "(" + kQuery.toSubquery() + ") AS", alias, "ON", "(", where.substring(4), ")"
                }, " ");
            } else if (where.startsWith("OR ")) {
                return StringUtils.join(new String[]{
                    joinType, "(" + kQuery.toSubquery() + ") AS", alias, "ON", "(", where.substring(3), ")"
                }, " ");
            }
            
            return StringUtils.join(new String[]{
                joinType, "(" + kQuery.toSubquery() + ") AS", alias, "ON", "(", where, ")"
            }, " ");
        }

        return StringUtils.join(new String[]{
            joinType, "(" + kQuery.toSubquery() + ") AS", alias, "ON", "(", on, where, ")"
        }, " ");
    }

    public static String generalJoin(final String joinType, final String table, final KJoin kJoin) {
        if (table == null || table.isEmpty()) {
            return null;
        }

        if (kJoin == null) {
            return null;
        }

        final String on = kJoin.getOn();
        final String where = KLogic.buildWhere(kJoin);

        if (on == null && (where == null || where.isEmpty())) {
            return null;
        }

        if (where == null || where.isEmpty()) {
            return StringUtils.join(new String[]{
                joinType, table, "ON", on
            }, " ");
        }

        if (on == null) {
            if (where.startsWith("AND ")) {
                return StringUtils.join(new String[]{
                    joinType, table, "ON", "(", where.substring(4), ")"
                }, " ");
            } else if (where.startsWith("OR ")) {
                return StringUtils.join(new String[]{
                    joinType, table, "ON", "(", where.substring(3), ")"
                }, " ");
            }
            
            return StringUtils.join(new String[]{
                joinType, table, "ON", "(", where, ")"
            }, " ");
        }

        return StringUtils.join(new String[]{
            joinType, table, "ON", "(", on, where, ")"
        }, " ");
    }

    public static String generalJoin(final String joinType, final KQuery kQuery, final String alias, final String c1, final String c2) throws KException {
        if (kQuery == null) {
            return null;
        }

        if (alias == null || alias.isEmpty()) {
            return null;
        }

        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return StringUtils.join(new String[]{
            joinType, "(" + kQuery.toSubquery() + ") AS", alias, "ON", KLogic.on(c1, c2)
        }, " ");
    }

    public static String[] whereRaw(final String c) {
        return new String[]{
            AND, c
        };
    }

    public static String[] whereEqual(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "=", "?"
            }, " ")
        };
    }
    
    public static String[] whereGreaterThan(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, ">", "?"
            }, " ")
        };
    }
    
    public static String[] whereGreaterThanOrEqualTo(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, ">=", "?"
            }, " ")
        };
    }
    
    public static String[] whereLessThan(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "<", "?"
            }, " ")
        };
    }
    
    public static String[] whereLessThanOrEqualTo(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "<=", "?"
            }, " ")
        };
    }
    
    public static String[] whereGreaterThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c1, ">", c2
            }, " ")
        };
    }
    
    public static String[] whereGreaterThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c1, ">=", c2
            }, " ")
        };
    }
    
    public static String[] whereLessThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c1, "<", c2
            }, " ")
        };
    }
    
    public static String[] whereLessThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c1, "<=", c2
            }, " ")
        };
    }
    
    public static String[] whereIGreaterThan(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), ">", "?"
            }, " ")
        };
    }
    
    public static String[] whereIGreaterThanOrEqualTo(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), ">=", "?"
            }, " ")
        };
    }
    
    public static String[] whereILessThan(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), "<", "?"
            }, " ")
        };
    }
    
    public static String[] whereILessThanOrEqualTo(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), "<=", "?"
            }, " ")
        };
    }
    
    public static String[] whereIGreaterThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "UPPER(", c1, ")", ">", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] whereIGreaterThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "UPPER(", c1, ")", ">=", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] whereILessThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "UPPER(", c1, ")", "<", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] whereILessThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "UPPER(", c1, ")", "<=", "UPPER(", c2, ")"
            }, " ")
        };
    }

    public static String[] whereIEqual(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), "=", "?"
            }, " ")
        };
    }
    
    public static String[] whereNotIEqual(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "NOT UPPER(", c, ")"
                }, ""), "=", "?"
            }, " ")
        };
    }

    public static String[] whereLike(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "LIKE", "?"
            }, " ")
        };
    }
    
    public static String[] whereNotLike(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT", c, "LIKE", "?"
            }, " ")
        };
    }

    public static String[] whereILike(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), "LIKE", "?"
            }, " ")
        };
    }
    
    public static String[] whereNotILike(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "NOT UPPER(", c, ")"
                }, ""), "LIKE", "?"
            }, " ")
        };
    }

    public static String[] whereNotEqual(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "<>", "?"
            }, " ")
        };
    }
    
    public static String[] whereNotGreaterThan(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT", c, ">", "?"
            }, " ")
        };
    }
    
    public static String[] whereNotGreaterThanOrEqualTo(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT", c, ">=", "?"
            }, " ")
        };
    }
    
    public static String[] whereNotLessThan(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT", c, "<", "?"
            }, " ")
        };
    }
    
    public static String[] whereNotLessThanOrEqualTo(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT", c, "<=", "?"
            }, " ")
        };
    }
    
    public static String[] whereNotGreaterThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT", c1, ">", c2
            }, " ")
        };
    }
    
    public static String[] whereNotGreaterThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT", c1, ">=", c2
            }, " ")
        };
    }
    
    public static String[] whereNotLessThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT", c1, "<", c2
            }, " ")
        };
    }
    
    public static String[] whereNotLessThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT", c1, "<=", c2
            }, " ")
        };
    }
    
    public static String[] whereNotIGreaterThan(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "NOT UPPER(", c, ")"
                }, ""), ">", "?"
            }, " ")
        };
    }
    
    public static String[] whereNotIGreaterThanOrEqualTo(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "NOT UPPER(", c, ")"
                }, ""), ">=", "?"
            }, " ")
        };
    }
    
    public static String[] whereNotILessThan(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "NOT UPPER(", c, ")"
                }, ""), "<", "?"
            }, " ")
        };
    }
    
    public static String[] whereNotILessThanOrEqualTo(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "NOT UPPER(", c, ")"
                }, ""), "<=", "?"
            }, " ")
        };
    }
    
    public static String[] whereNotIGreaterThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT UPPER(", c1, ")", ">", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] whereNotIGreaterThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT UPPER(", c1, ")", ">=", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] whereNotILessThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT UPPER(", c1, ")", "<", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] whereNotILessThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT UPPER(", c1, ")", "<=", "UPPER(", c2, ")"
            }, " ")
        };
    }

    public static String[] whereBetween(final String c, final Object low, final Object high) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (low == null || high == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "BETWEEN ? AND ?"
            }, " ")
        };
    }

    public static String[] whereNotBetween(final String c, final Object low, final Object high) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (low == null || high == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "NOT BETWEEN ? AND ?"
            }, " ")
        };
    }
    
    public static String[] whereIBetween(final String c, final String low, final String high) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (low == null || high == null) {
            return null;
        }
        
        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), "BETWEEN ? AND ?"
            }, " ")
        };
    }
    
    public static String[] whereNotIBetween(final String c, final String low, final String high) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (low == null || high == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), "NOT BETWEEN ? AND ?"
            }, " ")
        };
    }

    public static String[] whereIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        if (c == null || c.isEmpty()) {
            return null;
        }
        
        if (v == null) {
            return null;
        }

        if (v.isEmpty()) {
            if (skipWhenEmpty) {
                return null;
            }
            
            return new String[]{
                AND, StringUtils.join(new String[]{
                    "1 = 0"
                }, " ")
            };
        }

        final StringBuilder stringBuilder = new StringBuilder();

        final int length = v.size();

        for (int i = 0; i < length; i++) {
            if (i > 0) {
                stringBuilder.append(", ");
            }

            stringBuilder.append("?");
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "IN (", stringBuilder.toString(), ")"
            }, " ")
        };
    }

    public static String[] whereNotIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        if (c == null || c.isEmpty()) {
            return null;
        }
        
        if (v == null) {
            return null;
        }

        if (v.isEmpty()) {
            if (skipWhenEmpty) {
                return null;
            }
            
            return new String[]{
                AND, StringUtils.join(new String[]{
                    "1 = 1"
                }, " ")
            };
        }

        final StringBuilder stringBuilder = new StringBuilder();

        final int length = v.size();

        for (int i = 0; i < length; i++) {
            if (i > 0) {
                stringBuilder.append(", ");
            }

            stringBuilder.append("?");
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "NOT IN (", stringBuilder.toString(), ")"
            }, " ")
        };
    }

    public static String[] whereNull(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "IS NULL"
            }, " ")
        };
    }

    public static String[] whereNotNull(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "IS NOT NULL"
            }, " ")
        };
    }
    
    public static String[] whereTrue(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "IS TRUE"
            }, " ")
        };
    }

    public static String[] whereNotTrue(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "IS NOT TRUE"
            }, " ")
        };
    }
    
    public static String[] whereFalse(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "IS FALSE"
            }, " ")
        };
    }

    public static String[] whereNotFalse(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "IS NOT FALSE"
            }, " ")
        };
    }
    
    public static String[] whereUnknown(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "IS UNKNOWN"
            }, " ")
        };
    }

    public static String[] whereNotUnknown(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, "IS NOT UNKNOWN"
            }, " ")
        };
    }

    public static String[] where(final String c, final String operation, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (operation == null || operation.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c, operation, "?"
            }, " ")
        };
    }

    public static String[] whereMonth(final String c, final int v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "EXTRACT( MONTH FROM ", c, " )"
                }, ""), "= ?"
            }, " ")
        };
    }
    
    public static String[] whereNotMonth(final String c, final int v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "EXTRACT( MONTH FROM ", c, " )"
                }, ""), "<> ?"
            }, " ")
        };
    }

    public static String[] whereDay(final String c, final int v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "EXTRACT( DAY FROM ", c, " )"
                }, ""), "= ?"
            }, " ")
        };
    }
    
    public static String[] whereNotDay(final String c, final int v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "EXTRACT( DAY FROM ", c, " )"
                }, ""), "<> ?"
            }, " ")
        };
    }

    public static String[] whereYear(final String c, final int v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "EXTRACT( YEAR FROM ", c, " )"
                }, ""), "= ?"
            }, " ")
        };
    }
    
    public static String[] whereNotYear(final String c, final int v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "EXTRACT( YEAR FROM ", c, " )"
                }, ""), "<> ?"
            }, " ")
        };
    }

    public static String[] whereEqualColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c1, "=", c2
            }, " ")
        };
    }
    
    public static String[] whereNotEqualColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                c1, "<>", c2
            }, " ")
        };
    }

    public static String[] whereIEqualColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "UPPER(", c1, ")", "=", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] whereNotIEqualColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "UPPER(", c1, ")", "<>", "UPPER(", c2, ")"
            }, " ")
        };
    }

    public static String[] whereExists(final KQuery kQuery) throws KException {
        if (kQuery == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "EXISTS (", kQuery.toSubquery(), ")"
            }, " ")
        };
    }
    
    public static String[] whereNotExists(final KQuery kQuery) throws KException {
        if (kQuery == null) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "NOT EXISTS (", kQuery.toSubquery(), ")"
            }, " ")
        };
    }

    public static String[] where(final KWhere kWhere) {
        if (kWhere == null || kWhere.isEmpty()) {
            return null;
        }

        return new String[]{
            AND, StringUtils.join(new String[]{
                "(", kWhere.toSubquery(), ")"
            }, " ")
        };
    }

    /* OR */
    public static String[] orWhereRaw(final String c) {
        return new String[]{
            OR, c
        };
    }

    public static String[] orWhereEqual(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "=", "?"
            }, " ")
        };
    }

    public static String[] orWhereIEqual(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), "=", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotIEqual(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "NOT UPPER(", c, ")"
                }, ""), "=", "?"
            }, " ")
        };
    }

    public static String[] orWhereLike(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "LIKE", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotLike(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT", c, "LIKE", "?"
            }, " ")
        };
    }

    public static String[] orWhereILike(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), "LIKE", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotILike(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "NOT UPPER(", c, ")"
                }, ""), "LIKE", "?"
            }, " ")
        };
    }

    public static String[] orWhereNotEqual(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "<>", "?"
            }, " ")
        };
    }

    public static String[] orWhereBetween(final String c, final Object low, final Object high) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (low == null || high == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "BETWEEN ? AND ?"
            }, " ")
        };
    }

    public static String[] orWhereNotBetween(final String c, final Object low, final Object high) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (low == null || high == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "NOT BETWEEN ? AND ?"
            }, " ")
        };
    }
    
    public static String[] orWhereIBetween(final String c, final String low, final String high) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (low == null || high == null) {
            return null;
        }
        
        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), "BETWEEN ? AND ?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotIBetween(final String c, final String low, final String high) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (low == null || high == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), "NOT BETWEEN ? AND ?"
            }, " ")
        };
    }
    
    public static String[] orWhereLessThan(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "<", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereGreaterThan(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, ">", "?"
            }, " ")
        };
    }

    public static String[] orWhereIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        if (c == null || c.isEmpty()) {
            return null;
        }
        
        if (v == null) {
            return null;
        }

        if (v.isEmpty()) {
            if (skipWhenEmpty) {
                return null;
            }
            
            return new String[]{
                OR, StringUtils.join(new String[]{
                    "1 = 0"
                }, " ")
            };
        }

        final StringBuilder stringBuilder = new StringBuilder();

        final int length = v.size();

        for (int i = 0; i < length; i++) {
            if (i > 0) {
                stringBuilder.append(", ");
            }

            stringBuilder.append("?");
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "IN (", stringBuilder.toString(), ")"
            }, " ")
        };
    }

    public static String[] orWhereNotIn(final String c, final Collection v, final boolean skipWhenEmpty) {
        if (c == null || c.isEmpty()) {
            return null;
        }
        
        if (v == null) {
            return null;
        }

        if (v.isEmpty()) {
            if (skipWhenEmpty) {
                return null;
            }
            
            return new String[]{
                OR, StringUtils.join(new String[]{
                    "1 = 1"
                }, " ")
            };
        }

        final StringBuilder stringBuilder = new StringBuilder();

        final int length = v.size();

        for (int i = 0; i < length; i++) {
            if (i > 0) {
                stringBuilder.append(", ");
            }

            stringBuilder.append("?");
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "NOT IN (", stringBuilder.toString(), ")"
            }, " ")
        };
    }

    public static String[] orWhereNull(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "IS NULL"
            }, " ")
        };
    }

    public static String[] orWhereNotNull(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "IS NOT NULL"
            }, " ")
        };
    }
    
    public static String[] orWhereTrue(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "IS TRUE"
            }, " ")
        };
    }

    public static String[] orWhereNotTrue(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "IS NOT TRUE"
            }, " ")
        };
    }
    
    public static String[] orWhereFalse(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "IS FALSE"
            }, " ")
        };
    }

    public static String[] orWhereNotFalse(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "IS NOT FALSE"
            }, " ")
        };
    }
    
    public static String[] orWhereUnknown(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "IS UNKNOWN"
            }, " ")
        };
    }

    public static String[] orWhereNotUnknown(final String c) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "IS NOT UNKNOWN"
            }, " ")
        };
    }

    public static String[] orWhere(final String c, final String operation, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (operation == null || operation.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, operation, "?"
            }, " ")
        };
    }

    public static String[] orWhereMonth(final String c, final int v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "EXTRACT( MONTH FROM ", c, " )"
                }, ""), "= ?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotMonth(final String c, final int v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "EXTRACT( MONTH FROM ", c, " )"
                }, ""), "<> ?"
            }, " ")
        };
    }

    public static String[] orWhereDay(final String c, final int v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "EXTRACT( DAY FROM ", c, " )"
                }, ""), "= ?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotDay(final String c, final int v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "EXTRACT( DAY FROM ", c, " )"
                }, ""), "<> ?"
            }, " ")
        };
    }

    public static String[] orWhereYear(final String c, final int v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "EXTRACT( YEAR FROM ", c, " )"
                }, ""), "= ?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotYear(final String c, final int v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "EXTRACT( YEAR FROM ", c, " )"
                }, ""), "<> ?"
            }, " ")
        };
    }

    public static String[] orWhereEqualColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c1, "=", c2
            }, " ")
        };
    }
    
    public static String[] orWhereNotEqualColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c1, "<>", c2
            }, " ")
        };
    }
    
    public static String[] orWhereIEqualColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "UPPER(", c1, ")", "=", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] orWhereNotIEqualColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "UPPER(", c1, ")", "<>", "UPPER(", c2, ")"
            }, " ")
        };
    }

    public static String[] orWhereExists(final KQuery kQuery) throws KException {
        if (kQuery == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "EXISTS (", kQuery.toSubquery(), ")"
            }, " ")
        };
    }
    
    public static String[] orWhereNotExists(final KQuery kQuery) throws KException {
        if (kQuery == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT EXISTS (", kQuery.toSubquery(), ")"
            }, " ")
        };
    }

    public static String[] orWhere(final KWhere kWhere) {
        if (kWhere == null || kWhere.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "(", kWhere.toSubquery(), ")"
            }, " ")
        };
    }
    
    public static String[] orWhereNotGreaterThan(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT", c, ">", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotGreaterThanOrEqualTo(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT", c, ">=", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereGreaterThanOrEqualTo(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, ">=", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotLessThan(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT", c, "<", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotLessThanOrEqualTo(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT", c, "<=", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereLessThanOrEqualTo(final String c, final Object v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c, "<=", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotGreaterThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT", c1, ">", c2
            }, " ")
        };
    }
    
    public static String[] orWhereGreaterThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c1, ">", c2
            }, " ")
        };
    }
    
    public static String[] orWhereNotGreaterThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT", c1, ">=", c2
            }, " ")
        };
    }
    
    public static String[] orWhereGreaterThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c1, ">=", c2
            }, " ")
        };
    }
    
    public static String[] orWhereNotLessThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT", c1, "<", c2
            }, " ")
        };
    }
    
    public static String[] orWhereLessThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c1, "<", c2
            }, " ")
        };
    }
    
    public static String[] orWhereNotLessThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT", c1, "<=", c2
            }, " ")
        };
    }
    
    public static String[] orWhereLessThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                c1, "<=", c2
            }, " ")
        };
    }
    
    public static String[] orWhereNotIGreaterThan(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "NOT UPPER(", c, ")"
                }, ""), ">", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereIGreaterThan(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), ">", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotIGreaterThanOrEqualTo(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "NOT UPPER(", c, ")"
                }, ""), ">=", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereIGreaterThanOrEqualTo(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), ">=", "?"
            }, " ")
        };
    }

    public static String[] orWhereNotILessThan(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "NOT UPPER(", c, ")"
                }, ""), "<", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereILessThan(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), "<", "?"
            }, " ")
        };
    }
    

    public static String[] orWhereNotILessThanOrEqualTo(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "NOT UPPER(", c, ")"
                }, ""), "<=", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereILessThanOrEqualTo(final String c, final String v) {
        if (c == null || c.isEmpty()) {
            return null;
        }

        if (v == null) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                StringUtils.join(new String[]{
                    "UPPER(", c, ")"
                }, ""), "<=", "?"
            }, " ")
        };
    }
    
    public static String[] orWhereNotIGreaterThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT UPPER(", c1, ")", ">", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] orWhereIGreaterThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "UPPER(", c1, ")", ">", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] orWhereNotIGreaterThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT UPPER(", c1, ")", ">=", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] orWhereIGreaterThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "UPPER(", c1, ")", ">=", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] orWhereNotILessThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT UPPER(", c1, ")", "<", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] orWhereILessThanColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "UPPER(", c1, ")", "<", "UPPER(", c2, ")"
            }, " ")
        };
    }
    
    public static String[] orWhereNotILessThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "NOT UPPER(", c1, ")", "<=", "UPPER(", c2, ")"
            }, " ")
        };
    }
      
    public static String[] orWhereILessThanOrEqualToColumn(final String c1, final String c2) {
        if (c1 == null || c1.isEmpty()) {
            return null;
        }

        if (c2 == null || c2.isEmpty()) {
            return null;
        }

        return new String[]{
            OR, StringUtils.join(new String[]{
                "UPPER(", c1, ")", "<=", "UPPER(", c2, ")"
            }, " ")
        };
    }

    public static String joinWhere(final String[] w) {
        if (w == null) {
            return null;
        }

        if (w.length < 2) {
            return null;
        }

        if (w[0] == null || w[1] == null) {
            return null;
        }

        return StringUtils.join(new String[]{
            w[0], w[1]
        }, " ");
    }
    
//    public static List<String> getNamesAvailablesToOrder(final String c) {
//        final List<String> namesAvailablesToOrder = new ArrayList<>();
//
//        final String[] tokens = c.trim().split(" AS ");
//            
//        if (tokens.length == 2) {
//            namesAvailablesToOrder.add(tokens[0]);
//            namesAvailablesToOrder.add(tokens[1]);
//        }
//
//        if (tokens.length == 1) {
//            namesAvailablesToOrder.add(tokens[0]);
//        }
//
//        return namesAvailablesToOrder;
//    }
    
//    public static List<String> getNamesAvailablesToOrder(final KQuery kQuery) {
//        final List<String> select = kQuery.getSelect();
//
//        if (select == null || select.isEmpty()) {
//            return new ArrayList<>();
//        }
//        
//        final List<String> namesAvailablesToOrder = new ArrayList<>();
//
//        for (final String c : select) {
//            final String[] tokens = c.trim().split(" AS ");
//            
//            if (tokens.length == 2) {
//                namesAvailablesToOrder.add(tokens[0]);
//                namesAvailablesToOrder.add(tokens[1]);
//            }
//
//            if (tokens.length == 1) {
//                namesAvailablesToOrder.add(tokens[0]);
//            }
//        }
//
//        return namesAvailablesToOrder;
//    }

//    public static Map<String, Integer> getColumns(final KQuery kQuery) throws KException {
//        final Map<String, Integer> columns = new HashMap<>();
//
//        final List<String> select = kQuery.getSelect();
//
//        if (select == null || select.isEmpty()) {
//            return columns;
//        }
//
//        int i = 0;
//
//        for (final String c : select) {
//            columns.put(KLogic.extractColumnName(c), i++);
//        }
//
//        return columns;
//    }
    
    private static ArrayList<String> getTokensBeforeOf(final String[] tokens, final int k) {
        final ArrayList<String> list = new ArrayList<>();
        
        for (int i = 0; i < k; i++) {
            list.add(tokens[i]);
        }
        
        return list;
    }
    
    private static ArrayList<String> getTokensAfterOf(final String[] tokens, final int k) {
        final ArrayList<String> list = new ArrayList<>();
        
        for (int i = k + 1; i < tokens.length; i++) {
            list.add(tokens[i]);
        }
        
        return list;
    }
    
    public static SelectDataExtracted extractSelectData(final String select) throws KException {
        final String c = select.trim();
        
        if (c.length() > 1 && c.charAt(0) == '(' && c.charAt(c.length() - 1) == ')') {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "'Select: \"" + c + "\"' format not valid >> Can't start and end with parenthesis");
        }
        
        final String[] tokens = c.split(" ");
        
        if (tokens.length == 0) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "'Select' format not valid >> Can't be empty");
        }
        
        for (int i = tokens.length - 1; i >= 0; i--) {
            final String token = tokens[i];
            
            if (token.contains(")")) {
                final ArrayList namesAvailablesToOrder = new ArrayList<>();
                namesAvailablesToOrder.add(c);
                
                return new SelectDataExtracted(c, namesAvailablesToOrder);
            }
            
            final String tokenUp = tokens[i].toUpperCase();
            
            if (tokenUp.equals("AS")) {
                if (i == tokens.length - 1) {
                    throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "'Select: \"" + c + "\"' format not valid >> '" + token + "' Can't be the last word");
                }
                
                if (i == 0) {
                    throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "'Select: \"" + c + "\"' format not valid >> '" + token + "' Can't be the first word");
                }
                
                final String columnName = String.join(" ", KLogic.getTokensBeforeOf(tokens, i));
                final String aliasName = String.join(" ", KLogic.getTokensAfterOf(tokens, i));
                
                final ArrayList namesAvailablesToOrder = new ArrayList<>();
                namesAvailablesToOrder.add(columnName.trim());
                namesAvailablesToOrder.add(aliasName.trim());

                return new SelectDataExtracted(aliasName, namesAvailablesToOrder);
            }
        }
        
        final ArrayList namesAvailablesToOrder = new ArrayList<>();
        namesAvailablesToOrder.add(c);
        
        final int j = c.lastIndexOf(".");
        final String finalColumn;

        if (j != -1) {
            finalColumn = c.substring(j + 1);
        } else {
            finalColumn = c;
        }
        
        return new SelectDataExtracted(finalColumn, namesAvailablesToOrder);
    }

//    public static String extractColumnName(final String c) throws KException {
//        final String[] tokens = c.trim().split(" AS ");
//        
//        if (tokens.length == 2) {
//            return tokens[1];
//        }
//        
//        if (tokens.length == 1) {
//            if (tokens[0].contains("(") || tokens[0].contains(")")) {
//                return c;
//            }
//            
//            final int j = tokens[0].lastIndexOf(".");
//
//            if (j != -1) {
//                return tokens[0].substring(j + 1);
//            }
//            
//            return c;
//        }
//        
//        throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, StringUtils.join(new String[]{
//            "'", c, "' Format not valid!"
//        }, " "));
//    }

//    public static <T> T insert(final KModel kModel, final EntityManager entityManager) {
//        final KContext kContext = new KContext();
//        
//        final ExtractClazz extractClazz = new ExtractClazz();
//        
//        final String ql = KLogic.buildInsertInto(kModel, kContext, true, extractClazz);
//
//        final Query query = entityManager.createNativeQuery(ql);
//
//        int i = 1;
//
//        for (final Object o : kContext.getParams()) {
//            query.setParameter(i++, o);
//        }
//        
//        final Object value = query.getSingleResult();
//        
//        return (T) KQuery.castTo(value, extractClazz.getClazz());
//    }
    
//    public static void insertNoReturn(final KModel kModel, final EntityManager entityManager) {
//        final KContext kContext = new KContext();
//        
//        final String ql = KLogic.buildInsertInto(kModel, kContext, false, null);
//
//        final Query query = entityManager.createNativeQuery(ql);
//
//        int i = 1;
//
//        for (final Object o : kContext.getParams()) {
//            query.setParameter(i++, o);
//        }
//        
//        query.executeUpdate();
//    }
    
//    public static void ddInsert(final KModel kModel) {
//        final KContext kContext = new KContext();
//
//        final ExtractClazz extractClazz = new ExtractClazz();
//        
//        System.out.println(KLogic.buildInsertInto(kModel, kContext, true, extractClazz));
//        
//        int i = 1;
//        
//        System.out.println("Parámetros: >>>>");
//        for (final Object o : kContext.getParams()) {
//            
//            System.out.println(StringUtils.join(new String[]{
//                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
//            }, " "));
//
//            i++;
//        }
//    }
    
//    public static void ddInsertNoReturn(final KModel kModel) {
//        final KContext kContext = new KContext();
//
//        System.out.println(KLogic.buildInsertInto(kModel, kContext, false, null));
//        
//        int i = 1;
//        
//        System.out.println("Parámetros: >>>>");
//        for (final Object o : kContext.getParams()) {
//            
//            System.out.println(StringUtils.join(new String[]{
//                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
//            }, " "));
//
//            i++;
//        }
//    }

//    public static void updateById(final KModel kModel, final EntityManager entityManager) throws KException {
//        final KContext kContext = new KContext();
//
//        final String ql = KLogic.buildUpdateSetById(kModel, kContext);
//
//        final Query query = entityManager.createNativeQuery(ql);
//
//        int i = 1;
//
//        for (final Object o : kContext.getParams()) {
//            query.setParameter(i++, o);
//        }
//
//        query.executeUpdate();
//    }
    
//    public static void ddUpdateById(final KModel kModel) throws KException {
//        final KContext kContext = new KContext();
//        
//        System.out.println(KLogic.buildUpdateSetById(kModel, kContext));
//        
//        int i = 1;
//        
//        System.out.println("Parámetros: >>>>");
//        for (final Object o : kContext.getParams()) {
//            
//            System.out.println(StringUtils.join(new String[]{
//                "Parámetro '", String.valueOf(i), "':", "> ", o.getClass().getName(), ", Valor: ", o.toString()
//            }, " "));
//
//            i++;
//        }
//    }

//    public static String buildInsertInto(final KModel kModel, final KContext kContext, final boolean returning, final ExtractClazz extractClazz) {
//        String returningField = null;
//        
//        final Class myClass = kModel.getType();
//
//        final Table table = (Table) myClass.getAnnotation(Table.class);
//
//        final Field[] fields = myClass.getDeclaredFields();
//
//        final StringBuilder stringBuilderColumn = new StringBuilder();
//        final StringBuilder stringBuilderValues = new StringBuilder();
//
//        boolean first = true;
//
//        for (final Field field : fields) {
//            final Column column = field.getAnnotation(Column.class);
//
//            if (column == null) {
//                if (!returning) {
//                    continue;
//                }
//                
//                final Id id = field.getAnnotation(Id.class);
//                
//                if (id == null) {
//                    continue;
//                }
//                
//                returningField = id.name();
//                extractClazz.setClazz(id.clazz());
//                
//                continue;
//            }
//
//            try {
//                field.setAccessible(true);
//
//                final Object o = field.get(kModel);
//
//                if (o == null) {
//                    continue;
//                }
//
//                kContext.addParam(o);
//
//                if (first) {
//                    first = false;
//                } else {
//                    stringBuilderColumn.append(", ");
//                    stringBuilderValues.append(", ");
//                }
//
//                stringBuilderColumn.append(column.name());
//                stringBuilderValues.append("?");
//            } catch (Exception e) {
//                //NOTHING
//            }
//        }
//
//        final String returningClause = (returning) ? "RETURNING " + returningField : "";
//
//        return StringUtils.join(new String[]{
//            "INSERT INTO", table.name(), "(", stringBuilderColumn.toString(), ") VALUES (", stringBuilderValues.toString(), ")", returningClause
//        }, " ");
//    }
    
    public static String buildInsertInto(final KQuery kQuery, final Map<String, Object> d) {
        return buildInsertInto(kQuery, d, true);
    }
    
    private static String buildInsertInto(final KQuery kQuery, final Map<String, Object> d, final boolean returning) {
        return buildInsertInto(kQuery, d, new HashMap<>(), returning, "id");
    }
    
    public static String buildInsertInto(final KQuery kQuery, final Map<String, Object> d, final Map<String, String> processors) {
        return buildInsertInto(kQuery, d, processors, true);
    }
    
    public static String buildInsertInto(final KQuery kQuery, final Map<String, Object> d, final Map<String, String> processors, final boolean returning) {
        return buildInsertInto(kQuery, d, processors, returning, "id");
    }
    
    public static String buildInsertInto(
        final KQuery kQuery, 
        final Map<String, Object> d, 
        final Map<String, String> processors, 
        final boolean returning, 
        final String returningField
    ) {
        kQuery.generateNewTableName();
        
        final StringBuilder stringBuilderColumn = new StringBuilder();
        final StringBuilder stringBuilderValues = new StringBuilder();

        boolean first = true;
        int i = 0;

        for (final Map.Entry<String, Object> entry : d.entrySet()) {
            if (first) {
                first = false;
            } else {
                stringBuilderColumn.append(", ");
                stringBuilderValues.append(", ");
            }

            stringBuilderColumn.append(entry.getKey());

            stringBuilderValues.append((entry.getValue() == null) ? "NULL" : KLogic.getParamQuery(entry.getKey(), processors));

            if (entry.getValue() != null) {
                kQuery.getkContext().addParam(i++, entry.getValue());
            }
        }
        
        final String onConflictDoNothingClause = (kQuery.getOnConflictDoNothing()) ? "ON CONFLICT DO NOTHING" : "";
        final String returningClause = (returning) ? "RETURNING " + returningField : "";

        return StringUtils.join(new String[]{
            "INSERT INTO", kQuery.getTable(), "(", stringBuilderColumn.toString(), ") VALUES (", stringBuilderValues.toString(), ")", onConflictDoNothingClause, returningClause
        }, " ");
    }
    
    public static String buildMultipleInsertInto(final KQuery kQuery, final List<String> columns, final List<HashMap<String, Object>> d) {
        return buildMultipleInsertInto(kQuery, columns, d, new HashMap<>(), true, "id");
    }
    
    public static String buildMultipleInsertInto(final KQuery kQuery, final List<String> columns, final List<HashMap<String, Object>> d, final HashMap<String, String> processors) {
        return buildMultipleInsertInto(kQuery, columns, d, processors, true, "id");
    }
    
    public static String buildMultipleInsertInto(final KQuery kQuery, final List<String> columns, final List<HashMap<String, Object>> d, final HashMap<String, String> processors, final boolean returning) {
        return buildMultipleInsertInto(kQuery, columns, d, processors, returning, "id");
    }
    
    public static String buildMultipleInsertInto(final KQuery kQuery, final List<String> columns, final List<HashMap<String, Object>> d, final boolean returning) {
        return buildMultipleInsertInto(kQuery, columns, d, new HashMap<>(), returning, "id");
    }

    public static String buildMultipleInsertInto(
        final KQuery kQuery, 
        final List<String> columns, 
        final List<HashMap<String, Object>> d, 
        final HashMap<String, String> processors, 
        final boolean returning,
        final String returningField
    ) {
        kQuery.generateNewTableName();
        
        final StringBuilder stringBuilderColumn = new StringBuilder();

        boolean first = true;

        for (final String column : columns) {
            if (first) {
                first = false;
            } else {
                stringBuilderColumn.append(", ");
            }

            stringBuilderColumn.append(column);
        }

        final StringBuilder stringBuilderValues = new StringBuilder();

        first = true;

        for (final HashMap<String, Object> object : d) {
            if (first) {
                first = false;
            } else {
                stringBuilderValues.append(", ");
            }

            stringBuilderValues.append("(");

            boolean firstColumn = true;

            for (final String column : columns) {
                final Object value = object.get(column);

                if (firstColumn) {
                    firstColumn = false;
                } else {
                    stringBuilderValues.append(", ");
                }

                stringBuilderValues.append((value == null) ? "NULL" : KLogic.getParamQuery(column, processors));

                if (value != null) {
                    kQuery.getkContext().addParam(value);
                }
            }

            stringBuilderValues.append(")");
        }

        final String onConflictDoNothingClause = (kQuery.getOnConflictDoNothing()) ? "ON CONFLICT DO NOTHING" : "";
        final String returningClause = (returning) ? "RETURNING " + returningField : "";

        return StringUtils.join(new String[]{
            "INSERT INTO", kQuery.getTable(), "(", stringBuilderColumn.toString(), ") VALUES", stringBuilderValues.toString(), onConflictDoNothingClause, returningClause
        }, " ");
    }
    
    public static String buildInsertIntoFrom(final KQuery kQuery) throws KException {
        kQuery.generateNewTableName();
        
        final String select = KLogic.buildSelect(kQuery);
        final String from = KLogic.buildClauseFromInsertInto(kQuery);
        final String where = KLogic.buildWhere(kQuery);
        
        final StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(select);

        if (from != null && !from.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(from);
        }

        if (where != null && !where.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(where);
        }

        if (kQuery.getTable() == null) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "A 'insert into' table is required");
        }
        
        final List<String> insertInto = kQuery.getInsertInto();
        
        if (insertInto == null || insertInto.isEmpty()) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "At least one 'insert into' column is required");
        }
        
        return StringUtils.join(new String[]{
            "INSERT INTO", kQuery.getTable(), "(", StringUtils.join(insertInto, ", "), ")", stringBuilder.toString() 
        }, " ");
    }
    
    private static String buildClauseFromInsertInto(final KQuery kQuery) throws KException {
        final List<String> froms = kQuery.getFrom();
        
        if (froms == null) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "A 'from' table is required");
        }
        
        if (froms.size() > 1) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "A single 'from' table is required");
        }
        
        final String from = froms.get(0);

        if (from == null || from.isEmpty()) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "A 'from' table is required");
        }

        final String join = buildJoin(kQuery);

        if (join == null || join.isEmpty()) {
            return StringUtils.join(new String[]{
                "FROM", from
            }, " ");
        }

        return StringUtils.join(new String[]{
            "FROM", from, join
        }, " ");
    }

//    public static String buildUpdateSetById(final KModel kModel, final KContext kContext) throws KException {
//        final Class myClass = kModel.getType();
//        final List<String> fieldsToUpdate = kModel.getFieldsToUpdate();
//
//        final Table table = (Table) myClass.getAnnotation(Table.class);
//
//        final Field[] fields = myClass.getDeclaredFields();
//
//        final StringBuilder stringBuilder = new StringBuilder();
//
//        boolean first = true;
//
//        String idValue = "0";
//        String idFieldName = null;
//
//        for (final Field field : fields) {
//            final Column column = field.getAnnotation(Column.class);
//
//            if (column == null) {
//                try {
//                    final Id id = field.getAnnotation(Id.class);
//                    
//                    if (id == null) {
//                        continue;
//                    }
//                    
//                    field.setAccessible(true);
//                    
//                    idValue = field.get(kModel).toString();
//                    
//                    idFieldName = id.name();
//                    
//                    continue;
//                } catch (Exception e) {
//                    throw KExceptionHelper.internalServerError("An error occurred while detecting the object id");
//                }
//            }
//
//            final String columnName = column.name();
//
//            if (!fieldsToUpdate.contains(columnName)) {
//                continue;
//            }
//
//            try {
//                field.setAccessible(true);
//
//                final Object o = field.get(kModel);
//
//                if (first) {
//                    first = false;
//                } else {
//                    stringBuilder.append(", ");
//                }
//
//                stringBuilder.append(columnName);
//
//                if (o == null) {
//                    stringBuilder.append(" = NULL");
//                } else {
//                    stringBuilder.append(" = ?");
//
//                    kContext.addParam(o);
//                }
//            } catch (Exception e) {
//                //NOTHING
//            }
//        }
//
//        return StringUtils.join(new String[]{
//            "UPDATE", table.name(), "SET", stringBuilder.toString(), "WHERE", idFieldName, "=", idValue
//        }, " ");
//    }
    
    public static String buildUpdateSet(final KQuery kQuery, final Map<String, Object> d) {
        return KLogic.buildUpdateSet(kQuery, d, new HashMap<>());
    }
    
    public static String buildUpdateSet(final KQuery kQuery, final List<Map<String, Object>> fromValues, final String[] fromColumns, final Map<String, Object> d) {
        return KLogic.buildUpdateSet(kQuery, fromValues, fromColumns, d, new HashMap<>());
    }

    public static String buildUpdateSet(final KQuery kQuery, final Map<String, Object> d, final Map<String, String> processors) {
        kQuery.generateNewTableNameWithAlias();
        
        final StringBuilder stringBuilder = new StringBuilder("");

        boolean first = true;
        int i = 0;

        for (Map.Entry<String, Object> entry : d.entrySet()) {
            if (first) {
                first = false;
            } else {
                stringBuilder.append(", ");
            }

            if (entry.getValue() instanceof KRaw) {
                stringBuilder.append(StringUtils.join(new String[]{
                    entry.getKey(), "=", ((KRaw) entry.getValue()).getRaw()
                }, " "));
            } else {
                stringBuilder.append(StringUtils.join(new String[]{
                    entry.getKey(), "=", (entry.getValue() == null) ? "NULL" : KLogic.getParamQuery(entry.getKey(), processors)
                }, " "));

                if (entry.getValue() != null) {
                    kQuery.getkContext().addParam(i++, entry.getValue());
                }
            }
        }
        
        final String from = KLogic.buildUpdateFrom(kQuery);
        
        if (from != null && !from.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(from);
        }

        final String where = KLogic.buildWhere(kQuery);

        if (where != null && !where.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(where);
        }

        return StringUtils.join(new String[]{
            "UPDATE", kQuery.getTable(), "SET", stringBuilder.toString()
        }, " ");
    }
    
    public static String buildUpdateSet(final KQuery kQuery, final List<Map<String, Object>> fromValues, final String[] fromColumns, final Map<String, Object> d, final Map<String, String> processors) {
        kQuery.generateNewTableNameWithAlias();
        
        final StringBuilder stringBuilder = new StringBuilder("");

        boolean first = true;
        int i = 0;

        for (Map.Entry<String, Object> entry : d.entrySet()) {
            if (first) {
                first = false;
            } else {
                stringBuilder.append(", ");
            }

            if (entry.getValue() instanceof KRaw) {
                stringBuilder.append(StringUtils.join(new String[]{
                    entry.getKey(), "=", ((KRaw) entry.getValue()).getRaw()
                }, " "));
            } else {
                stringBuilder.append(StringUtils.join(new String[]{
                    entry.getKey(), "=", (entry.getValue() == null) ? "NULL" : KLogic.getParamQuery(entry.getKey(), processors)
                }, " "));

                if (entry.getValue() != null) {
                    kQuery.getkContext().addParam(i++, entry.getValue());
                }
            }
        }
        
        final String from = KLogic.buildUpdateFrom(kQuery, fromValues, fromColumns, i);
        
        stringBuilder.append(" ");
        stringBuilder.append(from);
        
        final String where = KLogic.buildWhere(kQuery);

        if (where != null && !where.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(where);
        }

        return StringUtils.join(new String[]{
            "UPDATE", kQuery.getTable(), "SET", stringBuilder.toString()
        }, " ");
    }

    public static String getParamQuery(final String key, final Map<String, String> processors) {
        final String processor = processors.get(key);

        return (processor == null) ? "?" : processor;
    }

    public static String buildDeleteFrom(final KQuery kQuery) {
        kQuery.generateNewTableNameWithAlias();
        
        final String where = KLogic.buildWhere(kQuery);
        final String using = KLogic.buildUsing(kQuery);

        final StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("DELETE FROM ");
        stringBuilder.append(kQuery.getTable());

        if (using != null && !using.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(using);
        }

        if (where != null && !where.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(where);
        }

        return stringBuilder.toString();
    }

    public static String buildTruncate(final KQuery kQuery, final boolean restartIdentity, final boolean cascade) {
        if (restartIdentity && cascade) {
            return StringUtils.join(new String[]{
                "TRUNCATE TABLE", kQuery.getTable(), "RESTART IDENTITY", "CASCADE"
            }, " ");
        }

        if (restartIdentity) {
            return StringUtils.join(new String[]{
                "TRUNCATE TABLE", kQuery.getTable(), "RESTART IDENTITY"
            }, " ");
        }

        if (cascade) {
            return StringUtils.join(new String[]{
                "TRUNCATE TABLE", kQuery.getTable(), "CASCADE"
            }, " ");
        }

        return StringUtils.join(new String[]{
            "TRUNCATE TABLE", kQuery.getTable()
        }, " ");
    }
    
    public static String with(final String tableName, final List<String> columns, final List<Map<String, Object>> values) {
        final StringBuilder stringBuilderValues = new StringBuilder();
        final StringBuilder stringBuilderColumns = new StringBuilder();
        
        stringBuilderColumns.append("(");
        
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                stringBuilderColumns.append(", ");
            }
            
            stringBuilderColumns.append(columns.get(i));
        }
        
        stringBuilderColumns.append(")");
        
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                stringBuilderValues.append(", ");
            }
            
            stringBuilderValues.append("(");
            
            for (int j = 0; j < columns.size(); j++) {
                if (j > 0) {
                    stringBuilderValues.append(", ");
                }
                
                final Object value = values.get(i).get(columns.get(j));
                
                stringBuilderValues.append(value != null ? "?" : "NULL");
            }
            
            stringBuilderValues.append(")");
        }
        
        return StringUtils.join(new String[]{
            tableName, stringBuilderColumns.toString(), "AS (VALUES ", stringBuilderValues.toString(), ")"
        }, " ");
    }
    
    public static String generateBoolAndClause(final KQuery kQuery, final String property) {
        kQuery.generateNewTableNameWithAlias();
        
        kQuery.whereEqualColumn(property, "ktc__._x");
        
        final String select = "SELECT 1";
        final String from = KLogic.buildFrom(kQuery);
        final String where = KLogic.buildWhere(kQuery);
        final String groupBy = KLogic.buildGroupBy(kQuery);
        final String having = KLogic.buildHaving(kQuery);
        final String orderBy = KLogic.buildOrderBy(kQuery);
        final String limit = KLogic.buildLimit(kQuery);
        final String offset = KLogic.buildOffset(kQuery);

        final StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(select);

        if (from != null && !from.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(from);
        }

        if (where != null && !where.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(where);
        }

        if (groupBy != null && !groupBy.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(groupBy);
        }

        if (having != null && !having.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(having);
        }

        if (orderBy != null && !orderBy.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(orderBy);
        }

        if (limit != null && !limit.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(limit);
        }

        if (offset != null && !offset.isEmpty()) {
            stringBuilder.append(" ");
            stringBuilder.append(offset);
        }
        
        return StringUtils.join(new String[]{
            "SELECT BOOL_AND(EXISTS (", stringBuilder.toString(), ")) FROM k_to_check__ ktc__"
        }, "");
    }
    
    public static String buildSelectDistinctOn(final String c, final String alias) {
        return StringUtils.join(new String[]{
            "DISTINCT ON (", c, ") ", alias
        }, "");
    }
}