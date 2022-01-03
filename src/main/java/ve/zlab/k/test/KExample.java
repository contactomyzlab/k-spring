package ve.zlab.k.test;

import ve.zlab.k.KException;
import ve.zlab.k.KExecutor;

public class KExample {
    
    public void main() throws KException {
        final KExecutor K = KTest.K();
        
        long startTime = System.currentTimeMillis();
        
//        new MyBookDTO().
//            setName("ALICIA EN EL >>>>").
//            toKQuery(K).
//            insert();
//        
//        K.
//        table(MyBook.class).
//        leftJoin(MyBook.withMyCustomer()).
//        select(
//            MyBook.ID,
//            MyCustomer.ID("customerId"),
//            MyCustomer.LAST_NAME("customerLastName")
//        ).
//        where(MyCustomer.ID, 7L).
//        whereGreaterThan(MyBook.ID, 2L).
//        whereILikeAny(MyCustomer.LAST_NAME, "JHON").
//        where(MyBook.NAME, "ads").
//        dd();
//        
//        K.
//        table("my_book mb").
//        leftJoin("my_customer mc", "mb.my_customer_id", "mc.id").
//        select(
//            "mb.id",
//            "mc.id AS customerId",
//            "mc.last_name AS customerLastName"
//        ).
//        where("mc.id", 7L).
//        whereGreaterThan("mc.id", 2L).
//        whereILikeAny("mc.last_name", "JHON").
//        dd();
        
//        K.
//        table(MyBook.class).
//        insertInto(MyBookDTO.MY_CUSTOMER_ID, "current_date_value", "date_day", "hour_from", "hour_to", "military_time", "reason", "postponed", "specialty_id", "doctor_id", "patient_id", "beneficiary_id").
//        select(MyBook.MY_CUSTOMER_ID, "da.current_date_value", "da.date_day", "s.hour_from", "s.hour_to", "s.military_time", "s.reason", "s.postponed", "s.specialty_id", "da.doctor_id", "s.patient_id", "s.beneficiary_id").
//        from(MyCustomer.class).
//        innerJoin("day_available da", "da.id", "s.day_available_id").
//        where("s.id", 1L).
//        ddInsertIntoFrom();

//        K.
//        table(MyBook.class).        
//        innerJoin(MyBook.withMyCustomer()).
//        select(
//            String.format("CAST (%s AS TEXT) AS %s", MyBook.ID, "theId"),
////            "CAST (mc.id AS TEXT) AS theId",
//            "mc.last_name AS lastName"
//        ).
//        dd();

//        new MyCustomerDTO().
//            setName("Cristiano").
//            setLastName("Ronaldo").
//            toKQuery(K).
//            whereBetween("id", 7L, 8L).
//            ddUpdate();
        
//        new MyCustomer().
//            setName("Cristiano").
//            setLastName("Ronaldo").
//            toKQuery(K).
//            whereBetween("id", 7L, 8L).
//            ddDelete();
        
//        new MyCustomer().
//            setName("Cristiano").
//            setLastName("Ronaldo").
//            toKQuery(K).
//            ddInsert();
        



//final Integer myMax = row.getInteger("max");

//        K.
//        table("app_user au").
//        select(
//            "au.id",
//            "au.last_name AS lastName"
//        ).
//        where("au.name", "Jhon").
//        dd();

//        K.table("app_user u").
//            select("u.id").
//            select("u.full_name", "r.name").
//            leftJoin("file f", "u.file_id", "f.id").
//            leftJoinSub(new KQuery("project p").
//                    select("p.name", "p.created_by"), "ap", new KJoin() {
//                @Override
//                public KJoin execute(final KJoin kJoin) {
//                    return kJoin.on("ap.created_by", "u.id");
//                }
//            }).
//            joinSub(new KQuery("project p2").
//                    select("p2.name", "p2.created_by"), "ap2", "ap2.created_by", "u.id").
//            join("role r", new KJoin() {
//                @Override
//                public KJoin execute(final KJoin kJoin) {
//                    return kJoin.on("u.role_id", "r.id").whereEqualColumn("u.created_at", "u.updated_at").where("u.password", "=", 1);
//                }
//            }).
//            join("app_user cb", new KJoin() {
//                @Override
//                public KJoin execute(final KJoin kJoin) {
//                    return kJoin.on("u.created_by", "cb.id").where("u.password", "=", 45);
//                }
//            }).
//            where("u.password", "=", "3").
//            whereEqualColumn("u.created_at", "u.updated_at").
//            orWhereNull("u.password").
//            where(new KWhere() {
//
//                @Override
//                public KWhere execute(final KWhere kWhere) throws KException {
//                    return kWhere.
//                            orWhereEqualColumn("u.created_at", "u.updated_at").
//                            orWhereRaw("u.password = '123'").
//                            orWhere(new KWhere() {
//
//                                @Override
//                                public KWhere execute(KWhere kWhere) throws KException {
//                                    return kWhere.
//                                            whereExists(new KQuery("app_user u1").select("1").whereNull("u1.password").where("u1.id", ">", 8).distinct());
//                                }
//                            });
//                }
//            }).
//            groupBy("u.id").
//            having("u.password", "=", "7").
//            orHavingRaw("u.password = '123'").
//            orHaving("u.password", "=", "77").
//            havingRaw("u.password = '123'").
//            orHaving(new KWhere() {
//
//                @Override
//                public KWhere execute(final KWhere kWhere) throws KException {
//                    return kWhere.
//                            orWhereEqualColumn("u.created_at", "u.updated_at").
//                            orWhereRaw("u.password = '123'").
//                            orWhere(new KWhere() {
//
//                                @Override
//                                public KWhere execute(KWhere kWhere) throws KException {
//                                    return kWhere.
//                                            whereExists(new KQuery("app_user u1").select("1").whereNull("u1.password").where("u1.id", ">", 10).distinct());
//                                }
//                            });
//                }
//            }).
//            //                orderBy("u.id ASC").
//            page(5L).
//            limit(10L).
//            dd();
        
        long endTime = System.currentTimeMillis();

        System.out.println("That took " + (endTime - startTime) + " milliseconds");
        
        /**/
//        final KRow k1 = K.
//                table("app_users").
//                select("name", "id").
//                single();
//
//        final String name = k1.getString("name");
//        final Long id = k1.getLong(1);
//        final BigDecimal price = k1.getBigDecimal("price_with_tax");
        /**/
//        final KRow k2 = K.
//                table("app_users").
//                single();
        /**/
//        final KCollection k3 = K.
//                table("app_users u").
//                join("role r", "u.role_id", "r.id").
//                distinct().
//                whereEqual("full_name", "John").
//                orderBy("id").multiple();

//        for (KRow krow : k3.getCollection()) {
//
//        }
        /**/
//        final KRow k4 = K.
//                table("app_users").
//                find(3L);
        /**/
//        final KRow k5 = K.
//                table("app_users").
//                max("price");
//        final Long maxPrice = k5.getBigDecimal(0).longValue();
        /**/
//        final KCollection k6 = K.
//                table("app_users").
//                whereEqual("full_name", "John").
//                multiple();
//        final List<String> names = k6.pluckString("full_name");
        /**/
//        final KQuery kQuery = K.
//                table("app_users").
//                whereEqual("full_name", "John");
//        final boolean exists = kQuery.exists().getBoolean(0);
        /**/
//        final KCollection k7 = K.
//                table("app_users").
//                select("COUNT(*) AS user_count", "status").
//                whereNotEqual("status", 1).
//                whereRaw("price > 0").
//                groupBy("status").
//                havingRaw("COUNT(*) > 3").
//                multiple();
        /**/
//        final KCollection k8 = K.
//                table("app_users u").
//                leftJoin("role r", "u.role_id", "r.id").
//                rightJoin("permission_authorized pa", "pa.role_id", "r.id").
//                join("permission p", "pa.permission_id", "p.id").
//                multiple();
        /**/
//        final KCollection k9 = K.
//                table("app_users u").
//                join("role r", new KJoin() {
//                    @Override
//                    public KJoin execute(final KJoin kJoin) {
//                        return kJoin.on("u.role_id", "r.id").whereEqual("r.id", 9);
//                    }
//                }).
//                leftJoin("role r", new KJoin() {
//                    @Override
//                    public KJoin execute(final KJoin kJoin) {
//                        return kJoin.on("u.role_id", "r.id").whereNull("r.last_user");
//                    }
//                }).
//                rightJoin("role r", new KJoin() {
//                    @Override
//                    public KJoin execute(final KJoin kJoin) {
//                        return kJoin.on("u.role_id", "r.id");
//                    }
//                }).multiple();
        /**/
//        final KQuery kQueryPosts = K.table("posts").
//                select("user_id", "MAX(created_at) as last_post_created_at").
//                whereEqual("is_published", true).
//                groupBy("user_id");
//
//        final KCollection k10 = K.
//                table("app_users u").
//                joinSub(kQueryPosts, "latest_posts p", new KJoin() {
//                    @Override
//                    public KJoin execute(final KJoin kJoin) {
//                        return kJoin.on("u.id", "p.user_id");
//                    }
//                }).multiple();
        /**/
//        final KQuery kQueryPassword = K.
//                table("app_users u").
//                where("password", "like", "123%").
//                orWhere("password", "like", "456%");
//
//        final KCollection k11 = K.
//                table("app_users u").
//                whereNull("last_name").
//                whereNotNull("first_name").
//                union(kQueryPassword).
//                multiple();
        /**/
//        final KCollection k12 = K.
//                table("app_users u").
//                whereBetween("id", 1, 100).
//                whereNotBetween("id", 5, 7).
//                multiple();
        /**/
//        final KCollection k13 = K.
//                table("app_users u").
//                whereIn("id", new Object[] {1, 2, 3, 4, 5, 100}).
//                whereNotIn("id", new Object[] {1, 2, 3, 4, 5, 100}).
//                multiple();
        /**/
//        final KCollection k14 = K.
//                table("app_users u").
//                whereIn("id", new Object[] {1, 2, 3, 4, 5, 100}).
//                whereNotIn("id", new Object[] {1, 2, 3, 4, 5, 100}).
//                where(new KWhere() {
//                    @Override
//                    public KWhere execute(final KWhere kWhere) {
//                        return kWhere.whereBetween("id", 1, 7).whereNull("password");
//                    }
//                }).
//                orWhere(new KWhere() {
//                    @Override
//                    public KWhere execute(final KWhere kWhere) {
//                        return kWhere.whereBetween("id", 1, 7).whereNull("password");
//                    }
//                }).
//                latest().
//                multiple();
        /**/
//        K.table("app_users").insert(new HashMap[] {
//            new HashMap<String, Object>() {{
//                put("password", "123");
//                put("name", "Jean");
//                put("age", 2);
//            }},
//            new HashMap<String, Object>() {{
//                put("password", "123");
//                put("name", "Jean");
//                put("age", 2);
//            }},
//            new HashMap<String, Object>() {{
//                put("password", "123");
//                put("name", "Jean");
//                put("age", 2);
//            }}
//        });
        /**/
//        K.table("app_users").insertOrIgnore(new HashMap[] {
//            new HashMap<String, Object>() {{
//                put("password", "123");
//                put("name", "Jean");
//                put("age", 2);
//            }},
//            new HashMap<String, Object>() {{
//                put("password", "123");
//                put("name", "Jean");
//                put("age", 2);
//            }},
//            new HashMap<String, Object>() {{
//                put("password", "123");
//                put("name", "Jean");
//                put("age", 2);
//            }}
//        }, new HashMap<String, String>() {{
//            put("name", "UPPERCASE(?)");
//        }});
        /**/
//        final Long id1 = K.table("app_users").insert(
//            new HashMap<String, Object>() {{
//                put("password", "123");
//                put("name", "Jean");
//                put("age", 2);
//            }});
        /**/
//        final Long id2 = K.table("app_users").insertGetId(
//            new HashMap<String, Object>() {{
//                put("password", "123");
//                put("name", "Jean");
//                put("age", 2);
//            }},new HashMap<String, String>() {{
//                put("password", "ENCRYPT(?)");
//            }});
        /**/
//        K.table("app_users").
//            whereEqual("id", 1).
//            update(new HashMap<String, Object>() {{
//                put("password", "123");
//                put("name", "Jean");
//                put("age", 2);
//            }},new HashMap<String, String>() {{
//                put("password", "ENCRYPT(?)");
//            }});
        /**/
//        K.table("app_users").
//                delete();
        /**/
//        K.table("app_users").
//                whereEqual("id", 1).
//                delete();
//            K.table("erm_column c").
//                    using("erm_edge e").
//                    using("erm_table t").
//                    whereColumn("c.id", "=", "e.erm_column_id").
//                    whereColumn("e.src_erm_table_id", "=", "t.id").
//                whereEqual("t.id", 999).
//                delete();
        /**/
//        K.table("app_users").
//                truncate();
    }

    public static void main(String args[]) throws KException {
        new KExample().main();
    }
}