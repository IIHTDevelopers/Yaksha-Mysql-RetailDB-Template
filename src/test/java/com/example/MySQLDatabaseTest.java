package com.example;

import org.junit.jupiter.api.*;
import java.sql.*;

import java.util.*;

import static com.example.utils.TestUtils.businessTestFile;
import static com.example.utils.TestUtils.currentTest;
import static com.example.utils.TestUtils.testReport;
import static com.example.utils.TestUtils.yakshaAssert;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MySQLDatabaseTest {

    static Connection conn;

    @BeforeAll
    public static void init() throws Exception {
        try {
            conn = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/RetailDB?serverTimezone=UTC",
                "root",
                "pass@word1"
            );
            System.out.println("RetailDB connection OK");
        } catch (Exception ex) {
            System.out.println("DB not found. Please create RetailDB and run DDL/DML first.");
        }
    }

    // ---------------------- Helpers ----------------------

    private boolean schemaExists(String schema) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean tableExists(String table) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, table, null)) {
            return rs.next();
        }
    }

    private boolean viewExists(String view) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'RetailDB' AND TABLE_NAME = ?")) {
            ps.setString(1, view);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean routineExists(String name) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'RetailDB' AND ROUTINE_NAME = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private String getRoutineBody(String name) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'RetailDB' AND ROUTINE_NAME = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString(1) == null ? "" : rs.getString(1).toUpperCase();
            }
        }
        return "";
    }

    private boolean triggerExists(String triggerName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA = 'RetailDB' AND TRIGGER_NAME = ?")) {
            ps.setString(1, triggerName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean indexExists(String table, String index) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='RetailDB' AND TABLE_NAME=? AND INDEX_NAME=?")) {
            ps.setString(1, table);
            ps.setString(2, index);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean rowExists(String sql, Object... params) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) ps.setObject(i + 1, params[i]);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean callProcedure(String callSyntax) {
        try (CallableStatement cs = conn.prepareCall(callSyntax)) {
            boolean hasResults = cs.execute();
            if (hasResults) return true;
            if (cs.getUpdateCount() != -1) return true;
            while (cs.getMoreResults() || cs.getUpdateCount() != -1) { /* consume */ }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ---------------------- Tests ----------------------

    @Test
    @Order(1)
    public void testDatabaseExists() throws Exception {
        try {
            boolean exists = (conn != null) && schemaExists("RetailDB");
            yakshaAssert(currentTest(), exists, businessTestFile);
        } catch (Exception ex) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    @Order(2)
    public void testTablesExist() throws Exception {
        try {
            List<String> expectedTables = Arrays.asList(
                "Customers","Products","Stores","Inventory","Orders","OrderItems"
            );
            boolean all = true;
            for (String t : expectedTables) if (!tableExists(t)) { all = false; break; }
            yakshaAssert(currentTest(), all, businessTestFile);
        } catch (Exception ex) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    @Order(3)
    public void testSeedDataExists() throws Exception {
        try {
            boolean ok =
                // Customers
                rowExists("SELECT 1 FROM Customers WHERE FullName=? AND Email=? AND JoinDate=?",
                        "Aisha Verma","aisha@example.com", java.sql.Date.valueOf("2024-07-10")) &&
                rowExists("SELECT 1 FROM Customers WHERE FullName=? AND Email=? AND JoinDate=?",
                        "Rahul Mehta","rahul@example.com", java.sql.Date.valueOf("2024-08-02")) &&
                rowExists("SELECT 1 FROM Customers WHERE FullName=? AND Email IS NULL AND JoinDate=?",
                        "Priya Shah", java.sql.Date.valueOf("2024-09-15")) &&

                // Products
                rowExists("SELECT 1 FROM Products WHERE SKU=? AND ProductName=? AND Category=? AND UnitPrice=? AND Active=1",
                        "SKU-TEE-001","Classic Tee","Apparel", 499.00) &&
                rowExists("SELECT 1 FROM Products WHERE SKU=? AND ProductName=? AND Category=? AND UnitPrice=? AND Active=1",
                        "SKU-MUG-101","Ceramic Mug","Home", 299.00) &&
                rowExists("SELECT 1 FROM Products WHERE SKU=? AND ProductName=? AND Category=? AND UnitPrice=? AND Active=1",
                        "SKU-NB-210","A5 Notebook","Stationery", 149.00) &&
                rowExists("SELECT 1 FROM Products WHERE SKU=? AND Active=0",
                        "SKU-BAG-505") &&

                // Stores
                rowExists("SELECT 1 FROM Stores WHERE StoreName=? AND City=? AND State=?",
                        "Downtown Outlet","Mumbai","MH") &&
                rowExists("SELECT 1 FROM Stores WHERE StoreName=? AND City=? AND State=?",
                        "City Center","Pune","MH") &&

                // Inventory
                rowExists("SELECT 1 FROM Inventory WHERE StoreID=1 AND ProductID=1 AND QuantityOnHand=40 AND LastRestockDate=?",
                        java.sql.Date.valueOf("2025-06-01")) &&
                rowExists("SELECT 1 FROM Inventory WHERE StoreID=1 AND ProductID=2 AND QuantityOnHand=25 AND LastRestockDate=?",
                        java.sql.Date.valueOf("2025-06-05")) &&
                rowExists("SELECT 1 FROM Inventory WHERE StoreID=2 AND ProductID=1 AND QuantityOnHand=10 AND LastRestockDate=?",
                        java.sql.Date.valueOf("2025-05-28")) &&
                rowExists("SELECT 1 FROM Inventory WHERE StoreID=2 AND ProductID=3 AND QuantityOnHand=60 AND LastRestockDate=?",
                        java.sql.Date.valueOf("2025-06-10")) &&

                // Orders
                rowExists("SELECT 1 FROM Orders WHERE CustomerID=1 AND StoreID=1 AND OrderDate=? AND Status='Delivered' AND PaymentMethod='Card' AND OrderTotal=1247.00",
                        Timestamp.valueOf("2025-07-01 10:15:00")) &&
                rowExists("SELECT 1 FROM Orders WHERE CustomerID=2 AND StoreID=2 AND OrderDate=? AND Status='Shipped' AND PaymentMethod='UPI' AND OrderTotal=149.00",
                        Timestamp.valueOf("2025-07-03 16:22:00")) &&
                rowExists("SELECT 1 FROM Orders WHERE CustomerID=1 AND StoreID=1 AND OrderDate=? AND Status='Cancelled' AND PaymentMethod='Wallet' AND OrderTotal=299.00",
                        Timestamp.valueOf("2025-07-05 12:05:00")) &&

                // OrderItems
                rowExists("SELECT 1 FROM OrderItems WHERE OrderID=1 AND ProductID=1 AND Quantity=2 AND UnitPriceAtSale=499.00") &&
                rowExists("SELECT 1 FROM OrderItems WHERE OrderID=1 AND ProductID=2 AND Quantity=1 AND UnitPriceAtSale=249.00") &&
                rowExists("SELECT 1 FROM OrderItems WHERE OrderID=2 AND ProductID=3 AND Quantity=1 AND UnitPriceAtSale=149.00") &&
                rowExists("SELECT 1 FROM OrderItems WHERE OrderID=3 AND ProductID=2 AND Quantity=1 AND UnitPriceAtSale=299.00");
            yakshaAssert(currentTest(), ok, businessTestFile);
        } catch (Exception ex) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    @Order(4)
    public void testInventoryCompositeUnique() throws Exception {
        // Try inserting a duplicate (StoreID, ProductID). Expect failure.
        boolean passed;
        conn.setAutoCommit(false);
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO Inventory(StoreID, ProductID, QuantityOnHand, LastRestockDate) VALUES (1,1,99, CURRENT_DATE())")) {
            ps.executeUpdate();
            passed = false; // if we got here, composite unique not enforced
        } catch (SQLException e) {
            passed = true; // uniqueness enforced
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
        yakshaAssert(currentTest(), passed, businessTestFile);
    }

    @Test
    @Order(5)
    public void testProceduresExist() throws Exception {
        try {
            List<String> procs = Arrays.asList(
                "sp_SalesAggregateStats",
                "sp_StringOps",
                "sp_JoinReports",
                "sp_FilterOperatorExamples",
                "sp_GroupingAndHaving",
                "sp_DateFormatting",
                "sp_InventoryChecks",
                "sp_OrderLifecycle"
            );
            boolean all = true;
            for (String p : procs) if (!routineExists(p)) { all = false; break; }
            yakshaAssert(currentTest(), all, businessTestFile);
        } catch (Exception e) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    @Order(6)
    public void testProcedureBodiesContainExpectedClauses() throws Exception {
        try {
            boolean ok = true;

            String agg = getRoutineBody("sp_SalesAggregateStats");
            ok &= agg.contains("COUNT") && (agg.contains("SUM") || agg.contains("TOTAL")) && agg.contains("MAX");

            String str = getRoutineBody("sp_StringOps");
            ok &= (str.contains("UPPER") || str.contains("UCASE")) && (str.contains("CONCAT") || str.contains("COALESCE"));

            String joins = getRoutineBody("sp_JoinReports");
            ok &= joins.contains("JOIN");

            String filt = getRoutineBody("sp_FilterOperatorExamples");
            ok &= (filt.contains("LIKE") || filt.contains("IN"));

            String grp = getRoutineBody("sp_GroupingAndHaving");
            ok &= grp.contains("GROUP BY") && grp.contains("HAVING");

            String dt = getRoutineBody("sp_DateFormatting");
            ok &= (dt.contains("DATE_FORMAT") || dt.contains("FORMAT") || dt.contains("TIMESTAMPDIFF"));

            String inv = getRoutineBody("sp_InventoryChecks");
            ok &= inv.contains("QUANTITYONHAND") && (inv.contains("DATEDIFF") || inv.contains("LASTRESTOCKDATE"));

            String life = getRoutineBody("sp_OrderLifecycle");
            ok &= life.contains("UPDATE") && life.contains("ORDERS");

            yakshaAssert(currentTest(), ok, businessTestFile);
        } catch (Exception e) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    @Order(7)
    public void testProceduresCallable() throws Exception {
        try {
            boolean ok = callProcedure("{call sp_SalesAggregateStats()}") &&
                         callProcedure("{call sp_StringOps()}") &&
                         callProcedure("{call sp_JoinReports()}") &&
                         callProcedure("{call sp_FilterOperatorExamples()}") &&
                         callProcedure("{call sp_GroupingAndHaving()}") &&
                         callProcedure("{call sp_DateFormatting()}") &&
                         callProcedure("{call sp_InventoryChecks()}");
            yakshaAssert(currentTest(), ok, businessTestFile);
        } catch (Exception e) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    @Order(8)
    public void testViewsExist() throws Exception {
        try {
            boolean ok = viewExists("vw_RecentOrders") && viewExists("vw_BestSellers");
            yakshaAssert(currentTest(), ok, businessTestFile);
        } catch (Exception e) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    @Order(9)
    public void testViewsReturnRows() throws Exception {
        try {
            boolean ok = true;
            try (Statement st = conn.createStatement()) {
                try (ResultSet rs = st.executeQuery("SELECT * FROM vw_RecentOrders LIMIT 1")) {
                    ok &= rs.next(); // at least one row (given sample orders are in 2025)
                }
                try (ResultSet rs = st.executeQuery("SELECT * FROM vw_BestSellers LIMIT 1")) {
                    ok &= rs.next();
                }
            }
            yakshaAssert(currentTest(), ok, businessTestFile);
        } catch (Exception e) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    @Test
    @Order(10)
    public void testOrderLifecycleValidTransitions() throws Exception {
        boolean ok = false;
        conn.setAutoCommit(false);
        try {
            // Insert a temp order in 'Placed'
            int customerId = 1, storeId = 1;
            long newOrderId;
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO Orders(CustomerID, StoreID, OrderDate, Status, PaymentMethod, OrderTotal) VALUES (?,?,?,?,?,?)",
                Statement.RETURN_GENERATED_KEYS)) {
                ps.setInt(1, customerId);
                ps.setInt(2, storeId);
                ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                ps.setString(4, "Placed");
                ps.setString(5, "Card");
                ps.setBigDecimal(6, new java.math.BigDecimal("0.00"));
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    keys.next();
                    newOrderId = keys.getLong(1);
                }
            }

            // Call lifecycle procedure to move to Shipped then Delivered
            ok = callProcedure("{call sp_OrderLifecycle(" + newOrderId + ", 'Shipped')}") &&
                 callProcedure("{call sp_OrderLifecycle(" + newOrderId + ", 'Delivered')}");

            // Verify final status = Delivered
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT Status FROM Orders WHERE OrderID=?")) {
                ps.setLong(1, newOrderId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) ok &= "Delivered".equalsIgnoreCase(rs.getString(1));
                }
            }
        } catch (Exception e) {
            ok = false;
        } finally {
            conn.rollback(); // clean up
            conn.setAutoCommit(true);
        }
        yakshaAssert(currentTest(), ok, businessTestFile);
    }

    @Test
    @Order(11)
    public void testOrderLifecycleInvalidTransitionBlocked() throws Exception {
        boolean ok = false;
        conn.setAutoCommit(false);
        try {
            // Insert a Delivered order
            long oid;
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO Orders(CustomerID, StoreID, OrderDate, Status, PaymentMethod, OrderTotal) VALUES (1,1,NOW(),'Delivered','Card',0.00)",
                Statement.RETURN_GENERATED_KEYS)) {
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) { keys.next(); oid = keys.getLong(1); }
            }
            // Attempt invalid transition back to Placed
            callProcedure("{call sp_OrderLifecycle(" + oid + ", 'Placed')}");

            // Verify status unchanged (still Delivered)
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT Status FROM Orders WHERE OrderID=?")) {
                ps.setLong(1, oid);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) ok = "Delivered".equalsIgnoreCase(rs.getString(1));
                }
            }
        } catch (Exception e) {
            ok = false;
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
        yakshaAssert(currentTest(), ok, businessTestFile);
    }

    @Test
    @Order(12)
    public void testJoinReportsLikelyWork() throws Exception {
        // Light sanity: ensure at least one line-level join result is obtainable using the relationships
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT c.FullName, p.ProductName, oi.Quantity, (oi.Quantity*oi.UnitPriceAtSale) AS LineTotal " +
                "FROM Orders o " +
                "JOIN Customers c ON c.CustomerID = o.CustomerID " +
                "JOIN OrderItems oi ON oi.OrderID = o.OrderID " +
                "JOIN Products p ON p.ProductID = oi.ProductID " +
                "LIMIT 1")) {
            boolean ok;
            try (ResultSet rs = ps.executeQuery()) { ok = rs.next(); }
            yakshaAssert(currentTest(), ok, businessTestFile);
        } catch (Exception e) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }

    // ---------------------- Optional Advanced (non-penalizing if absent) ----------------------

    @Test
    @Order(13)
    public void optionalTriggerExistsAndWorks() throws Exception {
        boolean status = true; // default pass if absent
        try {
            if (triggerExists("trg_OrderItems_AI")) {
                status = false; // will set true if behavior matches
                conn.setAutoCommit(false);

                // Find a store/product with stock
                int storeId = 1;
                int productId = 1;
                int startQty = 0;
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT QuantityOnHand FROM Inventory WHERE StoreID=? AND ProductID=?")) {
                    ps.setInt(1, storeId);
                    ps.setInt(2, productId);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) startQty = rs.getInt(1);
                    }
                }

                // Insert temp order at that store
                long oid;
                try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO Orders(CustomerID, StoreID, OrderDate, Status, PaymentMethod, OrderTotal) VALUES (1, ?, NOW(), 'Placed','Card',0.00)",
                    Statement.RETURN_GENERATED_KEYS)) {
                    ps.setInt(1, storeId);
                    ps.executeUpdate();
                    try (ResultSet keys = ps.getGeneratedKeys()) { keys.next(); oid = keys.getLong(1); }
                }

                // Insert order item (should fire trigger)
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO OrderItems(OrderID, ProductID, Quantity, UnitPriceAtSale) VALUES (?,?,?,?)")) {
                    ps.setLong(1, oid);
                    ps.setInt(2, productId);
                    ps.setInt(3, 1);
                    ps.setBigDecimal(4, new java.math.BigDecimal("499.00"));
                    ps.executeUpdate();
                }

                // Check inventory decreased by 1
                int endQty = startQty;
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT QuantityOnHand FROM Inventory WHERE StoreID=? AND ProductID=?")) {
                    ps.setInt(1, storeId);
                    ps.setInt(2, productId);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) endQty = rs.getInt(1);
                    }
                }
                status = (endQty == startQty - 1);

                conn.rollback();
                conn.setAutoCommit(true);
            }
        } catch (Exception e) {
            // keep status as-is (pass if absent)
        }
        yakshaAssert(currentTest(), status, businessTestFile);
    }

    @Test
    @Order(14)
    public void optionalFunctionFnOrderTotalWorks() throws Exception {
        boolean status = true; // pass if absent
        try {
            if (routineExists("fn_OrderTotal")) {
                status = false;
                conn.setAutoCommit(false);

                // Insert order + order items with known totals
                long oid;
                try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO Orders(CustomerID, StoreID, OrderDate, Status, PaymentMethod, OrderTotal) VALUES (1,1,NOW(),'Placed','Card',0.00)",
                    Statement.RETURN_GENERATED_KEYS)) {
                    ps.executeUpdate();
                    try (ResultSet keys = ps.getGeneratedKeys()) { keys.next(); oid = keys.getLong(1); }
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO OrderItems(OrderID, ProductID, Quantity, UnitPriceAtSale) VALUES " +
                        "(?,?,?,?),(?,?,?,?)")) {
                    ps.setLong(1, oid); ps.setInt(2, 1); ps.setInt(3, 2); ps.setBigDecimal(4, new java.math.BigDecimal("100.00"));
                    ps.setLong(5, oid); ps.setInt(6, 2); ps.setInt(7, 1); ps.setBigDecimal(8, new java.math.BigDecimal("50.00"));
                    ps.executeUpdate();
                }

                // Expect 250.00
                try (PreparedStatement ps = conn.prepareStatement("SELECT fn_OrderTotal(?)")) {
                    ps.setLong(1, oid);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            java.math.BigDecimal val = rs.getBigDecimal(1);
                            status = new java.math.BigDecimal("250.00").compareTo(val) == 0;
                        }
                    }
                }

                conn.rollback();
                conn.setAutoCommit(true);
            }
        } catch (Exception e) {
            // leave status pass if absent
        }
        yakshaAssert(currentTest(), status, businessTestFile);
    }

    @Test
    @Order(15)
    public void optionalIndexesPresent() throws Exception {
        boolean status = true; // pass if absent
        try {
            // Check presence if learner added them with these common names; tolerate absence
            boolean anyPresent = indexExists("Orders", "CustomerID") || indexExists("Orders", "CustomerID_OrderDate")
                               || indexExists("OrderItems", "OrderID")
                               || indexExists("Inventory", "StoreID")
                               || indexExists("Inventory", "StoreID_ProductID")
                               || indexExists("Products", "Category");
            // If none present, still pass (optional). If at least one present, also pass.
            status = true;
        } catch (Exception e) {
            status = true; // optional
        }
        yakshaAssert(currentTest(), status, businessTestFile);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        try {
            if (conn != null && !conn.isClosed()) conn.close();
        } catch (Exception ignored) { }
    }
}