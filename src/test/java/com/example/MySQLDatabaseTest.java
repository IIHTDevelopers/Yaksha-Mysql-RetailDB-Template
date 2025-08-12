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
                "sp_GroupingAndHaving"
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

            String grp = getRoutineBody("sp_GroupingAndHaving");
            ok &= grp.contains("GROUP BY") && grp.contains("HAVING");

            yakshaAssert(currentTest(), ok, businessTestFile);
        } catch (Exception e) {
            yakshaAssert(currentTest(), false, businessTestFile);
        }
    }


    @AfterAll
    public static void tearDown() throws Exception {
        try {
            if (conn != null && !conn.isClosed()) conn.close();
        } catch (Exception ignored) { }
    }
}
