package com.als;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Unit tests for DailyLoad class
 */
public class DailyLoadTest {
    
    private DailyLoadTestable testableLoader;
    
    /**
     * Testable subclass that overrides methods that interact with external systems
     */
    private class DailyLoadTestable extends DailyLoad {
        // Override executeCommand to avoid actual system calls
        @Override
        protected String executeCommand(String command) {
            System.out.println("Mock executing: " + command);
            
            // Return mock responses based on command
            if (command.contains("date +%a")) {
                return "Tue\n"; // Simulate Tuesday
            } else if (command.contains("date +%m/%d/%Y")) {
                return "02/28/2025\n"; // Fixed date for testing
            } else if (command.contains("head -1") && command.contains("E5")) {
                // Simulate E5 file content with extract date at positions 65-72
                return "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor 20250228 incididunt\n";
            } else if (command.contains("uname -n")) {
                return "testserver\n";
            } else if (command.contains("grep -c ERROR")) {
                return "0\n"; // No errors
            } else if (command.contains("grep -ic ERR")) {
                return "0\n"; // No errors
            }
            
            return ""; // Default empty response
        }
        
        // Override SQL execution to avoid actual database connections
        @Override
        protected String executeSqlQuery(String sql) {
            System.out.println("Mock SQL query: " + sql);
            
            // Return mock responses based on SQL
            if (sql.contains("TO_DATE") && sql.contains("- 1")) {
                return "02/27/2025"; // Yesterday's date
            } else if (sql.contains("DATELIB.xtrcthdy")) {
                return "01/01/1900"; // No holiday
            } else if (sql.contains("max(EXTRDT)") && sql.contains("E5")) {
                return "02/25/2025"; // Previous E5 date
            } else if (sql.contains("max(EXTRDT)") && sql.contains("E9")) {
                return "20250226"; // Previous E9 date (in YYYYMMDD format)
            } else if (sql.contains("TO_DATE") && sql.contains("+ 3")) {
                return "20250228"; // New extract date
            }
            
            return "01/01/2025"; // Default date
        }
        
        // Override methods that check file existence
        @Override
        protected boolean fileExists(String filePath) {
            // Simulate file existence for certain paths
            if (filePath.contains("/E5") || 
                filePath.contains("/E3") || 
                filePath.contains("/E8") || 
                filePath.contains("/E7") || 
                filePath.contains("/EB")) {
                return true; // Extract files exist
            }
            return false;
        }
        
        // Helper method to expose protected methods for testing
        public Object invokePrivateMethod(String methodName, Class<?>[] paramTypes, Object[] params) {
            try {
                Method method = DailyLoad.class.getDeclaredMethod(methodName, paramTypes);
                method.setAccessible(true);
                return method.invoke(this, params);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        
        // Helper method to check file existence
        protected boolean fileExists(String filePath) {
            return new File(filePath).exists();
        }
    }
    
    @Before
    public void setUp() {
        testableLoader = new DailyLoadTestable();
        try {
            // Call initialize with test paths
            testableLoader.invokePrivateMethod("initialize", new Class<?>[]{}, new Object[]{});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testGetYesterdayDate() {
        try {
            Object result = testableLoader.invokePrivateMethod("getYesterdayDate", 
                                                             new Class<?>[]{}, 
                                                             new Object[]{});
            assertNotNull("Yesterday's date should not be null", result);
            assertEquals("Yesterday should be 02/27/2025", "02/27/2025", result);
        } catch (Exception e) {
            fail("Exception occurred: " + e.getMessage());
        }
    }
    
    @Test
    public void testGetHolidayDate() {
        try {
            Object result = testableLoader.invokePrivateMethod("getHolidayDate", 
                                                             new Class<?>[]{}, 
                                                             new Object[]{});
            assertNotNull("Holiday date should not be null", result);
            assertEquals("No holiday expected", "01/01/1900", result);
        } catch (Exception e) {
            fail("Exception occurred: " + e.getMessage());
        }
    }
    
    @Test
    public void testConvertToJulianDate() {
        try {
            Object result = testableLoader.invokePrivateMethod("convertToJulianDate", 
                                                             new Class<?>[]{String.class}, 
                                                             new Object[]{"20250228"});
            assertNotNull("Julian date should not be null", result);
            // The result should be the day of year (around 59 for Feb 28)
            int julianDate = (Integer)result;
            assertTrue("Julian date should be between 58 and 60 for Feb 28", 
                       julianDate >= 58 && julianDate <= 60);
        } catch (Exception e) {
            fail("Exception occurred: " + e.getMessage());
        }
    }
}
