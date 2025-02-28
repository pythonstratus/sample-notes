package com.als;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * DailyLoad - Java implementation of the daily entity load process
 * 
 * This application handles the daily loading process for entity data extracts.
 * It performs the following main tasks:
 * 1. Check for holidays and exit if necessary
 * 2. Validate extract files and dates
 * 3. Process daily extracts
 * 4. Execute SQL loading procedures
 * 5. Generate reports and notifications
 * 
 * Original shell script: c.dailyLOAD
 */
public class DailyLoad {
    // Base paths and directories
    private String appBase;       // ALS application base directory
    private String logDir;        // Directory for log files
    private String logFile;       // Current log file 
    private String ftpDir;        // FTP directory where extracts arrive
    private String bkupDir;       // Backup directory
    private String splFile;       // Spool file for reports
    
    // Date extract positions (character positions in extract files)
    private Map<String, String> datePositions = new HashMap<>();
    
    // Unix command paths
    private String[] unixCommands = new String[14];
    
    // Server information
    private String server;        // Current server hostname
    private String devName;       // Development server name
    private String prodName;      // Production server name
    private String tstName;       // Test server name
    private String serverType;    // Current server type (DEVELOPMENT/TEST/PRODUCTION)
    private String subject;       // Email subject line
    
    // Entity and file lists
    private String[] dailyEntities;  // List of entity codes (E5, E3, E8, E7, EB)
    private String[] dataFiles;      // List of data files (with .dat extension)
    private String[] outFiles;       // List of output files (with .out extension)
    private String[] badFiles;       // List of bad record files (with .bad extension)
    private String[] logFiles;       // List of log files (with .log extension)
    
    // Directory paths
    private String entDir;           // Entity executables directory 
    private String loadDir;          // Load scripts directory
    private String decipherItPath;   // Path to password decryption utility
    private String oraclePath;       // Path to Oracle environment setup
    
    // Database credentials
    private String dbPassword;       // Decrypted database password
    
    // Current runtime values
    private String today;            // Today's date in MM/DD/YYYY format
    private String yesterday;        // Yesterday's date 
    private String holiday;          // Holiday date (if any)
    private int days2Add;            // Number of days to add based on day of week
    private String errChk;           // Error check file
    private String newExtractDate;   // Expected extract date for validation
    private String currE5;           // Current E5 extract date
    private String prevE9;           // Previous E9 extract date
    private int dateDiff;            // Difference between E5 and E9 dates
    
    // Collection of extract dates
    private Map<String, String> previousExtractDates = new HashMap<>();  // From database
    private Map<String, String> currentExtractDates = new HashMap<>();   // From files
    
    /**
     * Constructor initializing basic configuration
     */
    public DailyLoad() {
        // Initialize date extraction positions for each entity
        datePositions.put("E3", "3-10");
        datePositions.put("E5", "65-72");
        datePositions.put("E7", "78-85");
        datePositions.put("E8", "28-35");
        datePositions.put("EB", "48-55");
        
        // Set Unix commands equivalent paths
        unixCommands[0] = "/usr/bin/rm";      // RM
        unixCommands[1] = "/usr/bin/mv";      // MV
        unixCommands[2] = "/usr/bin/ls";      // LS
        unixCommands[3] = "/usr/bin/cp";      // CP
        unixCommands[4] = "/usr/bin/wc";      // WC
        unixCommands[5] = "/usr/bin/cat";     // CAT
        unixCommands[6] = "/usr/bin/cut";     // CUT
        unixCommands[7] = "/usr/bin/grep";    // GREP
        unixCommands[8] = "/usr/bin/mailx";   // MAILX
        unixCommands[9] = "/usr/bin/head";    // HEAD
        unixCommands[10] = "/usr/bin/tail";   // TAIL
        unixCommands[11] = "/usr/bin/echo";   // ECHO
        unixCommands[12] = "date +%a";        // DOW (Day of Week)
        unixCommands[13] = "/usr/gnu/bin/date"; // GNUDT
    }

    /**
     * Initialize application environment and settings
     */
    public void initialize() throws IOException {
        // Set base application directory
        appBase = "/als-ALS/app";
        
        // Set paths for special utilities
        decipherItPath = appBase + "/execloc/d.common/Decipherit als";
        oraclePath = appBase + "/execloc/d.als/ORA.path";
        
        // Set log file parameters
        logDir = "/path/to/log/directory"; // Set actual path in production
        logFile = logDir + "/dailyload.log";
        
        // Set other directories
        ftpDir = "/path/to/ftp/directory";    // Set actual path in production
        bkupDir = "/path/to/backup/directory"; // Set actual path in production
        splFile = "/tmp/logload_report.spl";   // Spool file for SQL reports
        
        // Load Oracle environment variables
        loadOracleEnvironment();
        
        // Determine which server we're running on
        serverType = determineServerType();
        
        // Set email subject line
        subject = "DAILY ENTITY LOADED ON " + serverType;
        
        // Set entity and file lists
        dailyEntities = new String[]{"E5", "E3", "E8", "E7", "EB"};
        dataFiles = new String[]{"E5.dat", "E3.dat", "E8.dat", "E7.dat", "EB.dat"};
        outFiles = new String[]{"E5.out", "E3.out", "E8.out", "E7.out", "EB.out"};
        badFiles = new String[]{"E5.bad", "E3.bad", "E8.bad", "E7.bad", "EB.bad"};
        logFiles = new String[]{"loadE5.log", "loadE3.log", "loadE8.log", "loadE7.log", "loadEB.log"};
        
        // Set application directories
        entDir = appBase + "/execloc/d.entity";
        loadDir = appBase + "/execloc/d.loads";
    }
    
    /**
     * Load Oracle environment variables
     */
    private void loadOracleEnvironment() throws IOException {
        // In real implementation, this would read the Oracle path file and set environment variables
        // For this demo, we'll simulate the process
        String oracleEnvContent = new String(Files.readAllBytes(Paths.get(oraclePath)));
        // Parse environment variables from the file
        
        // This would decrypt the ALS password
        dbPassword = "decrypted_password"; // In real app, would be: executeCommand(decipherItPath)
    }
    
    /**
     * Determine which server the application is running on by checking hostname
     */
    private String determineServerType() {
        String hostname = executeCommand("/usr/bin/uname -n").trim();
        
        if (hostname.equals(devName)) {
            return "DEVELOPMENT";
        } else if (hostname.equals(tstName)) {
            return "TEST";
        } else if (hostname.equals(prodName)) {
            return "PRODUCTION";
        } else {
            return "UNKNOWN";
        }
    }
    
    /**
     * Main processing method - coordinates the entire daily load process
     */
    public void processDailyLoad(String[] args) throws IOException, SQLException, ParseException, InterruptedException {
        // Set today's date - either from command-line argument or current system date
        if (args.length == 0) {
            today = executeCommand("date +%m/%d/%Y").trim();
        } else {
            today = args[0];
        }
        
        // Change to log directory
        changeDirectory(logDir);
        
        // Begin loading daily extracts - log the start of processing
        String logMessage = "Begin loading daily extracts on " + serverType + ".......... " + 
                            executeCommand("date +%m/%d/%Y' '%H:%M:%S").trim() + "\n";
        appendToFile(logFile, logMessage);
        
        // Get yesterday's date - used for holiday checking
        yesterday = getYesterdayDate();
        
        // Check if yesterday was a holiday
        holiday = getHolidayDate();
        
        // If yesterday was a holiday, insert record in LOGLOAD table and exit
        if (holiday.equals(yesterday)) {
            appendToFile(logFile, "Yesterday " + yesterday + " was a holiday, inserting LOGLOAD records\n");
            executeHolidayProcedure(holiday);
            appendToFile(logFile, "EXITING ... due to holiday\n");
            return; // Exit processing due to holiday
        } else {
            appendToFile(logFile, "Yesterday " + yesterday + " was not a holiday, continue process\n");
        }
        
        // Unlock entity processing
        String dayOfWeek = executeCommand("date +%a").trim();
        if (!dayOfWeek.equals("Sat")) {
            appendToFile(logFile, "Unlocking Entity ............................ " + 
                        executeCommand("date +%H:%M:%S").trim() + "\n");
            executeCommand(entDir + "/als_lock ent u load");
            appendToFile(logFile, executeCommand(entDir + "/als_lock ent u load").trim());
        } else {
            appendToFile(logFile, "Unlocking Entity ............................ " + 
                        executeCommand("date +%H:%M:%S").trim() + "\n");
            executeCommand(entDir + "/als_lock ent u load");
            appendToFile(logFile, executeCommand(entDir + "/als_lock ent u load").trim());
        }
        
        // Test to see if the previous weekly loaded before running the next daily load
        // This prevents major data corruption by comparing daily E5 with the weekly E9 extract
        if (dayOfWeek.equals("Tue")) {
            appendToFile(logFile, "Begin comparing weekly and daily extract dates .... " + 
                executeCommand("date +%H:%M:%S").trim() + "\n");
            
            compareWeeklyAndDailyExtracts();
        }
        
        // Determine days to add based on day of week
        days2Add = determineDays2Add();
        
        // Process backup of previous files
        backupPreviousFiles();
        
        // Check for extract files and wait if not present
        checkForExtractFiles();
        
        // Copy daily extracts from FTP directory to LOAD directory with .dat extension
        copyDailyExtracts();
        
        // Get previous extract dates from LOGLOAD table
        getPreviousExtractDates();
        
        // Calculate the new expected extract date and validate extract dates in files
        calculateNewExtractDate();
        
        // Load the daily extracts
        loadDailyExtracts();
        
        // Send final report and email notification
        generateFinalReport();
        
        // Final log message
        appendToFile(logFile, "Daily extract loading process completed successfully at " + 
                    executeCommand("date +%m/%d/%Y' '%H:%M:%S").trim() + "\n");
    }

    /**
     * Get yesterday's date using SQL query
     */
    private String getYesterdayDate() throws SQLException {
        String sql = "SELECT TO_DATE('" + today + "','MM/DD/YYYY') - 1 FROM dual";
        return executeSqlQuery(sql);
    }
    
    /**
     * Check if a date is a holiday using the DATELIB.xtrcthdy function
     */
    private String getHolidayDate() throws SQLException {
        String sql = "SELECT DATELIB.xtrcthdy('" + today + "') FROM dual";
        return executeSqlQuery(sql);
    }
    
    /**
     * Execute SQL procedure for holiday processing
     */
    private void executeHolidayProcedure(String holidayDate) throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "als", dbPassword);
            
            // Set NLS date format
            executeSqlUpdate(conn, "ALTER SESSION SET nls_date_format = 'MM/DD/YYYY HH24:MI:SS'");
            
            // Execute the stored procedure
            executeSqlUpdate(conn, "BEGIN DATELIB.nsrtholidayrecs('" + holidayDate + "'); END;");
            
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    /**
     * Compare weekly and daily extract dates to prevent data corruption
     */
    private void compareWeeklyAndDailyExtracts() throws IOException, SQLException, ParseException {
        // Check if E5 file exists
        File e5File = new File(ftpDir + "/E5");
        if (!e5File.exists()) {
            appendToFile(logFile, "No E5 extract EXITING ............................ " + 
                         executeCommand("date +%H:%M:%S").trim() + "\n");
            System.exit(1); // Exit with error
        }
        
        // Get E5 extract date from file - using position from datePositions map
        String extractRange = datePositions.get("E5");
        String[] range = extractRange.split("-");
        int startPos = Integer.parseInt(range[0]);
        int endPos = Integer.parseInt(range[1]);
        
        // Read the first line of the E5 file
        String e5FirstLine = executeCommand("head -1 " + ftpDir + "/E5").trim();
        
        // Extract the date portion based on position
        if (e5FirstLine.length() >= endPos) {
            currE5 = e5FirstLine.substring(startPos - 1, endPos);
        } else {
            appendToFile(logFile, "Invalid E5 file format EXITING .................... " + 
                         executeCommand("date +%H:%M:%S").trim() + "\n");
            System.exit(1);
        }
        
        // Get the previous E9 date from database
        prevE9 = getPreviousE9Date();
        
        // Convert dates to Julian format for comparison
        int currE5Julian = convertToJulianDate(currE5);
        int prevE9Julian = convertToJulianDate(prevE9);
        dateDiff = currE5Julian - prevE9Julian;
        
        // Log the comparison results
        appendToFile(logFile, "Current E5      -> " + currE5 + "\n");
        appendToFile(logFile, "Previous E9     -> " + prevE9 + "\n");
        appendToFile(logFile, "Current E5      -> " + currE5Julian + " julian\n");
        appendToFile(logFile, "Previous E9     -> " + prevE9Julian + " julian\n");
        appendToFile(logFile, "Difference      -> " + dateDiff + " Days\n");
        
        // Check if the difference is 2 days (expected gap between weekly and daily)
        if (dateDiff == 2) {
            appendToFile(logFile, "Weekly extract date is current ........... " + prevE9 + "\n");
            appendToFile(logFile, "Weekly loads ran ................................. " + 
                         executeCommand("date +%H:%M:%S").trim() + "\n");
            appendToFile(logFile, "End comparing weekly and daily extract dates ...... " + 
                         executeCommand("date +%H:%M:%S").trim() + "\n");
        } else {
            appendToFile(logFile, "Weekly extract date not is current ....... " + prevE9 + "\n");
            appendToFile(logFile, "Weekly loads did not run ........................... " + 
                         executeCommand("date +%H:%M:%S").trim() + "\n");
            System.exit(1); // Exit with error
        }
    }
    
    /**
     * Get the previous E9 extract date from the logload table
     */
    private String getPreviousE9Date() throws SQLException {
        String sql = "SELECT TO_CHAR(TO_DATE(max(extrdt)) ,'YYYYMMDD') " +
                     "FROM logload " +
                     "WHERE loadname = 'E9'";
        return executeSqlQuery(sql);
    }
    
    /**
     * Convert a date string to Julian date format for comparison
     */
    private int convertToJulianDate(String dateStr) throws ParseException {
        // Parse the date string based on format
        SimpleDateFormat sdf;
        if (dateStr.length() == 8) {
            sdf = new SimpleDateFormat("yyyyMMdd");
        } else {
            sdf = new SimpleDateFormat("MM/dd/yyyy");
        }
        
        Date date = sdf.parse(dateStr);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        
        // Calculate Julian date (day of year)
        return cal.get(Calendar.DAY_OF_YEAR);
    }
    
    /**
     * Determine days to add based on day of the week
     */
    private int determineDays2Add() {
        String dayOfWeek = executeCommand("date +%a").trim();
        
        switch (dayOfWeek) {
            case "Sun":
                return 2;
            case "Mon":
                appendToFile(logFile, "No loads on Monday\n");
                return 1; // Default case for Monday
            case "Tue":
                return 3;
            case "Wed":
            case "Thu":
            case "Fri":
            case "Sat":
                return 1;
            default:
                appendToFile(logFile, "No loads on Monday\n");
                return 1;
        }
    }
    
    /**
     * Backup previous files before processing new ones
     */
    private void backupPreviousFiles() throws IOException {
        appendToFile(logFile, "Begin backing up previous days files......... " + 
                    executeCommand("date +%H:%M:%S").trim() + "\n");
        
        // Process data files backup (remove old .dat files)
        for (String file : dataFiles) {
            File sourceFile = new File(loadDir + "/" + file);
            if (sourceFile.exists()) {
                executeCommand("rm -f " + loadDir + "/" + file + " > /dev/null");
                appendToFile(logFile, file + " has been removed...\n");
            } else {
                appendToFile(logFile, "No " + file + " to remove...\n");
            }
        }
        appendToFile(logFile, "\n");
        
        // Process bad files backup (append day of week to filename)
        for (String file : badFiles) {
            File sourceFile = new File(loadDir + "/" + file);
            if (sourceFile.exists()) {
                String dayOfWeek = executeCommand("date +%a").trim();
                executeCommand("mv " + loadDir + "/" + file + " " + bkupDir + "/" + file + "." + dayOfWeek);
                appendToFile(logFile, file + " has been backed up...\n");
            } else {
                appendToFile(logFile, "No " + file + " to backup...\n");
            }
        }
        appendToFile(logFile, "\n");
        
        // Process log files backup (append day of week to filename)
        for (String file : logFiles) {
            File sourceFile = new File(loadDir + "/" + file);
            if (sourceFile.exists()) {
                String dayOfWeek = executeCommand("date +%a").trim();
                executeCommand("mv " + loadDir + "/" + file + " " + bkupDir + "/" + file + "." + dayOfWeek);
                appendToFile(logFile, file + " has been backed up...\n");
            } else {
                appendToFile(logFile, "No " + file + " to backup...\n");
            }
        }
        appendToFile(logFile, "\n");
        
        // Process output files from SQL (*.out files)
        for (String file : outFiles) {
            File sourceFile = new File(loadDir + "/" + file);
            if (sourceFile.exists()) {
                String dayOfWeek = executeCommand("date +%a").trim();
                executeCommand("mv " + loadDir + "/" + file + " " + bkupDir + "/" + file + "." + dayOfWeek + " >& " + logFile);
                appendToFile(logFile, file + " has been backed up...\n");
            } else {
                appendToFile(logFile, "No " + file + " to backup...\n");
            }
        }
        
        // Clean up loaded.out file
        executeCommand("rm " + loadDir + "/loaded.out");
        
        appendToFile(logFile, "\nEnd backing up previous days files........... " + 
                    executeCommand("date +%H:%M:%S").trim() + "\n");
        appendToFile(logFile, "\n-------------------------------------------------------\n");
    }
    
    /**
     * Check for extract files and wait if they're not present
     */
    private void checkForExtractFiles() throws IOException, InterruptedException {
        appendToFile(logFile, "Begin checking for daily extracts............. " + 
                    executeCommand("date +%H:%M:%S").trim() + "\n");
        
        // Set default error flag
        errChk = "0";
        File errChkFile = new File(errChk);
        if (!errChkFile.exists()) {
            appendToFile(logFile, "0 > " + errChk + "\n");
        }
        
        // Loop through each daily entity and check for file
        for (String entity : dailyEntities) {
            boolean fileFound = false;
            
            while (!fileFound) {
                File extractFile = new File(ftpDir + "/" + entity);
                if (extractFile.exists()) {
                    appendToFile(logFile, entity + " extract file found...\n");
                    fileFound = true;
                } else {
                    appendToFile(logFile, "ERROR: " + entity + " extract file not found...\n");
                    // Sleep 5 minutes (300 seconds) and retry
                    TimeUnit.SECONDS.sleep(300);
                }
            }
        }
        
        appendToFile(logFile, "\nEnd checking for daily extracts.............. " + 
                    executeCommand("date +%H:%M:%S").trim() + "\n");
        appendToFile(logFile, "\n-------------------------------------------------------\n");
    }
    
    /**
     * Copy daily extract files from FTP directory to LOAD directory
     */
    private void copyDailyExtracts() throws IOException {
        appendToFile(logFile, "Begin copying daily extracts................. " + 
                    executeCommand("date +%H:%M:%S").trim() + "\n");
        
        for (String entity : dailyEntities) {
            File sourceFile = new File(ftpDir + "/" + entity);
            if (sourceFile.exists()) {
                // Copy and add .dat extension
                executeCommand("cp " + ftpDir + "/" + entity + " " + loadDir + "/" + entity + ".dat");
                
                // Verify copy was successful
                File destFile = new File(loadDir + "/" + entity + ".dat");
                if (destFile.exists()) {
                    appendToFile(logFile, entity + " extract copied to " + entity + ".dat...\n");
                } else {
                    appendToFile(logFile, "ERROR: " + entity + " extract file not copied...EXITING\n");
                    System.exit(1);
                }
            } else {
                appendToFile(logFile, "ERROR: " + entity + " extract file not found in FTP directory\n");
                System.exit(1);
            }
        }
        
        appendToFile(logFile, "\nEnd copying daily extracts................... " + 
                    executeCommand("date +%H:%M:%S").trim() + "\n");
        appendToFile(logFile, "\n-------------------------------------------------------\n");
    }
    
    /**
     * Get previous extract dates from the logload table
     */
    private void getPreviousExtractDates() throws SQLException, IOException {
        appendToFile(logFile, "Begin getting previous extract dates......... " + 
                    executeCommand("date +%H:%M:%S").trim() + "\n");
        
        // Get E5 extract date
        String e5Query = "SELECT max(EXTRDT) FROM LOGLOAD WHERE LOADNAME = 'E5'";
        String e5Date = executeSqlQuery(e5Query);
        previousExtractDates.put("E5", e5Date);
        appendToFile(logFile, "Last E5 extract date loaded: " + e5Date + "\n");
        
        // Get E3 extract date
        String e3Query = "SELECT max(EXTRDT) FROM LOGLOAD WHERE LOADNAME = 'E3'";
        String e3Date = executeSqlQuery(e3Query);
        previousExtractDates.put("E3", e3Date);
        appendToFile(logFile, "Last E3 extract date loaded: " + e3Date + "\n");
        
        // Get E8 extract date
        String e8Query = "SELECT max(EXTRDT) FROM LOGLOAD WHERE LOADNAME = 'E8'";
        String e8Date = executeSqlQuery(e8Query);
        previousExtractDates.put("E8", e8Date);
        appendToFile(logFile, "Last E8 extract date loaded: " + e8Date + "\n");
        
        // Get E7 extract date
        String e7Query = "SELECT max(EXTRDT) FROM LOGLOAD WHERE LOADNAME = 'E7'";
        String e7Date = executeSqlQuery(e7Query);
        previousExtractDates.put("E7", e7Date);
        appendToFile(logFile, "Last E7 extract date loaded: " + e7Date + "\n");
        
        // Get EB extract date
        String ebQuery = "SELECT max(EXTRDT) FROM LOGLOAD WHERE LOADNAME = 'EB'";
        String ebDate = executeSqlQuery(ebQuery);
        previousExtractDates.put("EB", ebDate);
        appendToFile(logFile, "Last EB extract date loaded: " + ebDate + "\n");
        
        appendToFile(logFile, "\nEnd getting previous extract dates........... " + 
                    executeCommand("date +%H:%M:%S").trim() + "\n");
        appendToFile(logFile, "\n-------------------------------------------------------\n");
    }
    
    /**
     * Calculate the new expected extract date
     */
    private void calculateNewExtractDate() throws SQLException, IOException, ParseException {
        appendToFile(logFile, "Begin comparing extract dates................ " + 
                    executeCommand("date +%H:%M:%S").trim() + "\n");
        
        // Compute the new extract date using SQL
        String previousE5ExtractDate = previousExtractDates.get("E5");
        
        // Format the SQL query to get the new extract date by adding days2Add to the previous date
        String sql = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYYMMDD'; " +
                     "SELECT TO_DATE('" + previousE5ExtractDate + "','MM/DD/YYYY') + " + days2Add + " FROM dual";
        
        // Execute the query to get the new extract date
        newExtractDate = executeSqlQuery(sql);
        
        appendToFile(logFile, "Current extract date should be... " + newExtractDate + "\n");
        
        // Get the extract dates from the current files and validate them
        validateExtractDates();
    }
    
    /**
     * Validate extract dates from files against the expected date
     */
    private void validateExtractDates() throws IOException {
        // For each entity, extract the date from the data file and compare to expected date
        for (String entity : dailyEntities) {
            String position = datePositions.get(entity);
            String[] range = position.split("-");
            int startPos = Integer.parseInt(range[0]);
            int endPos = Integer.parseInt(range[1]);
            
            // Extract date from file using head and cut commands
            String extractDate = executeCommand("head -1 " + loadDir + "/" + entity + ".dat | cut -c " + startPos + "-" + endPos).trim();
            
            // Validate the date against expected date
            if (extractDate.equals(newExtractDate)) {
                appendToFile(logFile, entity + " extract date is correct....... " + extractDate + "\n");
            } else {
                appendToFile(logFile, "ERROR: " + entity + " extract date " + extractDate + " is incorrect...EXITING\n");
                System.exit(1); // Exit if date is incorrect
            }
        }
        
        appendToFile(logFile, "\nEnd comparing extract dates.................. " + 
                    executeCommand("date +%H:%M:%S").trim() + "\n");
        appendToFile(logFile, "\n-------------------------------------------------------\n");
    }
    
    private void loadDailyExtracts() throws IOException {
        appendToFile(logFile, "Begin loading daily extracts..... " + 
                    executeCommand("date +%H:%M:%S").trim() + "\n");
        
        // Process each entity
        for (String entity : dailyEntities) {
            appendToFile(logFile, entity + " extract loading............... " + 
                        executeCommand("date +%H:%M:%S").trim() + "\n");
            
            // Execute the loading script and redirect output to the .out file
            executeCommand(loadDir + "/c.proc" + entity + " >> " + loadDir + "/" + entity + ".out");
            
            // Check for errors in the output file
            // First check for ERROR keyword
            String errCnt = executeCommand("grep -c ERROR " + loadDir + "/" + entity + ".out").trim();
            if (!errCnt.equals("0")) {
                appendToFile(logFile, "ERROR: Errors found in " + entity + ".out file...EXITING - " + 
                            executeCommand("date +%H:%M:%S").trim() + "\n");
                appendToFile(logFile, "\n");
                executeCommand("grep ERROR " + loadDir + "/" + entity + ".out >> " + logFile);
                System.exit(1);
            }
            
            // Also check for ERR keyword
            if (new File(loadDir + "/" + entity + ".out").exists()) {
                errCnt = executeCommand("grep -ic ERR " + loadDir + "/" + entity + ".out").trim();
                if (!errCnt.equals("0")) {
                    appendToFile(logFile, "ERROR: Errors found in " + entity + ".out file...EXITING - " + 
                                executeCommand("date +%H:%M:%S").trim() + "\n");
                    appendToFile(logFile, "\n");
                    executeCommand("grep ERR " + loadDir + "/" + entity + ".out >> " + logFile);
                    System.exit(1);
                }
            }
            
            // Add the log file content to the main log
            if (new File(loadDir + "/" + entity + ".out").exists()) {
                executeCommand("tail -1 " + loadDir + "/" + entity + ".out >> " + logFile);
                appendToFile(logFile, entity + " extract loaded............... " + 
                            executeCommand("date +%H:%M:%S").trim() + "\n");
            }
        }
        
        // If today is Saturday, do an additional unlock
        String dayOfWeek = executeCommand("date +%a").trim();
        if (dayOfWeek.equals("Sat")) {
            appendToFile(logFile, "Unlocking Entity ............................ " + 
                        executeCommand("date +%H:%M:%S").trim() + "\n");
            executeCommand(entDir + "/als_lock ent u load >> " + logFile);
        }
        
        // Log end of loading process
        appendToFile(logFile, "-------------------------------------------------------\n");
        appendToFile(logFile, "End loading daily extracts....... " + 
                    executeCommand("date +%H:%M:%S").trim() + "\n");
        appendToFile(logFile, "\n-------------------------------------------------------\n");
    }

    /**
     * Generate and send final report
     */
    private void generateFinalReport() throws IOException, SQLException {
        appendToFile(logFile, "Prepare report of logload records and send it to the ETS staff\n");
        
        Connection conn = null;
        try {
            // Connect to the database
            conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "als", dbPassword);
            
            // Set up SQL formatting
            executeSqlUpdate(conn, "ALTER SESSION SET nls_date_format = 'MM/DD/YYYY HH24:MI:SS'");
            executeSqlUpdate(conn, "SET feedback off");
            executeSqlUpdate(conn, "SET echo off");
            executeSqlUpdate(conn, "SET linesize 80");
            executeSqlUpdate(conn, "SET newpage 0");
            executeSqlUpdate(conn, "SET pagesize 60");
            
            // Define column formats for the report
            executeSqlUpdate(conn, "COLUMN LOADNAME FORMAT A4 HEADING 'FILE'");
            executeSqlUpdate(conn, "COLUMN EXTRDT FORMAT A12 HEADING 'EXTRACT DATE'");
            executeSqlUpdate(conn, "COLUMN LOADDT FORMAT A20 HEADING 'DATE LOADED'");
            executeSqlUpdate(conn, "COLUMN UNIX FORMAT A10 HEADING 'LOADED BY'");
            executeSqlUpdate(conn, "COLUMN NUMREC FORMAT 999999999999999999 HEADING 'RECORDS RECEIVED'");
            
            // Create a spool file for the report
            executeSqlUpdate(conn, "SPOOL " + splFile);
            
            // Execute the query to generate the report
            String reportQuery = "SELECT loadname, to_char(extrdt, 'MM/DD/YYYY ') \"EXTRACT DATE\", " +
                                "to_date(loaddt, 'MM/DD/YYYY HH24:MI:SS') \"DATE LOADED\", unix, " +
                                "numrec FROM logload WHERE loaddt LIKE '%" + today + "%' ORDER BY loaddt";
            
            executeSqlUpdate(conn, reportQuery);
            executeSqlUpdate(conn, "SPOOL OFF");
            
        } catch (SQLException e) {
            appendToFile(logFile, "ERROR: SQL Exception in generating report: " + e.getMessage() + "\n");
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // Ignore close errors
                }
            }
        }
        
        // Log the report content to the log file
        appendToFile(logFile, "LOADED ON " + serverType + " today..... " + today + "\n");
        executeCommand("cat " + splFile + " >> " + logFile);
        
        // Send email notification with the report
        appendToFile(logFile, "\nFINISHED LOADING ON " + serverType + "\n");
        executeCommand("mailx -s \"" + subject + "\" sbse.automated.liens.entity.cron.loads@irs.gov < " + logFile);
        
        // Backup the log file
        if (new File(logFile).exists()) {
            String dayOfWeek = executeCommand("date +%a").trim();
            executeCommand("cp " + logFile + " " + bkupDir + "/dailyload.log." + dayOfWeek);
        }
    }

    /**
     * Execute a SQL query and return the first result
     */
    private String executeSqlQuery(String sql) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "als", dbPassword);
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            
            if (rs.next()) {
                return rs.getString(1);
            }
            return "01/01/1900"; // Default date for no result
        } finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (conn != null) conn.close();
        }
    }

    /**
     * Execute a SQL update statement
     */
    private void executeSqlUpdate(Connection conn, String sql) throws SQLException {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(sql);
            stmt.executeUpdate();
        } finally {
            if (stmt != null) stmt.close();
        }
    }

    /**
     * Execute a system command and return the result
     */
    protected String executeCommand(String command) {
        StringBuilder output = new StringBuilder();
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(command);
            process.waitFor();
            java.io.BufferedReader reader = 
                new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()));
            
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output.toString();
    }

    /**
     * Change current directory
     */
    private void changeDirectory(String directory) {
        System.setProperty("user.dir", directory);
    }

    /**
     * Append text to a file
     */
    private void appendToFile(String filename, String text) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {
            writer.write(text);
        }
    }

    /**
     * Main method
     */
    public static void main(String[] args) {
        try {
            DailyLoad loader = new DailyLoad();
            loader.initialize();
            loader.processDailyLoad(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
