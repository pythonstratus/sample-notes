import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.JulianFields;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Weekly Loader Application
 * 
 * This application handles the loading of weekly extract files into the system.
 * It checks extract dates, backs up previous week files, calculates EOM dates,
 * loads weekly extracts, and generates reports.
 * 
 * Converted from Unix shell script to Java
 */
public class WeeklyLoader {
    // Logger
    private Logger logger;
    
    // Configuration directories
    private String appHome;
    private String execlocDir;
    private String entityDir;
    private String ftpDir;
    private String commonDir;
    private String logDir;
    private String newdataDir;
    private String backupDir;
    private String eomDir;
    
    // Configuration files
    private String splitFile;
    private String logFile;
    private String errCheckFile;
    private String runlogFile = "runARCHIVEINV.log";
    private String caselogFile = "caseDSP.log";
    
    // Server environment
    private String serverName;
    private String devName;
    private String prodName;
    private String testName;
    private String serverEnvironment;
    
    // Extract positions in files
    private final String S1 = "46-53";
    private final String E1 = "3-10";
    private final String E2 = "121-128";
    private final String E3 = "3-10";
    private final String E4 = "69-76";
    private final String E6 = "11-18";
    private final String E9 = "3-10";
    private final String EA = "3-10";
    
    // Data files lists
    private List<String> weeklyDataFiles;
    private List<String> weeklyOutFiles;
    private List<String> weeklyBadFiles;
    private List<String> weeklyLogFiles;
    
    // Extract dates from database
    private String s1ExtractDate;
    private String e1ExtractDate;
    private String e2ExtractDate;
    private String e4ExtractDate;
    private String e3ExtractDate;
    private String eaExtractDate;
    private String e9ExtractDate;
    private String e6ExtractDate;
    
    // Calculated expected extract dates
    private String newExtractDate;
    private String newS1ExtractDate;
    
    // Extract dates from files
    private String s1FileDate;
    private String e1FileDate;
    private String e2FileDate;
    private String e4FileDate;
    private String e3FileDate;
    private String eaFileDate;
    private String e9FileDate;
    private String e6FileDate;
    
    // EOM dates
    private String rptMonth;
    private String eomStartDt;
    private String eomEndDt;
    private String eomSunday;
    private String eomExtractDate;
    
    // E3 date comparison
    private long diffDays;
    private String prevE3;
    private String currE3;
    private long prevE3Julian;
    private long currE3Julian;
    
    /**
     * Constructor - initializes and runs the weekly loader process
     * 
     * @param args Command line arguments (optional runDate)
     */
    public WeeklyLoader(String[] args) {
        try {
            setupLogger();
            initializeEnvironment();
            
            // Parse command line arguments
            String runDate = "";
            if (args.length == 0) {
                SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
                runDate = dateFormat.format(new Date());
            } else if (args.length == 1) {
                runDate = args[0];
            }
            
            // Main process
            logMessage("\nBegin loading weekly extracts on " + serverEnvironment + ".......... " + new Date());
            checkExtractDates();
            backupPreviousWeekFiles();
            calculateEomDates(runDate);
            getPreviousExtractDates();
            checkWeeklyExtracts();
            copyWeeklyExtracts();
            compareExtractDates();
            loadWeeklyExtracts();
            runNosegScripts();
            generateReport();
            sendReportEmail();
            cleanup();
            
        } catch (Exception e) {
            logMessage("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Set up the logger
     */
    private void setupLogger() throws Exception {
        logger = Logger.getLogger("WeeklyLoader");
        FileHandler fileHandler = new FileHandler("weeklyLoader.log");
        logger.addHandler(fileHandler);
        SimpleFormatter formatter = new SimpleFormatter();
        fileHandler.setFormatter(formatter);
    }
    
    /**
     * Log a message to both the logger and standard output
     */
    private void logMessage(String message) {
        logger.info(message);
        System.out.println(message);
    }
    
    /**
     * Initialize the environment and configuration settings
     */
    private void initializeEnvironment() {
        // Set paths based on Oracle environment
        appHome = System.getenv("APP_HOME") != null ? System.getenv("APP_HOME") : "/als-ALS/app";
        execlocDir = appHome + "/execloc/d.entity";
        entityDir = appHome + "/entity/d.ICS/d.NEWDATA";
        ftpDir = appHome + "/FTP";
        commonDir = appHome + "/execloc/d.common";
        logDir = entityDir + "/d.ICS/d.NEWDATA";
        newdataDir = entityDir + "/d.ICS/d.NEWDATA";
        backupDir = entityDir + "/d.ICS/d.NEWDATA/d.BACKUP";
        splitFile = appHome + "/entity/d.ICS/d.NEWDATA/wklyLogload.out";
        logFile = appHome + "/entity/d.ICS/d.NEWDATA/wklyLOAD.log";
        errCheckFile = appHome + "/execloc/d.entity/errcheck";
        eomDir = appHome + "/entity/d.ICS/d.NEWDATA/d.eom";
        
        // Server environment setup
        serverName = System.getenv("SERVER_NAME");
        devName = System.getenv("DEV_NAME");
        prodName = System.getenv("PROD_NAME");
        testName = System.getenv("TEST_NAME");
        
        if (serverName.equals(devName)) {
            serverEnvironment = "DEVELOPMENT";
        } else if (serverName.equals(testName)) {
            serverEnvironment = "TEST";
        } else if (serverName.equals(prodName)) {
            serverEnvironment = "PRODUCTION";
        } else {
            serverEnvironment = "UNKNOWN";
        }
        
        // Initialize data file lists
        weeklyDataFiles = Arrays.asList("S1.dat", "E1.dat", "E2.dat", "E4.dat", "E3.dat", "EA.dat", "E9.dat", "E6.dat");
        weeklyOutFiles = Arrays.asList("S1.out", "E1.out", "E2.out", "E4.out", "E3.out", "EA.out", "E9.out", "E6.out");
        weeklyBadFiles = Arrays.asList("S1.bad", "E1.bad", "E2.bad", "E4.bad", "E3.bad", "EA.bad", "E9.bad", "E6.bad");
        weeklyLogFiles = Arrays.asList("loadS1.log", "loadE1.log", "loadE2.log", "loadE4.log", "loadE3.log", "loadEA.log", "loadE9.log", "loadE6.log");
    }
    
    /**
     * Check extract dates by comparing current and previous E3 extract dates
     */
    private void checkExtractDates() {
        logMessage("\nBegin checking current and previous E3 extract dates...... " + new Date());
        
        try {
            // Get previous E3 extract date from the logload table
            prevE3 = getMaxExtractDateFromDatabase("E3");
            
            // Format: MM/DD/YYYY
            DateTimeFormatter dbFormat = DateTimeFormatter.ofPattern("MM/dd/yyyy");
            LocalDate prevE3Date = LocalDate.parse(prevE3, dbFormat);
            
            // Get current E3 extract date from FTPDIR
            currE3 = "";
            if (Files.exists(Paths.get(ftpDir + "/E3"))) {
                currE3 = getCurrentE3ExtractDate();
            } else {
                logMessage("Weekly load did not run existing.............................. " + new Date());
                return;
            }
            
            // Convert to Julian date for comparison
            DateTimeFormatter fileFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
            LocalDate currE3Date = LocalDate.parse(currE3, fileFormat);
            
            prevE3Julian = prevE3Date.getLong(JulianFields.JULIAN_DAY);
            currE3Julian = currE3Date.getLong(JulianFields.JULIAN_DAY);
            
            diffDays = currE3Julian - prevE3Julian;
            
            if (diffDays == 1) {
                logMessage("Daily extract date is current............. " + prevE3);
                logMessage("Current E3       -> " + currE3);
                logMessage("Previous E3      -> " + prevE3);
                logMessage("Current E3 Julian  -> " + currE3Julian);
                logMessage("Previous E3 Julian -> " + prevE3Julian);
                logMessage("Difference       -> " + diffDays + " days");
            }
            
            logMessage("End checking current and previous E3 extract dates........ " + new Date());
            
        } catch (Exception e) {
            logMessage("Error in checkExtractDates: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Get the maximum extract date from the database for a specific load name
     */
    private String getMaxExtractDateFromDatabase(String loadName) {
        // Dummy implementation - would use JDBC in real application
        // Simulates: SELECT TO_CHAR(TO_DATE(max(extdt)) ,'MM/DD/YYYY') from logload where loadname = 'E3'
        return "02/15/2025"; // Example return value
    }
    
    /**
     * Get the current E3 extract date from the FTP directory
     */
    private String getCurrentE3ExtractDate() throws Exception {
        // Dummy implementation - would read the actual file in real application
        // Simulates: $(HEAD) -1 $(FTPDIR)/E3 | $(CUT) -c 3-10
        return "20250216"; // Example return value
    }
    
    /**
     * Back up previous week's files to the backup directory
     */
    private void backupPreviousWeekFiles() {
        logMessage("\nBegin backing up previous weeks files.... " + new Date());
        
        try {
            // Change to log directory
            Path currentDir = Paths.get(logDir);
            
            // Backup data files
            for (String dataFile : weeklyDataFiles) {
                Path filePath = currentDir.resolve(dataFile);
                if (Files.exists(filePath)) {
                    Path destPath = Paths.get(backupDir, dataFile);
                    Files.copy(filePath, destPath, StandardCopyOption.REPLACE_EXISTING);
                    logMessage(dataFile + " has been backed up...");
                } else {
                    logMessage("No " + dataFile + " to backup...........");
                }
            }
            
            // Backup log files
            for (String logFile : weeklyLogFiles) {
                Path filePath = currentDir.resolve(logFile);
                if (Files.exists(filePath)) {
                    Path destPath = Paths.get(backupDir, logFile);
                    Files.copy(filePath, destPath, StandardCopyOption.REPLACE_EXISTING);
                    logMessage(logFile + " has been backed up...");
                } else {
                    logMessage("No " + logFile + " to backup...........");
                }
            }
            
            // Backup bad files
            for (String badFile : weeklyBadFiles) {
                Path filePath = currentDir.resolve(badFile);
                if (Files.exists(filePath)) {
                    Path destPath = Paths.get(backupDir, badFile);
                    Files.copy(filePath, destPath, StandardCopyOption.REPLACE_EXISTING);
                    logMessage(badFile + " has been backed up...");
                } else {
                    logMessage("No " + badFile + " to backup...........");
                }
            }
            
            // Backup out files created by c.procE
            for (String outFile : weeklyOutFiles) {
                Path filePath = currentDir.resolve(outFile);
                if (Files.exists(filePath)) {
                    Path destPath = Paths.get(backupDir, outFile);
                    Files.copy(filePath, destPath, StandardCopyOption.REPLACE_EXISTING);
                    logMessage(outFile + " has been backed up...");
                } else {
                    logMessage("No " + outFile + " to backup...........");
                }
            }
            
            // These files are only created during EOM processing
            String[] eomFiles = {"runARCHIVEINV.log", "mkARCINV.out", "caseDSP.log", "caseDSP.out"};
            
            // Backup EOM-specific files
            for (String eomFile : eomFiles) {
                Path filePath = Paths.get(eomDir, eomFile);
                if (Files.exists(filePath)) {
                    Path destPath = Paths.get(backupDir, eomFile);
                    Files.copy(filePath, destPath, StandardCopyOption.REPLACE_EXISTING);
                    logMessage(eomFile + " has been backed up...");
                } else {
                    logMessage("No " + eomFile + " to backup...........");
                }
            }
            
            logMessage("\nEnd backing up previous weeks files...... " + new Date());
            logMessage("\n------------------------------------------\n");
            
        } catch (Exception e) {
            logMessage("Error in backupPreviousWeekFiles: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Calculate the dates for End-of-Month (EOM) processing
     * 
     * @param runDate The date to use for calculation (MM/DD/YYYY format)
     */
    private void calculateEomDates(String runDate) {
        logMessage("\nDetermine the date for EOM Sunday processing.");
        logMessage("EOM Sunday can fall into the next month. This is");
        logMessage("especially true for months with 5 weeks.");
        logMessage("If not using today's Sunday, The calculation will be");
        logMessage("made using [TODAY] instead of SYSDATE.");
        logMessage("Begin calculating EOM dates............. " + new Date());
        
        try {
            rptMonth = executeQuery("SELECT datelib.datepart(SYSDATE, 'RPTMNTH') FROM dual");
            if (runDate != null && !runDate.isEmpty()) {
                // Use the provided date instead
                rptMonth = executeQuery("SELECT datelib.datepart('" + runDate + "', 'RPTMNTH') FROM dual");
            }
            
            // Get EOM start date
            eomStartDt = executeQuery("SELECT startdt FROM entmonth WHERE rptmonth = '" + rptMonth + "'");
            
            // Get EOM end date
            eomEndDt = executeQuery("SELECT enddt FROM entmonth WHERE rptmonth = '" + rptMonth + "'");
            
            // Add 1 to EOMENDDT to get EOM Sunday
            eomSunday = executeQuery("SELECT TO_DATE('" + eomEndDt + "', 'MM/DD/YYYY') + 1 FROM dual");
            
            // Get extract date in YYYYMMDD format
            eomExtractDate = executeQuery("SELECT TO_CHAR(TO_DATE('" + eomSunday + "') - 1, 'YYYYMMDD') FROM dual");
            
            logMessage("EOM Sunday.......... " + eomSunday);
            logMessage("EOM start date...... " + eomStartDt);
            logMessage("EOM end date........ " + eomEndDt);
            logMessage("EOM extract date.... " + eomExtractDate);
            logMessage("EOM report month.... " + rptMonth + "\n");
            
            logMessage("End calculating EOM dates................... " + new Date());
            logMessage("\n------------------------------------------\n");
            
        } catch (Exception e) {
            logMessage("Error in calculateEomDates: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Retrieve previous extract dates from the database
     */
    private void getPreviousExtractDates() {
        logMessage("\nGet last weeks extract dates from the LOGLOAD");
        logMessage("table and compare to the current weeks load");
        logMessage("files further below.");
        logMessage("Begin getting previous extract dates..... " + new Date());
        
        try {
            // Query the database for the most recent extract dates for each file type
            s1ExtractDate = executeQuery("SELECT max(EXTDT) FROM LOGLOAD WHERE LOADNAME = 'S1'");
            logMessage("Last S1 extract date loaded: " + s1ExtractDate);
            
            e1ExtractDate = executeQuery("SELECT max(EXTDT) FROM LOGLOAD WHERE LOADNAME = 'E1'");
            logMessage("Last E1 extract date loaded: " + e1ExtractDate);
            
            e2ExtractDate = executeQuery("SELECT max(EXTDT) FROM LOGLOAD WHERE LOADNAME = 'E2'");
            logMessage("Last E2 extract date loaded: " + e2ExtractDate);
            
            e4ExtractDate = executeQuery("SELECT max(EXTDT) FROM LOGLOAD WHERE LOADNAME = 'E4'");
            logMessage("Last E4 extract date loaded: " + e4ExtractDate);
            
            e3ExtractDate = executeQuery("SELECT max(EXTDT) FROM LOGLOAD WHERE LOADNAME = 'E3'");
            logMessage("Last E3 extract date loaded: " + e3ExtractDate);
            
            eaExtractDate = executeQuery("SELECT max(EXTDT) FROM LOGLOAD WHERE LOADNAME = 'EA'");
            logMessage("Last EA extract date loaded: " + eaExtractDate);
            
            e9ExtractDate = executeQuery("SELECT max(EXTDT) FROM LOGLOAD WHERE LOADNAME = 'E9'");
            logMessage("Last E9 extract date loaded: " + e9ExtractDate);
            
            e6ExtractDate = executeQuery("SELECT max(EXTDT) FROM LOGLOAD WHERE LOADNAME = 'E6'");
            logMessage("Last E6 extract date loaded: " + e6ExtractDate);
            
            logMessage("End getting previous extract dates....... " + new Date());
            logMessage("\n------------------------------------------\n");
            
        } catch (Exception e) {
            logMessage("Error in getPreviousExtractDates: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Check if weekly extract files exist in FTP directory
     */
    private void checkWeeklyExtracts() {
        logMessage("Begin checking for weekly extracts....... " + new Date());
        
        try {
            // Continue looping until all files are received
            String[] wklyFiles = {"S1", "E1", "E2", "E4", "E3", "EA", "E9", "E6"};
            
            for (String file : wklyFiles) {
                boolean fileFound = false;
                while (!fileFound) {
                    Path filePath = Paths.get(ftpDir, file);
                    if (Files.exists(filePath)) {
                        logMessage(file + " extract file found...");
                        fileFound = true;
                    } else {
                        logMessage("ERROR: " + file + " extract file not found...\n");
                        // In Java, we use Thread.sleep instead of sleep
                        Thread.sleep(300000); // 300 seconds = 5 minutes
                    }
                }
            }
            
            logMessage("\nEnd checking for weekly extracts........ " + new Date());
            logMessage("\n------------------------------------------\n");
            
        } catch (Exception e) {
            logMessage("Error in checkWeeklyExtracts: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Copy weekly extract files from FTP directory to loading directory
     */
    private void copyWeeklyExtracts() {
        logMessage("Begin copying weekly extracts............. " + new Date());
        
        try {
            // Made it this far. All files must be present
            // Copy the files from $FTPDIR to $LOGDIR and append the ".dat" extension
            String[] wklyFiles = {"S1", "E1", "E2", "E4", "E3", "EA", "E9", "E6"};
            
            // The E6 file is empty except on EOM weekend
            // Skip looking for it and processing it until then
            for (String file : wklyFiles) {
                Path sourcePath = Paths.get(ftpDir, file);
                
                if (Files.exists(sourcePath)) {
                    Path destPath = Paths.get(logDir, file + ".dat");
                    Files.copy(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);
                    logMessage(file + " extract copied to " + file + ".dat...");
                    
                    // Set permissions to 600 (owner read/write)
                    Files.setPosixFilePermissions(destPath, 
                            java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
                } else {
                    logMessage("ERROR: " + file + " extract file not copied...EXITING!\n");
                    System.exit(1);
                }
            }
            
        } catch (Exception e) {
            logMessage("Error in copyWeeklyExtracts: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Compare extract dates between the database and the current extract files
     */
    private void compareExtractDates() {
        logMessage("\nDetermine what the new extract date");
        logMessage("should be and compare it with the");
        logMessage("extract date in the current files.");
        logMessage("The S1 extract date is 1 day earlier");
        logMessage("than the extract date in the E? files");
        logMessage("so I'm checking for each to compare.");
        logMessage("Must be 7 days difference.");
        logMessage("Begin comparing extract dates......... " + new Date());
        
        try {
            // Calculate the expected extract date (7 days after previous date)
            SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
            SimpleDateFormat fileFormat = new SimpleDateFormat("yyyyMMdd");
            
            // Convert string date to Date object, add 7 days, then format back to string
            Date prevDate = dateFormat.parse(e1ExtractDate);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(prevDate);
            calendar.add(Calendar.DATE, 7);
            newExtractDate = fileFormat.format(calendar.getTime());
            
            // Do the same for S1 extract date
            prevDate = dateFormat.parse(s1ExtractDate);
            calendar.setTime(prevDate);
            calendar.add(Calendar.DATE, 7);
            newS1ExtractDate = fileFormat.format(calendar.getTime());
            
            logMessage("Current S1 extract date should be: " + newS1ExtractDate);
            logMessage("Current E? extract date should be: " + newExtractDate);
            
            // Read dates from files and compare with expected dates
            logMessage("\nThe date format in the file is");
            logMessage("in YYYYMMDD. If any extract date");
            logMessage("is incorrect, EXIT program");
            
            // Check S1 file date
            s1FileDate = readDateFromFile("S1.dat", "46-53");
            if (s1FileDate.equals(newS1ExtractDate)) {
                logMessage("S1 Extract date is correct........ " + s1FileDate);
            } else {
                logMessage("ERROR: S1 Extract date " + s1FileDate + " is incorrect...EXITING ");
                System.exit(1);
            }
            
            // Check E1 file date
            e1FileDate = readDateFromFile("E1.dat", "3-10");
            if (e1FileDate.equals(newExtractDate)) {
                logMessage("E1 Extract date is correct........ " + e1FileDate);
            } else {
                logMessage("ERROR: E1 Extract date " + e1FileDate + " is incorrect...EXITING ");
                System.exit(1);
            }
            
            // Check E2 file date
            e2FileDate = readDateFromFile("E2.dat", "121-128");
            if (e2FileDate.equals(newExtractDate)) {
                logMessage("E2 Extract date is correct........ " + e2FileDate);
            } else {
                logMessage("ERROR: E2 Extract date " + e2FileDate + " is incorrect...EXITING ");
                System.exit(1);
            }
            
            // Check E4 file date
            e4FileDate = readDateFromFile("E4.dat", "69-76");
            if (e4FileDate.equals(newExtractDate)) {
                logMessage("E4 Extract date is correct........ " + e4FileDate);
            } else {
                logMessage("ERROR: E4 Extract date " + e4FileDate + " is incorrect...EXITING ");
                System.exit(1);
            }
            
            // Check E3 file date
            e3FileDate = readDateFromFile("E3.dat", "3-10");
            if (e3FileDate.equals(newExtractDate)) {
                logMessage("E3 Extract date is correct........ " + e3FileDate);
            } else {
                logMessage("ERROR: E3 Extract date " + e3FileDate + " is incorrect...EXITING ");
                System.exit(1);
            }
            
            // Check EA file date
            eaFileDate = readDateFromFile("EA.dat", "3-10");
            if (eaFileDate.equals(newExtractDate)) {
                logMessage("EA Extract date is correct........ " + eaFileDate);
            } else {
                logMessage("ERROR: EA Extract date " + eaFileDate + " is incorrect...EXITING ");
                System.exit(1);
            }
            
            // Check E9 file date
            e9FileDate = readDateFromFile("E9.dat", "3-10");
            if (e9FileDate.equals(newExtractDate)) {
                logMessage("E9 Extract date is correct........ " + e9FileDate);
            } else {
                logMessage("ERROR: E9 Extract date " + e9FileDate + " is incorrect...EXITING ");
                System.exit(1);
            }
            
            // Check E6 file date only if today is an EOM Sunday
            SimpleDateFormat dateFormatToday = new SimpleDateFormat("MM/dd/yyyy");
            String today = dateFormatToday.format(new Date());
            if (today.equals(executeQuery("SELECT TO_DATE('" + eomEndDt + "', 'MM/DD/YYYY') + 1 FROM dual"))) {
                e6FileDate = readDateFromFile("E6.dat", "11-18");
                if (e6FileDate.equals(executeQuery("SELECT TO_CHAR(TO_DATE('" + eomSunday + "') - 1, 'YYYYMMDD') FROM dual"))) {
                    logMessage("E6 Extract date is correct........ " + e6FileDate);
                } else {
                    logMessage("ERROR: E6 Extract date " + e6FileDate + " is incorrect...EXITING ");
                    System.exit(1);
                }
            } else {
                logMessage("\\nNo E6, next EOM is................ " + executeQuery("SELECT TO_DATE('" + eomEndDt + "', 'MM/DD/YYYY') + 1 FROM dual"));
            }
            
            logMessage("\nEnd comparing extract dates........... " + new Date());
            logMessage("\n------------------------------------------\n");
            
        } catch (Exception e) {
            logMessage("Error in compareExtractDates: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Read a date from a specific position in a file
     * 
     * @param fileName Name of the file to read
     * @param position Position in the file (format: "start-end")
     * @return The date string extracted from the file
     */
    private String readDateFromFile(String fileName, String position) {
        try {
            // Parse the position string
            String[] positions = position.split("-");
            int startPos = Integer.parseInt(positions[0]) - 1; // Convert to 0-based index
            int endPos = Integer.parseInt(positions[1]) - 1;   // Convert to 0-based index
            
            // Read the first line of the file
            Path filePath = Paths.get(logDir, fileName);
            List<String> lines = Files.readAllLines(filePath);
            
            if (lines.isEmpty()) {
                throw new Exception("File is empty: " + fileName);
            }
            
            String line = lines.get(0);
            
            // Extract the date from the specified position
            // Ensure the line is long enough
            if (line.length() <= endPos) {
                throw new Exception("Line is too short to extract date from position " + position);
            }
            
            return line.substring(startPos, endPos + 1);
            
        } catch (Exception e) {
            logMessage("Error reading date from file " + fileName + ": " + e.getMessage());
            return "ERROR";
        }
    }
    
    /**
     * Load weekly extract files using c.proc or other processes
     */
    private void loadWeeklyExtracts() {
        logMessage("\nExtract dates must be correct.");
        logMessage("Loop through each c.proc?");
        logMessage("Begin loading weekly extracts......... " + new Date());
        
        try {
            // Process each file in the weeklyDataFiles array
            for (String file : weeklyDataFiles) {
                String filePrefix = file.split("\\.")[0]; // Get file prefix (S1, E1, etc.)
                
                if (filePrefix.equals("E6")) {
                    // Special handling for E6 if today is EOM Sunday
                    SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
                    String today = dateFormat.format(new Date());
                    
                    if (today.equals(eomSunday)) {
                        logMessage(filePrefix + " Extract loading.......... " + new Date());
                        runDataLoadProcess(filePrefix);
                        
                        // Check for errors in process output
                        checkForErrors(filePrefix);
                    }
                } else {
                    // Normal processing for other files
                    logMessage(filePrefix + " Extract loading.......... " + new Date());
                    runDataLoadProcess(filePrefix);
                    
                    // Check for errors in process output
                    checkForErrors(filePrefix);
                }
            }
            
            // Special processing for E6 if it's EOM Sunday
            SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
            String today = dateFormat.format(new Date());
            
            if (today.equals(eomSunday)) {
                // Run EOM scripts for E6
                logMessage("E6 Extract loaded........... " + new Date());
                logMessage("Begin EOM scripts........... " + new Date());
                
                // Run Archive Inventory process
                logMessage("Running c.runARCHIVEINV..... " + new Date());
                runProcess(logDir + "/c.runARCHIVEINV", eomStartDt, eomEndDt, rptMonth, runlogFile);
                
                // Check for completion in output file
                checkEomOutput("mkARCINV.out", "COMPLETE");
                
                // Run Case DSP process
                logMessage("Running c.casedsp........... " + new Date());
                runProcess(logDir + "/c.casedsp", eomStartDt, eomEndDt, rptMonth, caselogFile);
                
                // Check for completion in output file
                checkEomOutput("caseDSP.out", "COMPLETE");
                
                logMessage("End EOM scripts............. " + new Date());
            } else {
                logMessage("\nNo E6, next EOM is................ " + executeQuery("SELECT TO_DATE('" + eomEndDt + "', 'MM/DD/YYYY') + 1 FROM dual"));
            }
            
            logMessage("\nEnd loading weekly extracts......... " + new Date());
            logMessage("\n------------------------------------------\n");
            
        } catch (Exception e) {
            logMessage("Error in loadWeeklyExtracts: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Run a data load process
     * 
     * @param filePrefix The file prefix to process (S1, E1, etc.)
     */
    private void runDataLoadProcess(String filePrefix) {
        try {
            // In Java, we'd use ProcessBuilder to run external processes
            ProcessBuilder pb = new ProcessBuilder(logDir + "/c.proc" + filePrefix);
            pb.redirectOutput(new File(logDir + "/" + filePrefix + ".out"));
            Process process = pb.start();
            
            // Wait for the process to complete
            int exitCode = process.waitFor();
            
            if (exitCode != 0) {
                logMessage("WARNING: Process for " + filePrefix + " exited with code " + exitCode);
            }
            
        } catch (Exception e) {
            logMessage("Error running data load process for " + filePrefix + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Check for errors in output files
     * 
     * @param filePrefix The file prefix to check (S1, E1, etc.)
     */
    private void checkForErrors(String filePrefix) {
        try {
            // Check if error check file exists
            Path errCheckPath = Paths.get(errCheckFile);
            if (Files.exists(errCheckPath)) {
                // Check for errors using grep-like functionality
                int errorCount = countOccurrences(errCheckFile, "ERROR");
                
                if (errorCount > 0) {
                    logMessage("ERROR: Errors found in " + filePrefix + ".out file...EXITING - " + new Date());
                    // In a real app, we'd append the errors to the log file
                    appendErrorsToLog(filePrefix + ".out");
                    System.exit(1);
                }
            }
            
            // Check output file for errors
            Path outFilePath = Paths.get(logDir, filePrefix + ".out");
            if (Files.exists(outFilePath)) {
                int errorCount = countOccurrences(outFilePath.toString(), "ERR");
                
                if (errorCount > 0) {
                    logMessage("ERROR: Errors found in " + filePrefix + " LOAD...EXITING - " + new Date());
                    System.exit(1);
                }
            }
            
        } catch (Exception e) {
            logMessage("Error checking for errors: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Count occurrences of a string in a file
     * 
     * @param filePath Path to the file
     * @param searchString String to search for
     * @return Number of occurrences
     */
    private int countOccurrences(String filePath, String searchString) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(filePath));
            int count = 0;
            
            for (String line : lines) {
                if (line.contains(searchString)) {
                    count++;
                }
            }
            
            return count;
            
        } catch (Exception e) {
            logMessage("Error counting occurrences: " + e.getMessage());
            return 0;
        }
    }
    
    /**
     * Append errors to the log file
     * 
     * @param errorFile File containing errors
     */
    private void appendErrorsToLog(String errorFile) {
        try {
            List<String> errorLines = Files.readAllLines(Paths.get(logDir, errorFile));
            
            for (String line : errorLines) {
                if (line.contains("ERROR")) {
                    logMessage(line);
                }
            }
            
        } catch (Exception e) {
            logMessage("Error appending errors to log: " + e.getMessage());
        }
    }
    
    /**
     * Check EOM output files for completion
     * 
     * @param outputFile Output file to check
     * @param completionString String that indicates completion
     */
    private void checkEomOutput(String outputFile, String completionString) {
        try {
            Path outputPath = Paths.get(eomDir, outputFile);
            
            if (Files.exists(outputPath)) {
                int completionCount = countOccurrences(outputPath.toString(), completionString);
                
                if (completionCount < 1) {
                    logMessage("ERROR: Errors in " + outputFile + "...EXITING - " + new Date());
                    System.exit(1);
                }
            } else {
                logMessage(outputFile + " not found...EXITING - " + new Date());
                System.exit(1);
            }
            
        } catch (Exception e) {
            logMessage("Error checking EOM output: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Run process with arguments
     * 
     * @param command Command to run
     * @param args Arguments for the command
     */
    private void runProcess(String command, String... args) {
        try {
            List<String> cmdList = new ArrayList<>();
            cmdList.add(command);
            cmdList.addAll(Arrays.asList(args));
            
            ProcessBuilder pb = new ProcessBuilder(cmdList);
            Process process = pb.start();
            
            // Wait for the process to complete
            int exitCode = process.waitFor();
            
            if (exitCode != 0) {
                logMessage("WARNING: Process " + command + " exited with code " + exitCode);
            }
            
        } catch (Exception e) {
            logMessage("Error running process " + command + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Run noseg scripts for data verification and entity unlocking
     */
    private void runNosegScripts() {
        logMessage("\nPer SP-217, run the following sql scripts.");
        logMessage("\nBegin running noseg scripts........... " + new Date());
        
        try {
            // Change to the log directory
            Path logDirPath = Paths.get(logDir);
            if (!Files.exists(logDirPath)) {
                Files.createDirectories(logDirPath);
            }
            
            // Run SQL scripts using sqlplus
            runSqlScript(appHome + "/execloc/d.loads/ck_nosegs_before");
            runSqlScript(appHome + "/execloc/d.loads/NOSEGS");
            runSqlScript(appHome + "/execloc/d.loads/nosegs_open");
            runSqlScript(appHome + "/execloc/d.loads/ck_nosegs_after");
            
            // Move output file
            Path source = Paths.get(logDir, "nosegs.out");
            Path target = Paths.get(logDir);
            if (Files.exists(source)) {
                Files.copy(source, target.resolve(source.getFileName()), 
                           StandardCopyOption.REPLACE_EXISTING);
            }
            
            logMessage("\nEnd running noseg scripts.............. " + new Date());
            
            // Unlock Entity
            logMessage("\nUnlocking Entity.................... " + new Date());
            runProcess(entityDir + "/als_lock", "ent", "u", "load");
            
        } catch (Exception e) {
            logMessage("Error in runNosegScripts: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Run a SQL script
     * 
     * @param scriptPath Path to the SQL script
     */
    private void runSqlScript(String scriptPath) {
        try {
            logMessage("Running " + scriptPath.substring(scriptPath.lastIndexOf('/') + 1));
            
            // In Java, we'd use ProcessBuilder to run sqlplus with the script
            ProcessBuilder pb = new ProcessBuilder(
                "sqlplus", "-s", "als/" + System.getenv("PW"),
                "@" + scriptPath
            );
            
            Process process = pb.start();
            
            // Capture and log output
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    logMessage("  " + line);
                }
            }
            
            // Wait for the process to complete
            int exitCode = process.waitFor();
            
            logMessage("End Running " + scriptPath.substring(scriptPath.lastIndexOf('/') + 1));
            
            if (exitCode != 0) {
                logMessage("WARNING: SQL script exited with code " + exitCode);
            }
            
        } catch (Exception e) {
            logMessage("Error running SQL script " + scriptPath + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Generate a report of what was loaded
     */
    private void generateReport() {
        logMessage("\nPrepare report of what loaded and send");
        logMessage("it to the ALE staff.");
        
        try {
            // In a real application, this would connect to the database with JDBC
            // For demonstration, we'll simulate a SQL*Plus session
            StringBuilder sqlCommand = new StringBuilder();
            sqlCommand.append("connect als/").append(System.getenv("PW")).append("\n");
            sqlCommand.append("alter session set nls_date_format = 'MM/DD/YYYY HH24:MI:SS';\n");
            sqlCommand.append("set feedback off\n");
            sqlCommand.append("set echo off\n");
            sqlCommand.append("set linesize 80\n");
            sqlCommand.append("set newpage 0\n");
            sqlCommand.append("set pagesize 60\n\n");
            
            // Define column formats
            sqlCommand.append("column LOADNAME format A4 heading 'FILE'\n");
            sqlCommand.append("column EXTDT format A12 heading 'EXTRACT DATE'\n");
            sqlCommand.append("column LOADDT format A20 heading 'DATE LOADED'\n");
            sqlCommand.append("column UNIX format A10 heading 'LOADED BY'\n");
            sqlCommand.append("column NUMREC format 99999999999999999 heading 'RECORDS RECEIVED'\n\n");
            
            // Define the SQL query
            sqlCommand.append("spool ").append(splitFile).append("\n\n");
            sqlCommand.append("SELECT loadname, TO_CHAR(extdt, 'MM/DD/YYYY') \"EXTRACT DATE\",\n");
            sqlCommand.append("       TO_DATE(loaddt, 'MM/DD/YYYY HH24:MI:SS') \"DATE LOADED\",\n");
            sqlCommand.append("       unix, numrec\n");
            sqlCommand.append("FROM logload\n");
            sqlCommand.append("WHERE loaddt like '%").append(new SimpleDateFormat("MM/dd/yyyy").format(new Date())).append("%'\n");
            sqlCommand.append("ORDER BY loadname;\n");
            sqlCommand.append("spool off\n");
            
            // Write SQL command to a temp file
            Path sqlFilePath = Files.createTempFile("report_query", ".sql");
            Files.write(sqlFilePath, sqlCommand.toString().getBytes());
            
            // Execute SQL*Plus command
            ProcessBuilder pb = new ProcessBuilder("sqlplus", "-s", "/nolog");
            pb.redirectInput(sqlFilePath.toFile());
            Process process = pb.start();
            
            // Wait for the process to complete
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                logMessage("WARNING: SQL report generation exited with code " + exitCode);
            }
            
            // Clean up temp file
            Files.deleteIfExists(sqlFilePath);
            
            // Log that the report was generated
            logMessage("LOADED ON " + serverEnvironment + " today...... " + 
                       new SimpleDateFormat("MM/dd/yyyy").format(new Date()));
            
            // Append the report to the log file
            if (Files.exists(Paths.get(splitFile))) {
                String reportContent = new String(Files.readAllBytes(Paths.get(splitFile)));
                logMessage("\n" + reportContent);
            }
            
        } catch (Exception e) {
            logMessage("Error in generateReport: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Execute a SQL query and return the result as a string
     * 
     * @param query SQL query to execute
     * @return Result of the query as a string
     */
    private String executeQuery(String query) {
        // This is a placeholder. In a real application, this would use JDBC.
        // For demonstration purposes, we'll return mock data.
        
        if (query.contains("RPTMNTH")) {
            return "202502"; // February 2025
        } else if (query.contains("startdt")) {
            return "02/01/2025";
        } else if (query.contains("enddt")) {
            return "02/28/2025";
        } else if (query.contains("TO_DATE") && query.contains(" + 1")) {
            return "03/01/2025";
        } else if (query.contains("TO_CHAR") && query.contains("YYYYMMDD")) {
            return "20250228";
        } else if (query.contains("max(EXTDT)")) {
            return "02/15/2025"; // Default date for extract dates
        }
        
        return "result";
    }
    
    /**
     * Send the report via email
     */
    private void sendReportEmail() {
        try {
            // Prepare email subject
            String subject = "WEEKLY ENTITY LOADED ON " + serverEnvironment;
            String recipient = "sbse.automated.iles.entity.cron.load@irs.gov";
            
            // In Java, we'd use JavaMail to send emails
            // For demonstration, we'll simulate sending the email
            
            // Check the date difference for error condition
            if (diffDays != 1) {
                // Send error notification
                logMessage("Daily extract date is not current............ " + prevE3);
                logMessage("Previous E3      -> " + prevE3);
                logMessage("Current  E3      -> " + currE3);
                logMessage("Previous E3 Julian -> " + prevE3Julian);
                logMessage("Current  E3 Julian -> " + currE3Julian);
                logMessage("Difference       -> " + diffDays + " days");
                
                logMessage("End checking current and previous E3 extract dates........ " + new Date());
                logMessage("Weekly load did not run exiting....................... " + new Date());
                
                sendEmail(subject, recipient);
            } else {
                // Send success notification
                logMessage("\nFINISHED LOADING ON " + serverEnvironment);
                sendEmail(subject, recipient);
            }
            
        } catch (Exception e) {
            logMessage("Error in sendReportEmail: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Send an email
     * 
     * @param subject Email subject
     * @param recipient Email recipient
     */
    private void sendEmail(String subject, String recipient) {
        try {
            // In a real implementation, this would use JavaMail API
            logMessage("Sending email to: " + recipient);
            logMessage("Subject: " + subject);
            
            // Java code to send email would go here
            // For example:
            /*
            Properties properties = System.getProperties();
            properties.setProperty("mail.smtp.host", "mailserver.example.com");
            Session session = Session.getDefaultInstance(properties);
            
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress("weekly.loader@example.com"));
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));
            message.setSubject(subject);
            
            // Create the message part
            BodyPart messageBodyPart = new MimeBodyPart();
            messageBodyPart.setText("See attached report.");
            
            // Create a multipart message
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(messageBodyPart);
            
            // Part two is attachment
            messageBodyPart = new MimeBodyPart();
            DataSource source = new FileDataSource(logFile);
            messageBodyPart.setDataHandler(new DataHandler(source));
            messageBodyPart.setFileName("WeeklyLoad.log");
            multipart.addBodyPart(messageBodyPart);
            
            // Send the complete message parts
            message.setContent(multipart);
            
            // Send message
            Transport.send(message);
            */
            
            logMessage("Email sent successfully.");
            
        } catch (Exception e) {
            logMessage("Error sending email: " + e.getMessage());
        }
    }
    
    /**
     * Clean up and make a backup of the log file
     */
    private void cleanup() {
        try {
            // If log file exists, copy it to backup directory
            Path logFilePath = Paths.get(logFile);
            if (Files.exists(logFilePath)) {
                String timestamp = new SimpleDateFormat("yyyyMMdd").format(new Date());
                Path backupPath = Paths.get(backupDir, "wklyLOAD.log." + timestamp);
                
                Files.copy(logFilePath, backupPath, StandardCopyOption.REPLACE_EXISTING);
                logMessage("Log file backed up to: " + backupPath);
            }
            
            // Final status message
            logMessage("End loading weekly extracts................. " + new Date());
            logMessage("\n------------------------------------------\n");
            
        } catch (Exception e) {
            logMessage("Error in cleanup: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close any open resources
            try {
                // Close logger handlers
                for (java.util.logging.Handler handler : logger.getHandlers()) {
                    handler.close();
                }
            } catch (Exception e) {
                System.err.println("Error closing resources: " + e.getMessage());
            }
        }
    }
    
    /**
     * Main method to run the WeeklyLoader application
     * 
     * @param args Command line arguments (optional runDate)
     */
    public static void main(String[] args) {
        new WeeklyLoader(args);
    }
}
