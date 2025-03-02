# WeeklyLoader Application

## Overview
WeeklyLoader is a Java application designed to handle the loading of weekly extract files into the system. It automates several key processes:

- Checking extract dates
- Backing up previous week files
- Calculating End-of-Month (EOM) dates
- Loading weekly extracts
- Generating reports
- Sending notification emails

This application is a Java conversion of an original Unix shell script.

## Prerequisites

- Java Development Kit (JDK) 17 or higher
- Oracle Database access
- Access to the necessary file system directories
- Required environment variables properly configured

## Environment Variables

The following environment variables must be set before running the application:

```
APP_HOME        - The application home directory (defaults to "/als-ALS/app")
SERVER_NAME     - The name of the current server
DEV_NAME        - The name of the development server
TEST_NAME       - The name of the test server
PROD_NAME       - The name of the production server
PW              - The database password
```

## Directory Structure

The application expects the following directory structure, based on the APP_HOME environment variable:

```
APP_HOME/
├── execloc/
│   ├── d.entity/
│   │   └── errcheck
│   ├── d.common/
│   └── d.loads/
│       ├── ck_nosegs_before
│       ├── NOSEGS
│       ├── nosegs_open
│       └── ck_nosegs_after
├── entity/
│   └── d.ICS/
│       └── d.NEWDATA/
│           ├── d.BACKUP/
│           └── d.eom/
└── FTP/
    ├── S1
    ├── E1
    ├── E2
    ├── E4
    ├── E3
    ├── EA
    ├── E9
    └── E6
```

## Building the Application

1. Compile the Java source file:

```shell
javac WeeklyLoader.java
```

2. Create an executable JAR file (optional):

```shell
jar cfe WeeklyLoader.jar WeeklyLoader WeeklyLoader.class
```

## Running the Application

### Basic Usage

Run the application without any arguments to use the current date:

```shell
java WeeklyLoader
```

Or with Java JAR:

```shell
java -jar WeeklyLoader.jar
```

### Custom Date

Run the application with a specific date (MM/DD/YYYY format):

```shell
java WeeklyLoader 03/01/2025
```

Or with Java JAR:

```shell
java -jar WeeklyLoader.jar 03/01/2025
```

## Process Flow

The application follows this process flow:

1. Setup logging
2. Initialize environment variables and directories
3. Check extract dates by comparing current and previous E3 extract dates
4. Back up previous week's files
5. Calculate EOM dates
6. Get previous extract dates from database
7. Check for weekly extract files in FTP directory
8. Copy weekly extract files to loading directory
9. Compare extract dates between database and current files
10. Load weekly extracts
11. Run NOSEG scripts for data verification
12. Generate report
13. Send report email
14. Clean up and back up log files

## Logging

The application logs its activities to:

1. Console output
2. weeklyLoader.log file in the current directory
3. wklyLOAD.log in the APP_HOME/entity/d.ICS/d.NEWDATA directory

After completion, the wklyLOAD.log is backed up with a timestamp to the backup directory.

## Error Handling

The application checks for various error conditions:

- Missing extract files
- Incorrect extract dates
- Errors in data loading processes
- Database errors

When critical errors are encountered, the application will log the error, send a notification email, and exit with a non-zero status code.

## End-of-Month Processing

The application includes special handling for End-of-Month (EOM) dates:

- Detects when the current date is an EOM Sunday
- Processes the E6 extract file (which is only processed during EOM)
- Runs additional EOM-specific scripts (c.runARCHIVEINV and c.casedsp)

## Database Requirements

The application expects the following database schemas:

- LOGLOAD table for tracking extract loads
- ENTMONTH table for EOM date information

## Troubleshooting

If the application fails:

1. Check the log files for specific error messages
2. Verify that all required environment variables are set
3. Ensure the database is accessible
4. Confirm that the required directory structure exists
5. Verify that the weekly extract files are present in the FTP directory
6. Check file permissions

## Support
