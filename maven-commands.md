# Maven Commands for DailyLoad Application

# Create the project structure from scratch using Maven archetype
mvn archetype:generate -DgroupId=com.als -DartifactId=daily-load -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

# Navigate to the project directory
cd daily-load

# Create the necessary package structure
mkdir -p src/main/java/com/als
mkdir -p src/test/java/com/als

# Move the DailyLoad.java to the proper location
# Assuming DailyLoad.java is in your current directory:
cp DailyLoad.java src/main/java/com/als/

# Build the project
mvn clean compile

# Run tests
mvn test

# Package the application into a JAR
mvn package

# Create an executable JAR with dependencies
mvn clean compile assembly:single

# Install to local Maven repository
mvn install

# Run the application (after packaging)
java -jar target/daily-load-1.0-SNAPSHOT-jar-with-dependencies.jar

# Generate project site documentation
mvn site
