Main readme

This is a template project for team project of 2AMD15 2023.

Various comments in the Main.java file indicate where functionality needs to be implemented.
These comments are marked TODO.

You are allowed to change the layout of the Main.java file (such as method names and signatures) in any way you like,
and you can add new classes (possibly in new .java files). Do make sure that there exists a class called "Main" in a
file called "Main.java" which contains a method with signature "public static void main(String[] args)" though.

To build this template project into a single JAR file which you can run locally or submit to the server, you will need
to have a version of the Java Development Kit installed which supports at least Java 11 (https://jdk.java.net/17/).
You will also need to have a version of the management software Apache Maven installed (https://maven.apache.org/).

From a terminal (such as Command-Prompt on Windows) in the project directory (i.e., the directory containing this file),
run the command "mvn clean package" to compile the project into a JAR. The JAR will be called app.jar and be located in
the "target" directory which is located in the project directory.

In case the "mvn" command is not recognized by your terminal, you should add the "bin" directory inside the directory
into which you installed Maven (typically "C:\Program Files (x86)\apache-maven-3.8.4\bin") to your PATH environment
variable. The tutorial for installing and running Spark locally contains a section on editing the
PATH environment variable for Windows systems.

Good luck on the project!

=========

GenVec Readme

Hi there! This short readme is supposed to accompany a file called GenVec.jar.
Genvec.jar can be used to generate a dataset containing labelled vectors for the 2AMD15 project.

GenVec takes one required- and two optional runtime parameters, namely:
	-	your group number (without leading a leading zero, so 9 instead of 09)
	-	the number of vectors to generate (default value is 40)
	- 	the length of each vector (default value is 20)
	
You can run GenVec, after installing an up-to-date version of Java, by running the following command:
	java -jar group-number [number-of-vectors] [length-of-vectors]
	
from a command line in the directory which contains GenVec.jar. This will (over)write a file named vectors.csv in the same directory.

The contents of vector.csv should be the same each time you run GenVec with the same runtime parameters.
If this is not the case, please let us know.