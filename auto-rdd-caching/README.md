## Apache Spark in Java Manual

### Setup (reference: https://www.mrityunjay.com/post/apache-spark-java-getting-started/)
1. Install [Apache Spark 3.0.2](http://spark.apache.org/downloads.html) (any version should be fine)
    * you will need to unzip the zip file to some directory
3. [OPTIONAL] Install Maven if you want to use terminal (otherwise it comes with Eclipse/Intellij IDE)
4. Install [JDK 1.8](https://www.oracle.com/java/technologies/javase-downloads.html) if you do not have already
    * you can check in Eclipse or run `java -version` to verify
5. Load our Maven project into your IDE (see instruction [here](https://vaadin.com/learn/tutorials/import-maven-project-eclipse#_import_the_project))
    * NOTE: Open the `pom.xml` so the IDE can recognize it as a Maven project

### How to run
1. Package our Maven project to generate a .jar file (called `auto-rdd-caching-1.0.jar`)
    * [Eclipse/IntelliJ] package the Maven project (I used Intellij, but see [here](https://stackoverflow.com/questions/31742344/how-to-create-jar-file-from-maven-project-in-eclipse) for Eclipse)
    * [shell] run `mvn package`
2. Verify that `auto-rdd-caching-1.0.jar` is located in `AutoRDDCaching/auto-rdd-caching/target/`
3. Submit the jar to Spark
    * run `<path_to_spark-3.0.2-bin-hadoop2.7>/bin/spark-submit --class cs5614.auto_rdd_caching.OurSimpleApp --deploy-mode client <path_to_AutoRDDCaching>/auto-rdd-caching/target/auto-rdd-caching-1.0.jar` (in your terminal)
    * you should see Spark logs and results from the driver program

TIP: Add `<path_to_spark-3.0.2-bin-hadoop2.7>/bin/` as a $PATH variable in your `.bashrc` to simply run `spark-submit ...` instead
