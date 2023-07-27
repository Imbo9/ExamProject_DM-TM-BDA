package org.example;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

class Exercise30 {
    public static void main(String[] args) {
        String inputPath;
        String outputPath;

        //inputPath = args[0];
        //outputPath = args[1];
        inputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\Exercise30\\ex30_data\\log.txt";
        outputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\testOutput\\ex30_out";

        System.out.println(inputPath);
        System.out.println(outputPath);

        // Create a Spark Session object and set the name of the application
        SparkSession ss = SparkSession
                .builder()
                .master("local")
                .appName("Spark Exercise #30")
                .config("spark.master", "local")
                .getOrCreate();

		// Create a DataFrame from persons.csv
		DataFrameReader dfr = ss.read().
				format("csv");
                //.option("header", true)
                //.option("inferSchema", true);

		Dataset<Row> logDF = dfr.load(inputPath);
        System.out.println(logDF.toString());

        Dataset<Row> googleDF = logDF.filter(lower(col("_c0")).contains("google"));
        System.out.println(googleDF);

        googleDF.write()
                .format("text")
                .save(outputPath);

        System.exit(1);
    }
}

class Schema {
    public static void main(String[] args) {
        String inputPath;
        String outputPath;

        //inputPath = args[0];
        //outputPath = args[1];
        inputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\Exercise30\\ex30_data\\log.txt";
        outputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\test\\exSchema_out.txt";

        System.out.println(inputPath);
        System.out.println(outputPath);

        // Create a Spark Session object and set the name of the application
        SparkSession ss = SparkSession
                .builder()
                .master("local")
                .appName("Spark Exercise #30")
                .config("spark.master", "local")
                .getOrCreate();

        // Create a DataFrame from persons.csv
        DataFrameReader dfr = ss.read().
                format("csv")
                .option("header", true)
                .option("inferSchema", true);

        Dataset<Row> logDF = dfr.load(inputPath);

        System.out.println(logDF.schema());

        StructType test = logDF.schema();

        test.iterator().foreach(x -> {
            System.out.println(x);
            return null;
        });

        test.iterator().foreach(x -> {
            return test_f(x);
        });

        test.iterator().foreach(x -> {
            test_f2(x);
            return null;
        });

        test.printTreeString();
        // logDF.show();

        System.exit(1);
    }

    public static boolean test_f(StructField sf) {
        System.out.println(sf);
        return true;
    };

    public static void test_f2(StructField sf) {
        System.out.println(sf);
    };
}

class Exercise31 {
    public static void main(String[] args) {
        String inputPath;
        String outputPath;

        //inputPath = args[0];
        //outputPath = args[1];
        inputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\Exercise31\\ex31_data\\log.txt";
        outputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\testOutput\\ex31_out";

        System.out.println(inputPath);
        System.out.println(outputPath);

        // Create a Spark Session object and set the name of the application
        SparkSession ss = SparkSession
                .builder()
                .master("local")
                .appName("Spark Exercise #31")
                .config("spark.master", "local")
                .getOrCreate();

        // Create a DataFrame from persons.csv
        DataFrameReader dfr = ss.read().
                format("csv");
        //.option("header", true)
        //.option("inferSchema", true);

        Dataset<Row> logDF = dfr.load(inputPath);
        Dataset<Row> googleDF = logDF.filter(lower(col("_c0")).contains("google"));

        // Extract the IP address from the selected lines
        // It can be implemented by using the map transformation
        Dataset<String> ipDF = googleDF.map(
                (MapFunction<Row, String>) logLine -> {
                    String[] parts = logLine.getString(0).split(" ");
                    String ip = parts[0];
                    return ip;
                },
                Encoders.STRING());

        // Apply the distinct transformation
        Dataset<String> ipDistinctDF = ipDF.distinct();

        ipDistinctDF.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("text")
                .save(outputPath);

        System.out.println("logDF");
        logDF.printSchema();
        logDF.show(false);
        System.out.println("-------------------------------------------------------");
        System.out.println("googleDF");
        googleDF.printSchema();
        googleDF.show(false);
        System.out.println("-------------------------------------------------------");
        System.out.println("ipDistinctDF");
        ipDistinctDF.printSchema();
        ipDistinctDF.show(false);

        ss.stop();
        System.exit(1);
    }
}

class Exercise32 {
    public static void main(String[] args) {
        String inputPath;

        //inputPath = args[0];
        inputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\Exercise32\\ex32_data\\sensors.txt";
        System.out.println(inputPath);

        SparkSession ss = SparkSession
                .builder()
                .master("local")
                .appName("Spark Exercise #32")
                .config("spark.master", "local")
                .getOrCreate();

        DataFrameReader dfr = ss.read().
                format("csv");

        Dataset<Row> sensorsDF = dfr.load(inputPath);

        Dataset<Double> pmDF = sensorsDF.map(
                (MapFunction<Row, Double>) sensorsLine -> {
                    String[] parts = sensorsLine.getString(2).split(",");
                    Double pm = Double.parseDouble(parts[0]);
                    return pm;
                },
                Encoders.DOUBLE()
        );

        Dataset<Double> pmDistinctDF = pmDF.distinct();

        System.out.println("pmDistinctDFDF");
        pmDistinctDF.printSchema();
        pmDistinctDF.show(false);
        System.out.println("-------------------------------------------------------");

        Double max_pm = pmDistinctDF.agg(max("value")).as(Encoders.DOUBLE()).first();

        System.out.println("Max pm10: " + max_pm.toString());

        ss.stop();
        System.exit(1);
    }
}

class Exercise33 {
    public static void main(String[] args) {
        String inputPath;

        //inputPath = args[0];
        inputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\Exercise33\\ex33_data\\sensors.txt";
        System.out.println(inputPath);

        SparkSession ss = SparkSession
                .builder()
                .master("local")
                .appName("Spark Exercise #33")
                .config("spark.master", "local")
                .getOrCreate();

        DataFrameReader dfr = ss.read().
                format("csv");

        Dataset<Row> sensorsDF = dfr.load(inputPath);

        Dataset<Double> pmDF = sensorsDF.map(
                (MapFunction<Row, Double>) sensorsLine -> {
                    String[] parts = sensorsLine.getString(2).split(",");
                    Double pm = Double.parseDouble(parts[0]);
                    return pm;
                },
                Encoders.DOUBLE()
        );

        Dataset<Double> pmDistinctDF = pmDF.distinct();

        System.out.println("pmDistinctDFDF");
        pmDistinctDF.printSchema();
        pmDistinctDF.show(false);
        System.out.println("-------------------------------------------------------");

        // Get the first three maximum PM10 values
        Dataset<Double> top3MaxValues = pmDistinctDF.orderBy(col("value").desc()).limit(3);

        System.out.println("Top three maximum PM10 values:");
        top3MaxValues.show(false);

        ss.stop();
        System.exit(1);
    }
}

class Exercise34 {
    public static void main(String[] args) {
        String inputPath;

        //inputPath = args[0];
        inputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\Exercise34\\ex34_data\\sensors.txt";
        System.out.println(inputPath);

        SparkSession ss = SparkSession
                .builder()
                .master("local")
                .appName("Spark Exercise #34")
                .config("spark.master", "local")
                .getOrCreate();

        ss.sparkContext().setLogLevel("ERROR");

        DataFrameReader dfr = ss.read().
                format("csv");

        Dataset<Row> sensorsDF = dfr.load(inputPath);

        Dataset<Double> pm10DF = sensorsDF.map(
                (MapFunction<Row, Double>) sensorsLine -> {
                    String[] parts = sensorsLine.getString(2).split(",");
                    Double pm10 = Double.parseDouble(parts[0]);
                    return pm10;
                },
                Encoders.DOUBLE()
        );
        Dataset<Double> pm10DistinctDF = pm10DF.distinct();

        Double max_pm10 = pm10DistinctDF.agg(max("value")).as(Encoders.DOUBLE()).first();

        Dataset<Row> maxLinesDF = sensorsDF.filter(
                (FilterFunction<Row>) pm10Reading-> {
                    String[] parts = pm10Reading.getString(2).split(",");
                    Double pm10 = Double.parseDouble(parts[0]);

                    if (max_pm10.equals(pm10))
                        return true;
                    else
                        return false;
                }
        );


        System.out.println("sensorsDF");
        sensorsDF.printSchema();
        sensorsDF.show(false);
        System.out.println("-------------------------------------------------------");
        System.out.println("pm10DF");
        pm10DistinctDF.printSchema();
        pm10DistinctDF.show(false);
        System.out.println("-------------------------------------------------------");
        System.out.println("Max pm10: " + max_pm10.toString());
        System.out.println("-------------------------------------------------------");
        System.out.println("maxLinesDF");
        maxLinesDF.printSchema();
        maxLinesDF.show(false);


        ss.stop();
        System.exit(1);
    }
}

class Exercise35 {
    public static void main(String[] args) {
        String inputPath;

        //inputPath = args[0];
        inputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\Exercise35\\ex35_data\\sensors.txt";
        System.out.println(inputPath);

        SparkSession ss = SparkSession
                .builder()
                .master("local")
                .appName("Spark Exercise #35")
                .config("spark.master", "local")
                .getOrCreate();

        ss.sparkContext().setLogLevel("ERROR");

        DataFrameReader dfr = ss.read().
                format("csv");

        Dataset<Row> sensorsDF = dfr.load(inputPath);

        Dataset<Double> pm10DF = sensorsDF.map(
                (MapFunction<Row, Double>) sensorsLine -> {
                    String[] parts = sensorsLine.getString(2).split(",");
                    Double pm10 = Double.parseDouble(parts[0]);
                    return pm10;
                },
                Encoders.DOUBLE()
        );
        Dataset<Double> pm10DistinctDF = pm10DF.distinct();

        Double max_pm10 = pm10DistinctDF.agg(max("value")).as(Encoders.DOUBLE()).first();

        Dataset<Row> maxLinesDF = sensorsDF.filter(
                (FilterFunction<Row>) pm10Reading-> {
                    String[] parts = pm10Reading.getString(2).split(",");
                    Double pm10 = Double.parseDouble(parts[0]);

                    if (max_pm10.equals(pm10))
                        return true;
                    else
                        return false;
                }
        );

        Dataset<String> maxDatesDF = maxLinesDF.map(
                (MapFunction<Row, String>) sensorsLine -> {
                    String[] parts = sensorsLine.getString(1).split(",");
                    String maxDate = parts[0];
                    return maxDate;
                },
                Encoders.STRING()
        );
        Dataset<String> maxDatesDistinctDF = maxDatesDF.distinct();


        System.out.println("sensorsDF");
        sensorsDF.printSchema();
        sensorsDF.show(false);
        System.out.println("-------------------------------------------------------");
        System.out.println("pm10DF");
        pm10DistinctDF.printSchema();
        pm10DistinctDF.show(false);
        System.out.println("-------------------------------------------------------");
        System.out.println("Max pm10: " + max_pm10.toString());
        System.out.println("-------------------------------------------------------");
        System.out.println("maxDatesDF");
        maxDatesDistinctDF.printSchema();
        maxDatesDistinctDF.show(false);


        ss.stop();
        System.exit(1);
    }
}

class Exercise36 {
    public static void main(String[] args) {
        String inputPath;

        //inputPath = args[0];
        inputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\Exercise36\\ex36_data\\sensors.txt";
        System.out.println(inputPath);

        SparkSession ss = SparkSession
                .builder()
                .master("local")
                .appName("Spark Exercise #36")
                .config("spark.master", "local")
                .getOrCreate();

        ss.sparkContext().setLogLevel("ERROR");

        DataFrameReader dfr = ss.read().
                format("csv");

        Dataset<Row> sensorsDF = dfr.load(inputPath);

        Dataset<Double> pm10DF = sensorsDF.map(
                (MapFunction<Row, Double>) sensorsLine -> {
                    String[] parts = sensorsLine.getString(2).split(",");
                    Double pm10 = Double.parseDouble(parts[0]);
                    return pm10;
                },
                Encoders.DOUBLE()
        );
        Dataset<Double> pm10DistinctDF = pm10DF.distinct();

        Double[] pm10 = pm10DistinctDF.collectAsList().toArray(new Double[0]);

        Double pm10_mean = Arrays.stream(pm10).mapToDouble(Double::doubleValue).average().orElse(Double.NaN);


        System.out.println("sensorsDF");
        sensorsDF.printSchema();
        sensorsDF.show(false);
        System.out.println("-------------------------------------------------------");
        System.out.println("pm10DF");
        pm10DistinctDF.printSchema();
        pm10DistinctDF.show(false);
        System.out.println("-------------------------------------------------------");
        System.out.print("Array of pm10: ");
        Arrays.stream(pm10).forEach(s -> System.out.print(s + ", "));
        System.out.println();
        System.out.println("-------------------------------------------------------");
        System.out.println("Average of pm10: " + String.format("%.2f", pm10_mean));


        ss.stop();
        System.exit(1);
    }
}

/*
class Exercise37 {
    public static void main(String[] args) {
        String inputPath;
        String outputPath;
        //inputPath = args[0];
        //outputPath = args[1];
        inputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\Exercise37\\ex37_data\\sensors.txt";
        outputPath = "G:\\Reply\\progetto\\Esercizi\\Solutions\\testOutput\\ex37_out";
        System.out.println(inputPath);
        System.out.println(outputPath);

        SparkSession ss = SparkSession
                .builder()
                .master("local")
                .appName("Spark Exercise #37")
                .config("spark.master", "local")
                .getOrCreate();

        ss.sparkContext().setLogLevel("ERROR");

        DataFrameReader dfr = ss.read().
                format("csv");

        Dataset<Row> sensorsDF = dfr.load(inputPath);

        Dataset<String> sensorsNameDF = sensorsDF.map(
                (MapFunction<Row, String>) sensorsLine -> {
                    String sensorName = sensorsLine.toString().trim().split(",")[0];
                    return sensorName;
                },
                Encoders.STRING()
        );
        Dataset<String> sensorsNameDistinctDF = sensorsNameDF.distinct();

        Dataset<Row> resultDF;
        resultDF.i

        Double[] pm10 = pm10DistinctDF.collectAsList().toArray(new Double[0]);

        Double pm10_mean = Arrays.stream(pm10).mapToDouble(Double::doubleValue).average().orElse(Double.NaN);


        System.out.println("sensorsDF");
        sensorsDF.printSchema();
        sensorsDF.show(false);
        System.out.println("-------------------------------------------------------");
        System.out.println("pm10DF");
        pm10DistinctDF.printSchema();
        pm10DistinctDF.show(false);
        System.out.println("-------------------------------------------------------");
        System.out.print("Array of pm10: ");
        Arrays.stream(pm10).forEach(s -> System.out.print(s + ", "));
        System.out.println();
        System.out.println("-------------------------------------------------------");
        System.out.println("Average of pm10: " + String.format("%.2f", pm10_mean));


        ss.stop();
        System.exit(1);
    }
}
*/

public class Main {
    public static void main(String[] args) {
        //Exercise30.main(args);
        //Schema.main(args);
        //Exercise31.main(args);
        //Exercise32.main(args);
        //Exercise33.main(args);
        //Exercise34.main(args);
        //Exercise35.main(args);
        Exercise36.main(args);

        /*
        Class<?> classToTest = Exercise30.class; // Change this to the class you want to test

        try {
            // Invoke the main method of the selected class
            Method mainMethod = classToTest.getDeclaredMethod("main", String[].class);
            String[] mainArgs = null; // You can pass arguments to the main method if needed
            mainMethod.invoke(null, (Object) mainArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        */
    }
}

