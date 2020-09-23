package com.kdg.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecordApplication {

    private static Schema buildScheme() {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse("{\n" +
                "     \"type\": \"record\",\n" +
                "     \"namespace\": \"com.example\",\n" +
                "     \"name\": \"Customer\",\n" +
                "     \"doc\": \"Avro Schema for our Customer\",     \n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
                "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
                "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
                "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
                "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
                "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n" +
                "     ]\n" +
                "}");
    }

    private static GenericData.Record createCustomer(Schema schema, String firstName, String lastName, int age, float height, float weight, boolean automatedEmail) {
        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);

        customerBuilder.set("first_name", firstName);
        customerBuilder.set("last_name", lastName);
        customerBuilder.set("age", age);
        customerBuilder.set("height", height);
        customerBuilder.set("weight", weight);
        customerBuilder.set("automated_email", automatedEmail);

        GenericData.Record customer = customerBuilder.build();
        System.out.println(customer);

        return customer;
    }

    private static GenericData.Record createCustomerDefault(Schema schema, String firstName, String lastName, int age, float height, float weight) {
        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);

        customerBuilder.set("first_name", firstName);
        customerBuilder.set("last_name", lastName);
        customerBuilder.set("age", age);
        customerBuilder.set("height", height);
        customerBuilder.set("weight", weight);

        GenericData.Record customer = customerBuilder.build();
        System.out.println(customer);

        return customer;
    }

    private static void writeAvroFile(Schema schema, GenericData.Record customer, String filePath) {
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        try (
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)
        ) {
            dataFileWriter.create(customer.getSchema(), new File(filePath));
            dataFileWriter.append(customer);
            dataFileWriter.close();
            System.out.println("Written customer-generic.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }
    }

    private static void readAvroFile(String filePath) {
        final File file = new File(filePath);
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)){
            GenericRecord customerRead = dataFileReader.next();
            System.out.println("Successfully read avro file");
            System.out.println(customerRead.toString());

            // get the data from the generic record
            System.out.println("First name: " + customerRead.get("first_name"));

            // read a non existent field
            System.out.println("Non existent field: " + customerRead.get("not_here"));
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        // Define Schema
        Schema schema = buildScheme();

        // Create Generic Records
        GenericData.Record customer1 = createCustomer(schema, "John", "Doe", 26, 175f, 70.5f, false);
        GenericData.Record customer2 = createCustomerDefault(schema, "Billy", "Mory", 30, 185f, 75.5f);

        // Write to a Avro File
        writeAvroFile(schema, customer1, "customer-generic.avro");

        // Read from a Avro File
        readAvroFile("customer-generic.avro");
    }
}
