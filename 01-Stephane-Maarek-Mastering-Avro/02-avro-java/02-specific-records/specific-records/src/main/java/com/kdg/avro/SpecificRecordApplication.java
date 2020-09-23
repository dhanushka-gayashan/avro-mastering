package com.kdg.avro;


import com.kdg.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordApplication {

    private static Customer createCustomer(String firstName, String lastName, int age, float height, float weight, boolean automatedEmail) {
        Customer.Builder customerBuilder = Customer.newBuilder();

        customerBuilder.setFirstName(firstName);
        customerBuilder.setLastName(lastName);
        customerBuilder.setAge(age);
        customerBuilder.setHeight(height);
        customerBuilder.setWeight(weight);
        customerBuilder.setAutomatedEmail(automatedEmail);

        Customer customer = customerBuilder.build();
        System.out.println(customer.toString());

        return customer;
    }

    private static void writeAvroFile(Customer customer, String filePath) {
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);

        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File(filePath));
            dataFileWriter.append(customer);
            System.out.println("successfully wrote customer-specific.avro");
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    private static void readAvroFile(String filePath) {
        final File file = new File(filePath);
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);

        try {
            System.out.println("Reading our specific record");
            DataFileReader<Customer> dataFileReader = new DataFileReader<>(file, datumReader);
            while (dataFileReader.hasNext()) {
                Customer readCustomer = dataFileReader.next();
                System.out.println(readCustomer.toString());
                System.out.println("First name: " + readCustomer.getFirstName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        // Generate Specific Record
        Customer customer = createCustomer("Mark", "Simpson", 30, 180f, 90f, true);

        // Write to a Avro File
        writeAvroFile(customer, "customer-generic.avro");

        // Read from a Avro File
        readAvroFile("customer-generic.avro");
    }
}
