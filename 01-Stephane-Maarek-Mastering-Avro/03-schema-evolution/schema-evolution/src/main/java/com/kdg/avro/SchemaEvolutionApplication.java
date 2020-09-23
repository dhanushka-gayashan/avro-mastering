package com.kdg.avro;

import com.kdg.CustomerV1;
import com.kdg.CustomerV2;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;

public class SchemaEvolutionApplication {

    private static CustomerV1 createCustomerV1() {
        return CustomerV1.newBuilder()
                .setAge(34)
                .setAutomatedEmail(false)
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(178f)
                .setWeight(75f)
                .build();
    }

    private static CustomerV2 createCustomerV2() {
        return CustomerV2.newBuilder()
                .setAge(25)
                .setFirstName("Mark")
                .setLastName("Simpson")
                .setEmail("mark.simpson@gmail.com")
                .setHeight(160f)
                .setWeight(65f)
                .setPhoneNumber("123-456-7890")
                .build();
    }

    private static void writeCustomerV1ToFile(CustomerV1 customer, String filePath) throws Exception{
        final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<>(CustomerV1.class);
        final DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<>(datumWriter);

        dataFileWriter.create(customer.getSchema(), new File(filePath));
        dataFileWriter.append(customer);
        dataFileWriter.close();

        System.out.println("successfully wrote to " + filePath);
    }

    private static void writeCustomerV2ToFile(CustomerV2 customer, String filePath) throws Exception{
        final DatumWriter<CustomerV2> datumWriter = new SpecificDatumWriter<>(CustomerV2.class);
        final DataFileWriter<CustomerV2> dataFileWriter = new DataFileWriter<>(datumWriter);

        dataFileWriter.create(customer.getSchema(), new File(filePath));
        dataFileWriter.append(customer);
        dataFileWriter.close();

        System.out.println("successfully wrote to " + filePath);
    }

    private static void readWithCustomerV1Schema(String filePath) throws Exception{
        System.out.println("Reading "+ filePath +" with v1 schema");

        final File file = new File(filePath);
        final DatumReader<CustomerV1> datumReader = new SpecificDatumReader<>(CustomerV1.class);
        final DataFileReader<CustomerV1> dataFileReader = new DataFileReader<>(file, datumReader);

        while (dataFileReader.hasNext()) {
            CustomerV1 customer = dataFileReader.next();
            System.out.println("Customer V1 = " + customer.toString());
        }

    }

    private static void readWithCustomerV2Schema(String filePath) throws Exception{
        System.out.println("Reading "+ filePath +" with v2 schema");

        final File file = new File(filePath);
        final DatumReader<CustomerV2> datumReader = new SpecificDatumReader<>(CustomerV2.class);
        final DataFileReader<CustomerV2> dataFileReader = new DataFileReader<>(file, datumReader);

        while (dataFileReader.hasNext()) {
            CustomerV2 customer = dataFileReader.next();
            System.out.println("Customer V2 = " + customer.toString());
        }
    }

    public static void main(String[] args) throws Exception{

        // File Paths
        String customerV1FilePath = "customerV1.avro";
        String customerV2FilePath = "customerV2.avro";

        // Create CustomerV1
        CustomerV1 customerV1 = createCustomerV1();
        System.out.println("Customer V1 = " + customerV1.toString());

        // Create CustomerV2
        CustomerV2 customerV2 = createCustomerV2();
        System.out.println("Customer V2 = " + customerV2.toString());

        // Write Customers in to Files
        writeCustomerV1ToFile(customerV1, customerV1FilePath);
        writeCustomerV2ToFile(customerV2, customerV2FilePath);

        // Backward Compatibility - Read CustomerV1 File from CustomerV2 Schema
        System.out.println("Backward Compatibility : ");
        readWithCustomerV2Schema(customerV1FilePath);

        // Forward Compatibility - Read CustomerV2 File from CustomerV1 Schema
        System.out.println("Forward Compatibility : ");
        readWithCustomerV1Schema(customerV2FilePath);
    }
}
