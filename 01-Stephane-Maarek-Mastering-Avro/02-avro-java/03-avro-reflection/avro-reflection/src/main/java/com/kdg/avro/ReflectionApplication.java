package com.kdg.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;

public class ReflectionApplication {

    private static void writeToFile(Schema schema, String filePath, String firstName, String lastName, String nickName) throws Exception{
        System.out.println("Writing " + filePath);

        File file = new File(filePath);

        DatumWriter<Customer> writer = new ReflectDatumWriter<>(Customer.class);
        DataFileWriter<Customer> out = new DataFileWriter<>(writer)
                .setCodec(CodecFactory.deflateCodec(9))
                .create(schema, file);

        out.append(new Customer(firstName, lastName, nickName));
        out.close();
    }

    private static void readFromFile(String filePath) throws Exception{
        System.out.println("Reading " + filePath);

        File file = new File(filePath);
        DatumReader<Customer> reader = new ReflectDatumReader<>(Customer.class);
        DataFileReader<Customer> in = new DataFileReader<>(file, reader);

        // read Customers from the file & print them as JSON
        for (Customer reflectedCustomer : in) {
            System.out.println(reflectedCustomer.fullName());
        }

        // close the input file
        in.close();
    }

    public static void main(String[] args) throws Exception{

        Schema schema = ReflectData.get().getSchema(Customer.class);
        System.out.println("schema = " + schema.toString(true));

        writeToFile(schema, "customer-reflected.avro", "Dhanushka", "Gayashan", "Dhanu");

        readFromFile("customer-reflected.avro");
    }
}
