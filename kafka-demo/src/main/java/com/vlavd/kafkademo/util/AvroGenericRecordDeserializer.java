package com.vlavd.kafkademo.util;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import lombok.SneakyThrows;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.io.Resources.getResource;

public class AvroGenericRecordDeserializer implements Deserializer {

    @SneakyThrows
    @Override
    public Object deserialize(String s, byte[] bytes) {
        Schema schema = new Schema.Parser()
                .parse(Resources.toString(getResource("schema/user.avro"), Charsets.UTF_8));
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        SeekableByteArrayInput arrayInput = new SeekableByteArrayInput(bytes);
        GenericRecord record = null;

        DataFileReader<GenericRecord> dataFileReader;
        try {
            dataFileReader = new DataFileReader<>(arrayInput, datumReader);
            record = dataFileReader.next();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return record;

    }
}
