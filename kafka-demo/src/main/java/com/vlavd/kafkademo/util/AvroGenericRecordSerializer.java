package com.vlavd.kafkademo.util;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static com.google.common.io.Resources.getResource;

public class AvroGenericRecordSerializer implements Serializer<GenericRecord> {

    @SneakyThrows
    @Override public byte[] serialize(String arg0, GenericRecord record) {
        Schema schema = new Schema.Parser().parse(Resources.toString(getResource("schema/user.avro"), Charsets.UTF_8));
        byte[] retVal = null;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        DataFileWriter dataFileWriter = new DataFileWriter<>(datumWriter);
        try {
            dataFileWriter.create(schema, outputStream);
                dataFileWriter.append(record);
            dataFileWriter.flush();
            dataFileWriter.close();
            retVal = outputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override public void close() {
    }
}
