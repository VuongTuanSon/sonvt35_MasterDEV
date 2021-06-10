import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * Support class for avro messages.
 */
public class AvroSupport {


    public static Schema getSchema() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(new File("C:\\Users\\DELL\\OneDrive\\Máy tính\\BTVN GHTK\\AvroSeDe\\src\\main\\java\\schema.avsc"));
        return schema;
    }

    public static byte[] dataToByteArray(Schema schema, GenericRecord datum) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            Encoder e = EncoderFactory.get().binaryEncoder(os, null);
            writer.write(datum, e);
            e.flush();
            byte[] byteData = os.toByteArray();
            return byteData;
        } finally {
            os.close();
        }
    }

    public static GenericRecord byteArrayToData(Schema schema, byte[] byteData) {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        ByteArrayInputStream byteArrayInputStream = null;
        try {
            byteArrayInputStream = new ByteArrayInputStream(byteData);
            Decoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);
            return reader.read(null, decoder);
        } catch (IOException e) {
            return null;
        } finally {
            try {
                byteArrayInputStream.close();
            } catch (IOException e) {

            }
        }
    }


    public static <T> T getValue(GenericRecord genericRecord, String name, Class<T> clazz) {
        Object obj = genericRecord.get(name);
        if (obj == null)
            return null;
        if (obj.getClass() == Utf8.class) {
            return (T) obj.toString();
        }
        if (obj.getClass() == Integer.class) {
            return (T) obj;
        }
        return null;
    }
}