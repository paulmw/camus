package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Properties;

public class DirectAvroMessageDecoder extends MessageDecoder<Message, Record> {
  private static final Logger log = Logger.getLogger(DirectAvroMessageDecoder.class);

  private Schema schema;

  @Override
  public void init(Properties props, String topicName) {
    super.init(props, topicName);
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      InputStream is = fs.open(new Path("/schemas/" + topicName + ".avsc"));

      schema = new Schema.Parser().parse(is);
      is.close();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public Record deserialise(byte [] b) {
    Record record = null;
    GenericDatumReader<Record> datumReader = new GenericDatumReader<Record>(schema);
    BinaryDecoder d = null;
    d = DecoderFactory.get().binaryDecoder(b, d);
    try {
      record = datumReader.read(record, d);
      return record;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }


  public CamusWrapper<Record> decode(Message message) {
    return new CamusWrapper<Record>(deserialise(message.getPayload()));
  }


}
