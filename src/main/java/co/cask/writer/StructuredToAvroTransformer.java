package co.cask.writer;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.hydrator.common.RecordConverter;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

public class StructuredToAvroTransformer extends RecordConverter<StructuredRecord, GenericRecord> {
  private final Map<Integer, Schema> schemaCache = Maps.newHashMap();
  private final co.cask.cdap.api.data.schema.Schema outputCDAPSchema;
  private final Schema outputAvroSchema;

  public StructuredToAvroTransformer(String outputSchema) {
    try {
      this.outputCDAPSchema = outputSchema != null?co.cask.cdap.api.data.schema.Schema.parseJson(outputSchema):null;
    } catch (IOException var3) {
      throw new IllegalArgumentException("Unable to parse schema: Reason: " + var3.getMessage(), var3);
    }

    this.outputAvroSchema = outputSchema != null?(new Schema.Parser()).parse(outputSchema):null;
  }

  public GenericRecord transform(StructuredRecord structuredRecord) throws IOException {
    return this.transform(structuredRecord, this.outputCDAPSchema == null?structuredRecord.getSchema():this.outputCDAPSchema);
  }

  public GenericRecord transform(StructuredRecord structuredRecord, co.cask.cdap.api.data.schema.Schema schema) throws IOException {
    co.cask.cdap.api.data.schema.Schema structuredRecordSchema = structuredRecord.getSchema();
    Schema avroSchema = this.getAvroSchema(schema);
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    Iterator i$ = avroSchema.getFields().iterator();

    while(i$.hasNext()) {
      Schema.Field field = (Schema.Field)i$.next();
      String fieldName = field.name();
      co.cask.cdap.api.data.schema.Schema.Field schemaField = structuredRecordSchema.getField(fieldName);
      if(schemaField == null) {
        throw new IllegalArgumentException("Input record does not contain the " + fieldName + " field.");
      }

      recordBuilder.set(fieldName, this.convertField(structuredRecord.get(fieldName), schemaField.getSchema()));
    }

    return recordBuilder.build();
  }

  protected Object convertBytes(Object field) {
    return field instanceof ByteBuffer ?field:ByteBuffer.wrap((byte[])((byte[])field));
  }

  private Schema getAvroSchema(co.cask.cdap.api.data.schema.Schema cdapSchema) {
    int hashCode = cdapSchema.hashCode();
    if(this.schemaCache.containsKey(Integer.valueOf(hashCode))) {
      return (Schema)this.schemaCache.get(Integer.valueOf(hashCode));
    } else {
      Schema avroSchema = (new Schema.Parser()).parse(cdapSchema.toString());
      this.schemaCache.put(Integer.valueOf(hashCode), avroSchema);
      return avroSchema;
    }
  }
}

