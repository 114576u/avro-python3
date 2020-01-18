
import avro.schema
from avro.datafile import  DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# https://stackoverflow.com/questions/41405729/module-avro-schema-has-no-attribute-parse
# schema = avro.schema.parse(open("user.avsc", "rb").read())
schema = avro.schema.Parse(open("user.avsc", "r").read())

writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)
writer.append({"name": "Alyssa", "favourite_number": 256})
writer.append({"name": "Ben", "favourite_number": 7, "favourite_color": "red"})
writer.close()

reader = DataFileReader(open("users.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()