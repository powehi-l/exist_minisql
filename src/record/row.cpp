#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
    char *newbuf = buf;
    Field *field = nullptr;
    uint32_t bytes = 0 ;
    for (int i = 0; i < int(fields_.size()); ++i) {
        field = fields_[i];
        bytes = field->SerializeTo(newbuf);
        newbuf += bytes;
    }
    return newbuf - buf;
}


uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here
    char *newbuf = buf;
    Field *field = nullptr;
    uint32_t bytes = 0 ;
    for (int i = 0; i < int(schema->GetColumnCount()); ++i) {
        bytes = field->DeserializeFrom(newbuf, schema->GetColumn(i)->GetType(), &field, false, heap_);
        fields_.push_back(field);
        newbuf += bytes;
    }
    return newbuf - buf;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here
    if (fields_.empty())
        return 0;
    Field *field = nullptr;
    uint32_t bytes = 0;
    for (int i = 0; i < int(fields_.size()); ++i) {
        field = fields_[i];
        if (field == nullptr)
            continue;
        bytes += field->GetSerializedSize();
    }
    return bytes;
}
