#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
    char *newbuf = buf;
    for (auto column : columns_)
        column->SerializeTo(newbuf);
    return newbuf - buf;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
    uint32_t size = 0;
    for (auto column : columns_)
        size += column->GetSerializedSize();
    return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
    char *newbuf = buf;
    std::vector<Column *> columns;
    Column *column;
    while (*newbuf){
        Column::DeserializeFrom(newbuf, column, heap);
        columns.push_back(column);
        newbuf += column->GetSerializedSize();
    }
    void *mem = heap->Allocate(sizeof(Schema));
    schema = new(mem)Schema(columns);
    return newbuf - buf;
}