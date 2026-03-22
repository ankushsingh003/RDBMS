#include "BlockBuffer.h"
#include <cstring>

// Constructor for BlockBuffer
BlockBuffer::BlockBuffer(int blockNum) {
  this->blockNum = blockNum;
}

// Get the header of the block
int BlockBuffer::getHeader(struct HeadInfo *head) {
  unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, this->blockNum);
  memcpy(head, buffer, HEADER_SIZE);
  return SUCCESS;
}

// Constructor for RecBuffer
RecBuffer::RecBuffer(int blockNum) : BlockBuffer(blockNum) {}

// Get the record at a specific slotNum
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, this->blockNum);

  struct HeadInfo head;
  this->getHeader(&head);

  int numAttrs = head.numAttrs;
  int numSlots = head.numSlots;

  // Offset of record = HEADER_SIZE + numSlots + (slotNum * numAttrs * ATTR_SIZE)
  // Each slot has one byte in the slotmap
  int recordSize = numAttrs * ATTR_SIZE;
  unsigned char *slotPointer = buffer + HEADER_SIZE + numSlots + (slotNum * recordSize);
  
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}