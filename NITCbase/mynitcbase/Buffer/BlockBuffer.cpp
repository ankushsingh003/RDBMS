#include "BlockBuffer.h"
#include <cstring>

// Constructor for BlockBuffer
BlockBuffer::BlockBuffer(int blockNum) {
  this->blockNum = blockNum;
}

// Get the header of the block through the buffer
int BlockBuffer::getHeader(struct HeadInfo *head) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  memcpy(head, bufferPtr, HEADER_SIZE);
  return SUCCESS;
}

// Load the block into the buffer and get a pointer to it
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {
  // Check if the block is already in the buffer
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  if (bufferNum == E_BLOCKNOTINBUFFER) {
    // If not in buffer, find a free buffer block
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    if (bufferNum == E_DISKFULL) {
      return E_DISKFULL;
    }

    // Read the block from the disk into the assigned buffer block
    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }

  // Provide the pointer to the buffer block
  *buffPtr = StaticBuffer::blocks[bufferNum];
  return SUCCESS;
}

// Constructor for RecBuffer
RecBuffer::RecBuffer(int blockNum) : BlockBuffer(blockNum) {}

// Get the record at a specific slotNum through the buffer
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  this->getHeader(&head);

  int numAttrs = head.numAttrs;
  int numSlots = head.numSlots;

  // Offset of record = HEADER_SIZE + numSlots + (slotNum * recordSize)
  int recordSize = numAttrs * ATTR_SIZE;
  unsigned char *slotPointer = bufferPtr + HEADER_SIZE + numSlots + (slotNum * recordSize);
  
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}