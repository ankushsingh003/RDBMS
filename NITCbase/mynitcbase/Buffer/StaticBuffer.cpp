#include "StaticBuffer.h"

// Initializing the static members
unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

StaticBuffer::StaticBuffer() {
  // Load block allocation map from disk (first 4 blocks)
  for (int i = 0; i < BLOCK_ALLOCATION_MAP_SIZE; i++) {
    Disk::readBlock(blockAllocMap + i * BLOCK_SIZE, i);
  }

  for (int i = 0; i < BUFFER_CAPACITY; i++) {
    metainfo[i].free = true;
    metainfo[i].dirty = false;
    metainfo[i].blockNum = -1;
    metainfo[i].timeStamp = -1;
  }
}

StaticBuffer::~StaticBuffer() {
  // Save block allocation map to disk
  for (int i = 0; i < BLOCK_ALLOCATION_MAP_SIZE; i++) {
    Disk::writeBlock(blockAllocMap + i * BLOCK_SIZE, i);
  }

  // Write back all dirty blocks to the disk
  for (int i = 0; i < BUFFER_CAPACITY; i++) {
    if (!metainfo[i].free && metainfo[i].dirty) {
      Disk::writeBlock(blocks[i], metainfo[i].blockNum);
    }
  }
}

int StaticBuffer::getFreeBuffer(int blockNum) {
  if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }

  int bufferNum = -1;

  // 1. Find a free buffer block
  for (int i = 0; i < BUFFER_CAPACITY; i++) {
    if (metainfo[i].free) {
      bufferNum = i;
      break;
    }
  }

  // 2. If no free buffer is found, implement LRU
  if (bufferNum == -1) {
    int maxTimeStamp = -1;
    for (int i = 0; i < BUFFER_CAPACITY; i++) {
      if (metainfo[i].timeStamp > maxTimeStamp) {
        maxTimeStamp = metainfo[i].timeStamp;
        bufferNum = i;
      }
    }

    // Write-back the dirty block
    if (metainfo[bufferNum].dirty) {
      Disk::writeBlock(blocks[bufferNum], metainfo[bufferNum].blockNum);
    }
  }

  // 3. Update metainfo for the allocated buffer
  metainfo[bufferNum].free = false;
  metainfo[bufferNum].dirty = false;
  metainfo[bufferNum].blockNum = blockNum;
  metainfo[bufferNum].timeStamp = 0;

  return bufferNum;
}

int StaticBuffer::getBufferNum(int blockNum) {
  if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }

  // Check if the block is already in the buffer
  for (int i = 0; i < BUFFER_CAPACITY; i++) {
    if (!metainfo[i].free && metainfo[i].blockNum == blockNum) {
      return i;
    }
  }

  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum) {
  // find the buffer number corresponding to the block using getBufferNum
  int bufferNum = getBufferNum(blockNum);

  // if block not in buffer, return E_BLOCKNOTINBUFFER
  if (bufferNum == E_BLOCKNOTINBUFFER) {
    return E_BLOCKNOTINBUFFER;
  }

  // if error, return error
  if (bufferNum == E_OUTOFBOUND) {
    return E_OUTOFBOUND;
  }

  // set the dirty bit for the buffer to true
  metainfo[bufferNum].dirty = true;

  return SUCCESS;
}
