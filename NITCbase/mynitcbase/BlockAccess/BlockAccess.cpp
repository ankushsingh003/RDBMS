#include "BlockAccess.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>

RecId BlockAccess::linearSearch(int relId, char *attrName, Attribute attrVal, int op) {
  // Get the relation catalog entry
  RelCatEntry relCatBuf;
  RelCacheTable::getRelCatEntry(relId, &relCatBuf);

  // Get the search index from the cache
  RecId prevRecId;
  RelCacheTable::getSearchIndex(relId, &prevRecId);

  int blockNum, slotNum;
  if (prevRecId.block == -1 && prevRecId.slot == -1) {
    // Start from the first block and first slot
    blockNum = relCatBuf.firstBlk;
    slotNum = 0;
  } else {
    // Start from the next slot
    blockNum = prevRecId.block;
    slotNum = prevRecId.slot + 1;
  }

  // Iterate through all record blocks
  while (blockNum != -1) {
    RecBuffer currentBlock(blockNum);
    struct HeadInfo head;
    currentBlock.getHeader(&head);

    unsigned char *slotMap = (unsigned char *)::malloc(head.numSlots);
    currentBlock.getSlotMap(slotMap);

    // Get the attribute catalog entry for the given attribute name
    AttrCatEntry attrCatBuf;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

    if (ret != SUCCESS) {
      // If the attribute doesn't exist, we can't search
      ::free(slotMap);
      return {-1, -1};
    }

    // Iterate through slots in the current block
    for (int slot = slotNum; slot < head.numSlots; slot++) {
      if (slotMap[slot] == SLOT_OCCUPIED) {
        Attribute *record = (Attribute *)::malloc(sizeof(Attribute) * relCatBuf.numAttrs);
        currentBlock.getRecord(record, slot);

        // Compare the attribute in the record with attrVal using the op
        int cmpVal = compareAttrs(record[attrCatBuf.offset], attrVal, attrCatBuf.attrType);

        if (
            (op == EQ && cmpVal == 0) ||
            (op == NE && cmpVal != 0) ||
            (op == LT && cmpVal < 0) ||
            (op == LE && cmpVal <= 0) ||
            (op == GT && cmpVal > 0) ||
            (op == GE && cmpVal >= 0)
        ) {
          // Success! Update the search index and return the RecId
          RecId hit = {blockNum, slot};
          RelCacheTable::setSearchIndex(relId, &hit);
          ::free(record);
          ::free(slotMap);
          return hit;
        }
        ::free(record);
      }
    }

    ::free(slotMap);
    // Move to the next block
    blockNum = head.rblock;
    slotNum = 0;
  }

  // No record satisfying the condition was found
  return {-1, -1};
}

int BlockAccess::insert(int relId, Attribute *record) {
  // 1. Get relation catalog entry
  RelCatEntry relCatBuf;
  RelCacheTable::getRelCatEntry(relId, &relCatBuf);

  int firstBlk = relCatBuf.firstBlk;
  int numAttributes = relCatBuf.numAttrs;
  int numSlots = relCatBuf.numSlotsPerBlk;

  int prevBlkNum = -1;
  int currBlkNum = firstBlk;

  // 2. Efficiently find a block with a free slot
  while (currBlkNum != -1) {
    RecBuffer currBlock(currBlkNum);
    unsigned char *slotMap = (unsigned char *)::malloc(numSlots);
    currBlock.getSlotMap(slotMap);

    for (int i = 0; i < numSlots; i++) {
      if (slotMap[i] == SLOT_UNOCCUPIED) {
        // Found a free slot
        currBlock.setRecord(record, i);
        slotMap[i] = SLOT_OCCUPIED;
        currBlock.setSlotMap(slotMap);

        struct HeadInfo head;
        currBlock.getHeader(&head);
        head.numEntries++;
        currBlock.setHeader(&head);

        // Update numRecs in RelCache
        relCatBuf.numRecs++;
        RelCacheTable::setRelCatEntry(relId, &relCatBuf);

        ::free(slotMap);
        return SUCCESS;
      }
    }
    prevBlkNum = currBlkNum;
    struct HeadInfo head;
    currBlock.getHeader(&head);
    currBlkNum = head.rblock;
    ::free(slotMap);
  }

  // 3. No free slots found, allocate a new block
  RecBuffer newBlock;
  int newBlkNum = newBlock.getBlockNum();
  if (newBlkNum == E_DISKFULL) return E_DISKFULL;

  struct HeadInfo head;
  newBlock.getHeader(&head);
  head.numAttrs = numAttributes;
  head.numSlots = numSlots;
  head.rblock = -1;
  head.lblock = -1;
  newBlock.setHeader(&head);

  unsigned char *slotMap = (unsigned char *)::malloc(numSlots);
  for (int i = 0; i < numSlots; i++) slotMap[i] = SLOT_UNOCCUPIED;
  newBlock.setSlotMap(slotMap);

  // 4. Update the previous block's rblock or the relation's firstBlk
  if (prevBlkNum == -1) {
    relCatBuf.firstBlk = newBlkNum;
  } else {
    RecBuffer prevBlock(prevBlkNum);
    struct HeadInfo prevHead;
    prevBlock.getHeader(&prevHead);
    prevHead.rblock = newBlkNum;
    prevBlock.setHeader(&prevHead);
  }
  relCatBuf.lastBlk = newBlkNum;
  RelCacheTable::setRelCatEntry(relId, &relCatBuf);

  // 5. Insert the record into the first slot of the new block
  newBlock.setRecord(record, 0);
  slotMap[0] = SLOT_OCCUPIED;
  newBlock.setSlotMap(slotMap);
  head.numEntries = 1;
  newBlock.setHeader(&head);

  // Update numRecs in RelCache
  relCatBuf.numRecs++;
  RelCacheTable::setRelCatEntry(relId, &relCatBuf);

  ::free(slotMap);
  return SUCCESS;
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {
  // 1. Reset search index of RELATIONCAT
  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  Attribute oldNameAttr;
  ::strcpy(oldNameAttr.sVal, oldName);

  // 2. Search for the relation catalog entry of oldName
  RecId relcatRecId = linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, oldNameAttr, EQ);

  // 3. If not found, return E_RELNOTEXIST
  if (relcatRecId.block == -1 && relcatRecId.slot == -1) {
    return E_RELNOTEXIST;
  }

  // 4. Get the relation catalog record and update the name
  RecBuffer relcatBlock(relcatRecId.block);
  Attribute relcatRecord[RELCAT_NO_ATTRS];
  relcatBlock.getRecord(relcatRecord, relcatRecId.slot);
  strcpy(relcatRecord[RELCAT_REL_NAME_INDEX].sVal, newName);

  // 5. Write back the record
  relcatBlock.setRecord(relcatRecord, relcatRecId.slot);

  // 6. Update all attribute catalog entries with the new relation name
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  while (true) {
    RecId attrcatRecId = linearSearch(ATTRCAT_RELID, (char *)ATTRCAT_ATTR_RELNAME, oldNameAttr, EQ);
    if (attrcatRecId.block == -1 && attrcatRecId.slot == -1) {
      break;
    }

    RecBuffer attrcatBlock(attrcatRecId.block);
    Attribute attrcatRecord[ATTRCAT_NO_ATTRS];
    attrcatBlock.getRecord(attrcatRecord, attrcatRecId.slot);
    strcpy(attrcatRecord[ATTRCAT_REL_NAME_INDEX].sVal, newName);
    attrcatBlock.setRecord(attrcatRecord, attrcatRecId.slot);
  }

  return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {
  // 1. Reset search index of ATTRIBUTECAT
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

  Attribute relNameAttr;
  strcpy(relNameAttr.sVal, relName);

  // 2. Iterate through all attributes of relName in ATTRIBUTECAT
  while (true) {
    RecId attrcatRecId = linearSearch(ATTRCAT_RELID, (char *)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
    if (attrcatRecId.block == -1 && attrcatRecId.slot == -1) {
      break;
    }

    RecBuffer attrcatBlock(attrcatRecId.block);
    Attribute attrcatRecord[ATTRCAT_NO_ATTRS];
    attrcatBlock.getRecord(attrcatRecord, attrcatRecId.slot);

    // 3. If the attribute name matches oldName, update it to newName
    if (strcmp(attrcatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0) {
      strcpy(attrcatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
      attrcatBlock.setRecord(attrcatRecord, attrcatRecId.slot);
      return SUCCESS;
    }
  }

  // 4. If not found, return E_ATTRNOTEXIST
  return E_ATTRNOTEXIST;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
  // 1. Search RELCAT for the relation
  Attribute relNameAttr;
  strcpy(relNameAttr.sVal, relName);
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  RecId relCatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttr, EQ);

  if (relCatRecId.block == -1 || relCatRecId.slot == -1) {
    return E_RELNOTEXIST;
  }

  // 2. Get info from RELCAT record
  RecBuffer relCatBlock(relCatRecId.block);
  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, relCatRecId.slot);

  int firstBlk = (int)relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
  int numAttrs = (int)relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

  // 3. Release all blocks of the relation
  int currentBlkNum = firstBlk;
  while (currentBlkNum != -1) {
    RecBuffer currentBlock(currentBlkNum);
    struct HeadInfo header;
    currentBlock.getHeader(&header);
    int nextBlk = header.rblock;
    currentBlock.releaseBlock();
    currentBlkNum = nextBlk;
  }

  // 4. Delete entries from ATTRCAT
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  for (int i = 0; i < numAttrs; i++) {
    RecId attrCatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
    RecBuffer attrCatBlock(attrCatRecId.block);
    struct HeadInfo header;
    attrCatBlock.getHeader(&header);
    header.numEntries--;
    attrCatBlock.setHeader(&header);

    unsigned char *slotMap = (unsigned char *)::malloc(header.numSlots);
    attrCatBlock.getSlotMap(slotMap);
    slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
    attrCatBlock.setSlotMap(slotMap);
    ::free(slotMap);
  }

  // 5. Delete entry from RELCAT
  RecBuffer relCatBlockFinal(relCatRecId.block);
  struct HeadInfo relCatHeader;
  relCatBlockFinal.getHeader(&relCatHeader);
  relCatHeader.numEntries--;
  relCatBlockFinal.setHeader(&relCatHeader);

  unsigned char *relCatSlotMap = (unsigned char *)::malloc(relCatHeader.numSlots);
  relCatBlockFinal.getSlotMap(relCatSlotMap);
  relCatSlotMap[relCatRecId.slot] = SLOT_UNOCCUPIED;
  relCatBlockFinal.setSlotMap(relCatSlotMap);
  ::free(relCatSlotMap);

  // 6. Update numRecs in RelCache
  RelCatEntry relCatBuf;
  RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatBuf);
  relCatBuf.numRecs--;
  RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatBuf);

  RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatBuf);
  relCatBuf.numRecs -= numAttrs;
  RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatBuf);

  return SUCCESS;
}

int BlockAccess::project(int relId, Attribute *record) {
  // get the previous search index from the relation cache
  RecId prevRecId;
  RelCacheTable::getSearchIndex(relId, &prevRecId);

  int blockNum, slotNum;

  // if the search index is {-1, -1}, start from the beginning
  if (prevRecId.block == -1 && prevRecId.slot == -1) {
    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(relId, &relCatBuf);
    blockNum = relCatBuf.firstBlk;
    slotNum = 0;
  } else {
    blockNum = prevRecId.block;
    slotNum = prevRecId.slot + 1;
  }

  while (blockNum != -1) {
    RecBuffer recBuffer(blockNum);
    HeadInfo header;
    recBuffer.getHeader(&header);

    unsigned char *slotMap = (unsigned char *)::malloc(header.numSlots);
    recBuffer.getSlotMap(slotMap);

    for (int i = slotNum; i < header.numSlots; i++) {
        if (slotMap[i] == SLOT_OCCUPIED) {
            recBuffer.getRecord(record, i);

            RecId newRecId = {blockNum, i};
            RelCacheTable::setSearchIndex(relId, &newRecId);

            ::free(slotMap);
            return SUCCESS;
        }
    }
    blockNum = header.rblock;
    slotNum = 0;
    ::free(slotMap);
  }

  // if no more records are found, return E_NOTFOUND
  return E_NOTFOUND;
}
