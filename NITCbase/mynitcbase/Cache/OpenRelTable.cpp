#include "OpenRelTable.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {
  for (int i = 0; i < MAX_OPEN; i++) {
    OpenRelTable::tableMetaInfo[i].free = true;
    RelCacheTable::relCache[i] = NULL;
    AttrCacheTable::attrCache[i] = NULL;
  }

  RecBuffer relCatBlock(RELCAT_BLOCK);
  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  struct RelCacheEntry *relCacheEntry = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  RelCacheTable::recordToRelCatEntry(relCatRecord, &(relCacheEntry->relCatEntry));
  relCacheEntry->recId.block = RELCAT_BLOCK;
  relCacheEntry->recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;
  relCacheEntry->dirty = false;
  relCacheEntry->searchIndex = {-1, -1};
  RelCacheTable::relCache[RELCAT_RELID] = relCacheEntry;

  tableMetaInfo[RELCAT_RELID].free = false;
  memset(tableMetaInfo[RELCAT_RELID].relName, 0, ATTR_SIZE);
  strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);

  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
  relCacheEntry = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  RelCacheTable::recordToRelCatEntry(relCatRecord, &(relCacheEntry->relCatEntry));
  relCacheEntry->recId.block = RELCAT_BLOCK;
  relCacheEntry->recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;
  relCacheEntry->dirty = false;
  relCacheEntry->searchIndex = {-1, -1};
  RelCacheTable::relCache[ATTRCAT_RELID] = relCacheEntry;

  tableMetaInfo[ATTRCAT_RELID].free = false;
  memset(tableMetaInfo[ATTRCAT_RELID].relName, 0, ATTR_SIZE);
  strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);

  RecBuffer attrCatBlock(ATTRCAT_BLOCK);
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  
  AttrCacheEntry *head = NULL, *prev = NULL;
  for (int i = 0; i < 6; i++) {
    attrCatBlock.getRecord(attrCatRecord, i);
    AttrCacheEntry *entry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &entry->attrCatEntry);
    entry->recId.block = ATTRCAT_BLOCK;
    entry->recId.slot = i;
    entry->dirty = false;
    entry->searchIndex = {-1, -1};
    entry->next = NULL;

    if (head == NULL) head = entry;
    if (prev != NULL) prev->next = entry;
    prev = entry;
  }
  AttrCacheTable::attrCache[RELCAT_RELID] = head;

  head = NULL; prev = NULL;
  for (int i = 6; i < 12; i++) {
    attrCatBlock.getRecord(attrCatRecord, i);
    AttrCacheEntry *entry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &entry->attrCatEntry);
    entry->recId.block = ATTRCAT_BLOCK;
    entry->recId.slot = i;
    entry->dirty = false;
    entry->searchIndex = {-1, -1};
    entry->next = NULL;

    if (head == NULL) head = entry;
    if (prev != NULL) prev->next = entry;
    prev = entry;
  }
  AttrCacheTable::attrCache[ATTRCAT_RELID] = head;
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
  for (int i = 0; i < MAX_OPEN; i++) {
    if (!tableMetaInfo[i].free && strcmp(tableMetaInfo[i].relName, relName) == 0) {
      return i;
    }
  }
  return E_RELNOTEXIST;
}

int OpenRelTable::getFreeOpenRelTableEntry() {
  for (int i = 2; i < MAX_OPEN; i++) {
    if (tableMetaInfo[i].free) {
      return i;
    }
  }
  return E_CACHEFULL;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]) {
  int relId = getRelId(relName);
  if (relId >= 0) {
    return relId;
  }

  relId = getFreeOpenRelTableEntry();
  if (relId == E_CACHEFULL) {
    return E_CACHEFULL;
  }

  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  Attribute attrVal;
  strcpy(attrVal.sVal, relName);

  RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, attrVal, EQ);

  if (relcatRecId.block == -1 && relcatRecId.slot == -1) {
    return E_RELNOTEXIST;
  }

  RecBuffer relCatBlock(relcatRecId.block);
  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, relcatRecId.slot);

  struct RelCacheEntry *relCacheEntry = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  RelCacheTable::recordToRelCatEntry(relCatRecord, &(relCacheEntry->relCatEntry));
  relCacheEntry->recId = relcatRecId;
  relCacheEntry->dirty = false;
  relCacheEntry->searchIndex = {-1, -1};
  RelCacheTable::relCache[relId] = relCacheEntry;

  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

  AttrCacheEntry *head = NULL, *prev = NULL;
  RecId attrcatRecId;
  while (true) {
    attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, attrVal, EQ);
    if (attrcatRecId.block == -1 && attrcatRecId.slot == -1) {
      break;
    }

    RecBuffer attrCatBlock(attrcatRecId.block);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    attrCatBlock.getRecord(attrCatRecord, attrcatRecId.slot);

    AttrCacheEntry *entry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(entry->attrCatEntry));
    entry->recId = attrcatRecId;
    entry->dirty = false;
    entry->searchIndex = {-1, -1};
    entry->next = NULL;

    if (head == NULL) head = entry;
    if (prev != NULL) prev->next = entry;
    prev = entry;
  }
  AttrCacheTable::attrCache[relId] = head;

  tableMetaInfo[relId].free = false;
  memset(tableMetaInfo[relId].relName, 0, ATTR_SIZE);
  strcpy(tableMetaInfo[relId].relName, relName);

  return relId;
}

int OpenRelTable::closeRel(int relId) {
  if (relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
    return E_NOTPERMITTED;
  }
  if (relId < 0 || relId >= MAX_OPEN || tableMetaInfo[relId].free) {
    return E_OUTOFBOUND;
  }

  if (RelCacheTable::relCache[relId]->dirty) {
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[relId]->relCatEntry), relCatRecord);
    RecId recId = RelCacheTable::relCache[relId]->recId;
    RecBuffer relCatBlock(recId.block);
    relCatBlock.setRecord(relCatRecord, recId.slot);
  }

  for (AttrCacheEntry* entry = AttrCacheTable::attrCache[relId]; entry != NULL; entry = entry->next) {
    if (entry->dirty) {
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      AttrCacheTable::attrCatEntryToRecord(&(entry->attrCatEntry), attrCatRecord);
      RecBuffer attrCatBlock(entry->recId.block);
      attrCatBlock.setRecord(attrCatRecord, entry->recId.slot);
    }
  }

  free(RelCacheTable::relCache[relId]);
  RelCacheTable::relCache[relId] = NULL;

  AttrCacheEntry *curr = AttrCacheTable::attrCache[relId];
  while (curr != NULL) {
    AttrCacheEntry *next = curr->next;
    free(curr);
    curr = next;
  }
  AttrCacheTable::attrCache[relId] = NULL;

  tableMetaInfo[relId].free = true;
  return SUCCESS;
}

OpenRelTable::~OpenRelTable() {
  printf("Closing OpenRelTable...\n");
  for (int i = 2; i < MAX_OPEN; i++) {
    if (!tableMetaInfo[i].free) {
      closeRel(i);
    }
  }

  for (int i = 0; i < 2; i++) {
    if (RelCacheTable::relCache[i]->dirty) {
      Attribute relCatRecord[RELCAT_NO_ATTRS];
      RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[i]->relCatEntry), relCatRecord);
      RecBuffer relCatBlock(RelCacheTable::relCache[i]->recId.block);
      relCatBlock.setRecord(relCatRecord, RelCacheTable::relCache[i]->recId.slot);
    }
    free(RelCacheTable::relCache[i]);

    AttrCacheEntry *curr = AttrCacheTable::attrCache[i];
    while (curr != NULL) {
      if (curr->dirty) {
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        AttrCacheTable::attrCatEntryToRecord(&(curr->attrCatEntry), attrCatRecord);
        RecBuffer attrCatBlock(curr->recId.block);
        attrCatBlock.setRecord(attrCatRecord, curr->recId.slot);
      }
      AttrCacheEntry *next = curr->next;
      free(curr);
      curr = next;
    }
  }
}