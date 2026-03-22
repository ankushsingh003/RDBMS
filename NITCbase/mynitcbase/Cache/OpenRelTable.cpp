#include "OpenRelTable.h"
#include <cstring>
#include <cstdlib>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {
  // Initialize all entries in tableMetaInfo to indicate they are free
  for (int i = 0; i < MAX_OPEN; i++) {
    OpenRelTable::tableMetaInfo[i].free = true;
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
  }

  // Step 1: Load RELATIONCAT (rel-id 0)
  RecBuffer relCatBlock(RELCAT_BLOCK);
  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT); // Slot 0

  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;
  relCacheEntry.dirty = false;
  relCacheEntry.searchIndex = {-1, -1};

  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

  // Step 2: Load ATTRIBUTECAT (rel-id 1)
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT); // Slot 1
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;
  
  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry;

  // Step 3: Load Attributes for RELATIONCAT and ATTRIBUTECAT into AttrCache
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

  // We know that the first 6 entries in ATTRCAT_BLOCK are for RELATIONCAT
  // and the next 6 entries are for ATTRIBUTECAT
  
  // Attributes for RELATIONCAT
  AttrCacheEntry *head = nullptr, *prev = nullptr;
  for (int i = 0; i < 6; i++) {
    attrCatBlock.getRecord(attrCatRecord, i);
    AttrCacheEntry *entry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &entry->attrCatEntry);
    entry->recId.block = ATTRCAT_BLOCK;
    entry->recId.slot = i;
    entry->dirty = false;
    entry->searchIndex = {-1, -1};
    entry->next = nullptr;

    if (head == nullptr) head = entry;
    if (prev != nullptr) prev->next = entry;
    prev = entry;
  }
  AttrCacheTable::attrCache[RELCAT_RELID] = head;

  // Attributes for ATTRIBUTECAT
  head = nullptr; prev = nullptr;
  for (int i = 6; i < 12; i++) {
    attrCatBlock.getRecord(attrCatRecord, i);
    AttrCacheEntry *entry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &entry->attrCatEntry);
    entry->recId.block = ATTRCAT_BLOCK;
    entry->recId.slot = i;
    entry->dirty = false;
    entry->searchIndex = {-1, -1};
    entry->next = nullptr;

    if (head == nullptr) head = entry;
    if (prev != nullptr) prev->next = entry;
    prev = entry;
  }
  AttrCacheTable::attrCache[ATTRCAT_RELID] = head;

  // Step 4: Load Students relation (rel-id 2) into cache
  relCatBlock.getRecord(relCatRecord, 2); // Slot 2
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = 2;
  relCacheEntry.dirty = false;
  relCacheEntry.searchIndex = {-1, -1};

  RelCacheTable::relCache[2] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[2]) = relCacheEntry;

  // Attributes for Students
  head = nullptr; prev = nullptr;
  for (int i = 12; i < 16; i++) {
    attrCatBlock.getRecord(attrCatRecord, i);
    AttrCacheEntry *entry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &entry->attrCatEntry);
    entry->recId.block = ATTRCAT_BLOCK;
    entry->recId.slot = i;
    entry->dirty = false;
    entry->searchIndex = {-1, -1};
    entry->next = nullptr;

    if (head == nullptr) head = entry;
    if (prev != nullptr) prev->next = entry;
    prev = entry;
  }
  AttrCacheTable::attrCache[2] = head;

  // Set tableMetaInfo for Students
  tableMetaInfo[2].free = false;
  strcpy(tableMetaInfo[2].relName, "Students");
}

OpenRelTable::~OpenRelTable() {
  // Free the memory allocated for the cache entries
  for (int i = 0; i < MAX_OPEN; i++) {
    if (RelCacheTable::relCache[i] != nullptr) {
      free(RelCacheTable::relCache[i]);
    }
    AttrCacheEntry *curr = AttrCacheTable::attrCache[i];
    while (curr != nullptr) {
      AttrCacheEntry *next = curr->next;
      free(curr);
      curr = next;
    }
  }
}