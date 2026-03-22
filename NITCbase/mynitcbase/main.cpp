#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>
#include <cstring>

int main(int argc, char *argv[]) {
  Disk disk_run;
  StaticBuffer buffer; // Initialize the disk buffer

  RecBuffer relCatBlock(RELCAT_BLOCK);

  HeadInfo relCatHeader;
  relCatBlock.getHeader(&relCatHeader);

  for (int i = 0; i < relCatHeader.numEntries; i++) {

    Attribute relCatRecord[RELCAT_NO_ATTRS]; // RELCAT_NO_ATTRS = 6
    relCatBlock.getRecord(relCatRecord, i);

    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);

    RecBuffer attrCatBlock(ATTRCAT_BLOCK);

    HeadInfo attrCatHeader;
    attrCatBlock.getHeader(&attrCatHeader);

    for (int j = 0; j < attrCatHeader.numEntries; j++) {

      Attribute attrCatRecord[ATTRCAT_NO_ATTRS]; // ATTRCAT_NO_ATTRS = 6
      attrCatBlock.getRecord(attrCatRecord, j);

      if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, 
                 relCatRecord[RELCAT_REL_NAME_INDEX].sVal) == 0) {
        
        const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
        printf("  %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
      }
    }
    printf("\n");
  }

  return 0;
}