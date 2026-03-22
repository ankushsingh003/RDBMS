#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>
#include <cstring>
#include <cstdio>

int main(int argc, char *argv[]) {
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;

  for (int i = 0; i < 3; i++) {
    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(i, &relCatBuf);

    printf("Relation: %s\n", relCatBuf.relName);

    for (int j = 0; j < relCatBuf.numAttrs; j++) {
      AttrCatEntry attrCatBuf;
      AttrCacheTable::getAttrCatEntry(i, j, &attrCatBuf);

      const char *attrType = attrCatBuf.attrType == NUMBER ? "NUM" : "STR";
      printf("  %s: %s\n", attrCatBuf.attrName, attrType);
    }
    printf("\n");
  }

  return 0;
}