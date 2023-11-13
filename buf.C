#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

const Status BufMgr::allocBuf(int &frame) {
    int count = 0;

    while (count < 2 * static_cast<int>(numBufs)) {
        advanceClock();
        BufDesc &current_Frame = bufTable[clockHand];

        // Check if the frame can be allocated
        if (!current_Frame.valid) {
            frame = clockHand;
            return OK;
        }

        // Use the clock algorithm for valid frames
        if (current_Frame.refbit) { // If referenced recently, give another chance and clear refbit
            current_Frame.refbit = false;
        } else if (current_Frame.pinCnt == 0) {
            // Frame is not pinned, can be replaced
            if (current_Frame.dirty) {
                // Page needs to be written back to disk before replacement
                Status status = current_Frame.file->writePage(current_Frame.pageNo, &bufPool[clockHand]);
                if (status != OK) {
                    return UNIXERR;
                }
                current_Frame.dirty = false;
            }
            // Allocate the frame after making sure it is not dirty
            frame = clockHand;
            hashTable->remove(current_Frame.file, current_Frame.pageNo);
            current_Frame.Clear();
            return OK;
        }
        count++;
    }

    return BUFFEREXCEEDED;
}




const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
    int frame_number;

    // Check if page is already in buffer pool using lookup()
    Status status = hashTable->lookup(file, PageNo, frame_number);

    // Case 1: Page is not in the buffer pool
    if (status == HASHNOTFOUND) {
        // Allocate a buffer frame
        status = allocBuf(frame_number);
        if (status != OK) {
            return status; // Return BUFFEREXCEEDED or any other errors from allocBuf()
        }

        // Read the page from disk into the buffer pool frame
        status = file->readPage(PageNo, &(bufPool[frame_number]));
        if (status != OK) {
            return UNIXERR; // If an error occurred while reading the page
        }

        // Insert the page into the hash table
        status = hashTable->insert(file, PageNo, frame_number);
        if (status != OK) {
            return HASHTBLERROR; // If there's an error in insertion
        }

        // Setup the frame properly
        bufTable[frame_number].Set(file, PageNo);
        page = &bufPool[frame_number];
    } else if (status == OK) { // Case 2: Page is in the buffer pool
        //update control structures
        bufTable[frame_number].refbit = true;
        bufTable[frame_number].pinCnt++;
        page = &bufPool[frame_number];
    } else {
        return HASHTBLERROR; // Any other unexpected error from lookup()
    }
    return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) {
    int frame_number;

    // Check if page is in the buffer pool 
    Status status = hashTable->lookup(file, PageNo, frame_number);

    // If the page is not in the buffer pool
    if (status == HASHNOTFOUND) {
        return HASHNOTFOUND;
    }

    // if the pin count is already 0
    if (bufTable[frame_number].pinCnt <= 0) {
        return PAGENOTPINNED;
    }
    bufTable[frame_number].pinCnt--;

    // Set the dirty bit if dirty == true
    if (dirty) {
        bufTable[frame_number].dirty = true;
    }
    return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) {
    // Allocate an empty page in the specified file
    Status status = file->allocatePage(pageNo);
    if (status != OK) {
        return UNIXERR; 
    }

    // Call allocBuf() to obtain a buffer pool frame
    int frame_number;
    status = allocBuf(frame_number);
    if (status != OK) {
        return status; // Return error allocBuf() returned 
    }

    // Insert an entry into the hash table
    status = hashTable->insert(file, pageNo, frame_number);
    if (status != OK) {
        return HASHTBLERROR; // Error inserting into the hash table
    }

    bufTable[frame_number].Set(file, pageNo);
    // Return the page number and a pointer to the buffer frame
    page = &bufPool[frame_number];

    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
