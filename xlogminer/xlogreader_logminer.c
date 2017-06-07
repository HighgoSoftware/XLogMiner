/*-------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *	  contrib/xlogminer/xlogreader_logminer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "pg_logminer.h"

#define MAX_ERRORMSG_LEN 1000

static bool allocate_recordbuf_logminer(XLogReaderState *state, uint32 reclength);
static void ResetDecoder_logminer(XLogReaderState *state);
static bool ValidXLogPageHeader_logminer(XLogReaderState *state, XLogRecPtr recptr,
					XLogPageHeader hdr);
static bool ValidXLogRecordHeader_logminer(XLogReaderState *state, XLogRecPtr RecPtr,
				 XLogRecPtr PrevRecPtr, XLogRecord *record, bool randAccess);

static bool ValidXLogRecord_logminer(XLogReaderState *state, XLogRecord *record, XLogRecPtr recptr);
static int ReadPageInternal_logminer(XLogReaderState *state, XLogRecPtr pageptr,int reqLen);

static void
ResetDecoder_logminer(XLogReaderState *state)
{
	int			block_id;

	state->decoded_record = NULL;

	state->main_data_len = 0;

	for (block_id = 0; block_id <= state->max_block_id; block_id++)
	{
		state->blocks[block_id].in_use = false;
		state->blocks[block_id].has_image = false;
		state->blocks[block_id].has_data = false;
	}
	state->max_block_id = -1;
}


/*
 * Allocate readRecordBuf to fit a record of at least the given length.
 * Returns true if successful, false if out of memory.
 *
 * readRecordBufSize is set to the new buffer size.
 *
 * To avoid useless small increases, round its size to a multiple of
 * XLOG_BLCKSZ, and make sure it's at least 5*Max(BLCKSZ, XLOG_BLCKSZ) to start
 * with.  (That is enough for all "normal" records, but very large commit or
 * abort records might need more space.)
 */
static bool
allocate_recordbuf_logminer(XLogReaderState *state, uint32 reclength)
{
	uint32		newSize = reclength;

	newSize += XLOG_BLCKSZ - (newSize % XLOG_BLCKSZ);
	newSize = Max(newSize, 5 * Max(BLCKSZ, XLOG_BLCKSZ));

	if (state->readRecordBuf)
		pfree(state->readRecordBuf);
	state->readRecordBuf =
		(char *) palloc_extended(newSize, MCXT_ALLOC_NO_OOM);
	if (state->readRecordBuf == NULL)
	{
		state->readRecordBufSize = 0;
		return false;
	}
	state->readRecordBufSize = newSize;
	return true;
}


/*
 * Read a single xlog page including at least [pageptr, reqLen] of valid data
 * via the read_page() callback.
 *
 * Returns -1 if the required page cannot be read for some reason; errormsg_buf
 * is set in that case (unless the error occurs in the read_page callback).
 *
 * We fetch the page from a reader-local cache if we know we have the required
 * data and if there hasn't been any error since caching the data.
 */
 
static int
ReadPageInternal_logminer(XLogReaderState *state, XLogRecPtr pageptr, int reqLen)
{
	int			readLen;
	uint32		targetPageOff;
	XLogSegNo	targetSegNo;
	XLogPageHeader hdr;

	Assert((pageptr % XLOG_BLCKSZ) == 0);

	XLByteToSeg(pageptr, targetSegNo);
	targetPageOff = (pageptr % XLogSegSize);

	/* check whether we have all the requested data already */
	if (targetSegNo == state->readSegNo && targetPageOff == state->readOff &&
		reqLen < state->readLen)
		return state->readLen;

	/*
	 * Data is not in our buffer.
	 *
	 * Every time we actually read the page, even if we looked at parts of it
	 * before, we need to do verification as the read_page callback might now
	 * be rereading data from a different source.
	 *
	 * Whenever switching to a new WAL segment, we read the first page of the
	 * file and validate its header, even if that's not where the target
	 * record is.  This is so that we can check the additional identification
	 * info that is present in the first page's "long" header.
	 */
	if (targetSegNo != state->readSegNo && targetPageOff != 0)
	{
		XLogPageHeader hdr;
		XLogRecPtr	targetSegmentPtr = pageptr - targetPageOff;
		readLen = state->read_page(state, targetSegmentPtr, XLOG_BLCKSZ,
								   state->currRecPtr,
								   state->readBuf, &state->readPageTLI);
		if (readLen < 0)
			goto err;

		/* we can be sure to have enough WAL available, we scrolled back */
		Assert(readLen == XLOG_BLCKSZ);

		hdr = (XLogPageHeader) state->readBuf;
		if(rrctl.logprivate.changewal && pageptr < rrctl.logprivate.startptr)
		{
			if(!rrctl.logprivate.serialwal)
			{
				return 0;
			}
			pageptr = rrctl.logprivate.startptr;
			targetSegmentPtr = pageptr;
			XLByteToSeg(pageptr, targetSegNo);
			targetPageOff = (pageptr % XLogSegSize);
			targetSegmentPtr = pageptr - targetPageOff;
		}
		if (!ValidXLogPageHeader_logminer(state, targetSegmentPtr, hdr))
			goto err;
	}

	/*
	 * First, read the requested data length, but at least a short page header
	 * so that we can validate it.
	 */

	readLen = state->read_page(state, pageptr, Max(reqLen, SizeOfXLogShortPHD),
							   state->currRecPtr,
							   state->readBuf, &state->readPageTLI);
	if(rrctl.logprivate.changewal && pageptr < rrctl.logprivate.startptr)
	{
		if(!rrctl.logprivate.serialwal)
		{
			return 0;
		}
		pageptr = rrctl.logprivate.startptr;
		XLByteToSeg(pageptr, targetSegNo);
		targetPageOff = (pageptr % XLogSegSize);
	}

	if (readLen < 0)
		goto err;

	Assert(readLen <= XLOG_BLCKSZ);

	/* Do we have enough data to check the header length? */
	if (readLen <= SizeOfXLogShortPHD)
		goto err;

	Assert(readLen >= reqLen);

	hdr = (XLogPageHeader) state->readBuf;

	/* still not enough */
	if (readLen < XLogPageHeaderSize(hdr))
	{
		readLen = state->read_page(state, pageptr, XLogPageHeaderSize(hdr),
								   state->currRecPtr,
								   state->readBuf, &state->readPageTLI);
	if(rrctl.logprivate.changewal && pageptr < rrctl.logprivate.startptr)
	{
		if(!rrctl.logprivate.serialwal)
		{
			return 0;
		}
		pageptr = rrctl.logprivate.startptr;
		XLByteToSeg(pageptr, targetSegNo);
		targetPageOff = (pageptr % XLogSegSize);
	}
		if (readLen < 0)
			goto err;
	}

	/*
	 * Now that we know we have the full header, validate it.
	 */
	if (!ValidXLogPageHeader_logminer(state, pageptr, hdr))
		goto err;

	/* update cache information */
	state->readSegNo = targetSegNo;
	state->readOff = targetPageOff;
	state->readLen = readLen;

	return readLen;

err:
	state->readSegNo = 0;
	state->readOff = 0;
	state->readLen = 0;
	return -1;
}


/*
 * Attempt to read an XLOG record.
 *
 * If RecPtr is valid, try to read a record at that position.  Otherwise
 * try to read a record just after the last one previously read.
 *
 * If the read_page callback fails to read the requested data, NULL is
 * returned.  The callback is expected to have reported the error; errormsg
 * is set to NULL.
 *
 * If the reading fails for some other reason, NULL is also returned, and
 * *errormsg is set to a string with details of the failure.
 *
 * The returned pointer (or *errormsg) points to an internal buffer that's
 * valid until the next call to XLogReadRecord.
 */
XLogRecord *
XLogReadRecord_logminer(XLogReaderState *state, XLogRecPtr RecPtr, char **errormsg)
{
	XLogRecord *record;
	XLogRecPtr	targetPagePtr;
	bool		randAccess = false;
	uint32		len,
				total_len;
	uint32		targetRecOff;
	uint32		pageHeaderSize;
	bool		gotheader;
	int 		readOff;

	/* reset error state */
	*errormsg = NULL;
	state->errormsg_buf[0] = '\0';

	ResetDecoder_logminer(state);

	if (RecPtr == InvalidXLogRecPtr)
	{
		RecPtr = state->EndRecPtr;

		if (state->ReadRecPtr == InvalidXLogRecPtr)
			randAccess = true;

		/*
		 * RecPtr is pointing to end+1 of the previous WAL record.	If we're
		 * at a page boundary, no more records can fit on the current page. We
		 * must skip over the page header, but we can't do that until we've
		 * read in the page, since the header size is variable.
		 */
	}
	else
	{
		/*
		 * In this case, the passed-in record pointer should already be
		 * pointing to a valid record starting position.
		 */
		Assert(XRecOffIsValid(RecPtr));
		randAccess = true;		/* allow readPageTLI to go backwards too */
	}

	state->currRecPtr = RecPtr;

	targetPagePtr = RecPtr - (RecPtr % XLOG_BLCKSZ);
	targetRecOff = RecPtr % XLOG_BLCKSZ;

	/*
	 * Read the page containing the record into state->readBuf. Request enough
	 * byte to cover the whole record header, or at least the part of it that
	 * fits on the same page.
	 */
	readOff = ReadPageInternal_logminer(state,
							   targetPagePtr,
						  Min(targetRecOff + SizeOfXLogRecord, XLOG_BLCKSZ));

	if(rrctl.logprivate.changewal && RecPtr < rrctl.logprivate.startptr)
	{
		if(!rrctl.logprivate.serialwal)
		{
			return NULL;
		}
		RecPtr = rrctl.logprivate.startptr;
		state->currRecPtr = RecPtr;
		targetPagePtr = RecPtr - (RecPtr % XLOG_BLCKSZ);
		targetRecOff = RecPtr % XLOG_BLCKSZ;
		rrctl.logprivate.changewal = false;
		randAccess = true;
	}

	
	if (readOff < 0)
		goto err;

	/*
	 * ReadPageInternal_logminer always returns at least the page header, so we can
	 * examine it now.
	 */
	pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) state->readBuf);
	if (targetRecOff == 0)
	{
		/*
		 * At page start, so skip over page header.
		 */
		RecPtr += pageHeaderSize;
		targetRecOff = pageHeaderSize;
	}
	else if (targetRecOff < pageHeaderSize)
	{
		ereport(ERROR,(errmsg("invalid record offset at %X/%X",(uint32) (RecPtr >> 32), (uint32) RecPtr)));
	}

	if ((((XLogPageHeader) state->readBuf)->xlp_info & XLP_FIRST_IS_CONTRECORD) &&
		targetRecOff == pageHeaderSize)
	{
		ereport(ERROR,(errmsg("contrecord is requested by %X/%X",(uint32) (RecPtr >> 32), (uint32) RecPtr)));
	}

	/* ReadPageInternal_logminer has verified the page header */
	Assert(pageHeaderSize <= readOff);

	/*
	 * Read the record length.
	 *
	 * NB: Even though we use an XLogRecord pointer here, the whole record
	 * header might not fit on this page. xl_tot_len is the first field of the
	 * struct, so it must be on this page (the records are MAXALIGNed), but we
	 * cannot access any other fields until we've verified that we got the
	 * whole header.
	 */
	record = (XLogRecord *) (state->readBuf + RecPtr % XLOG_BLCKSZ);
	total_len = record->xl_tot_len;

	/*
	 * If the whole record header is on this page, validate it immediately.
	 * Otherwise do just a basic sanity check on xl_tot_len, and validate the
	 * rest of the header after reading it from the next page.	The xl_tot_len
	 * check is necessary here to ensure that we enter the "Need to reassemble
	 * record" code path below; otherwise we might fail to apply
	 * ValidXLogRecordHeader_logminer at all.
	 */
	if (targetRecOff <= XLOG_BLCKSZ - SizeOfXLogRecord)
	{
		if(!ValidXLogRecordHeader_logminer(state, RecPtr, state->ReadRecPtr, record,
								   randAccess))
			goto err;
		gotheader = true;
	}
	else
	{
		/* XXX: more validation should be done here */
		if (total_len < SizeOfXLogRecord)
		{
			ereport(NOTICE,(errmsg("It has been loaded the xlog file without the normal end.",(uint32) (RecPtr >> 32), (uint32) RecPtr)));
			goto err;
		}
		gotheader = false;
	}

	/*
	 * Enlarge readRecordBuf as needed.
	 */
	if (total_len > state->readRecordBufSize &&
		!allocate_recordbuf_logminer(state, total_len))
	{
		/* We treat this as a "bogus data" condition */
		ereport(ERROR,(errmsg("record length %u at %X/%X too long", total_len, (uint32) (RecPtr >> 32), (uint32) RecPtr)));
	}

	len = XLOG_BLCKSZ - RecPtr % XLOG_BLCKSZ;
	if (total_len > len)
	{
		/* Need to reassemble record */
		char	   *contdata;
		XLogPageHeader pageHeader;
		char	   *buffer;
		uint32		gotlen;

		/* Copy the first fragment of the record from the first page. */
		memcpy(state->readRecordBuf,
			   state->readBuf + RecPtr % XLOG_BLCKSZ, len);
		buffer = state->readRecordBuf + len;
		gotlen = len;

		do
		{
			/* Calculate pointer to beginning of next page */
			targetPagePtr += XLOG_BLCKSZ;

			/* Wait for the next page to become available */
			readOff = ReadPageInternal_logminer(state, targetPagePtr,
								 Min(total_len - gotlen + SizeOfXLogShortPHD,
									 XLOG_BLCKSZ));
			if(!rrctl.logprivate.serialwal)
			{
				return NULL;
			}

			if (readOff < 0)
				goto err;

			Assert(SizeOfXLogShortPHD <= readOff);

			/* Check that the continuation on next page looks valid */
			pageHeader = (XLogPageHeader) state->readBuf;
			if (!(pageHeader->xlp_info & XLP_FIRST_IS_CONTRECORD))
			{
				ereport(ERROR,(errmsg("there is no contrecord flag at %X/%X", (uint32)(RecPtr >> 32), (uint32) RecPtr)));
			}

			/*
			 * Cross-check that xlp_rem_len agrees with how much of the record
			 * we expect there to be left.
			 */
			if (pageHeader->xlp_rem_len == 0 ||
				total_len != (pageHeader->xlp_rem_len + gotlen))
			{
				ereport(ERROR,(errmsg("invalid contrecord length %u at %X/%X",
									  pageHeader->xlp_rem_len, (uint32) (RecPtr >> 32), (uint32) RecPtr)));
			}

			/* Append the continuation from this page to the buffer */
			pageHeaderSize = XLogPageHeaderSize(pageHeader);

			if (readOff < pageHeaderSize)
				readOff = ReadPageInternal_logminer(state, targetPagePtr,
										   pageHeaderSize);

			Assert(pageHeaderSize <= readOff);

			contdata = (char *) state->readBuf + pageHeaderSize;
			len = XLOG_BLCKSZ - pageHeaderSize;
			if (pageHeader->xlp_rem_len < len)
				len = pageHeader->xlp_rem_len;

			if (readOff < pageHeaderSize + len)
				readOff = ReadPageInternal_logminer(state, targetPagePtr,
										   pageHeaderSize + len);

			memcpy(buffer, (char *) contdata, len);
			buffer += len;
			gotlen += len;

			/* If we just reassembled the record header, validate it. */
			if (!gotheader)
			{
				record = (XLogRecord *) state->readRecordBuf;
				ValidXLogRecordHeader_logminer(state, RecPtr, state->ReadRecPtr,
										   record, randAccess);
				gotheader = true;
			}
		} while (gotlen < total_len);

		Assert(gotheader);

		record = (XLogRecord *) state->readRecordBuf;
		if (!ValidXLogRecord_logminer(state, record, RecPtr))
			goto err;

		pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) state->readBuf);
		state->ReadRecPtr = RecPtr;
		state->EndRecPtr = targetPagePtr + pageHeaderSize
			+ MAXALIGN(pageHeader->xlp_rem_len);
	}
	else
	{
		/* Wait for the record data to become available */
		readOff = ReadPageInternal_logminer(state, targetPagePtr,
								 Min(targetRecOff + total_len, XLOG_BLCKSZ));
		if (readOff < 0)
			goto err;

		/* Record does not cross a page boundary */
		if (!ValidXLogRecord_logminer(state, record, RecPtr))
			goto err;

		state->EndRecPtr = RecPtr + MAXALIGN(total_len);

		state->ReadRecPtr = RecPtr;
		memcpy(state->readRecordBuf, record, total_len);
	}

	/*
	 * Special processing if it's an XLOG SWITCH record
	 */
	if (record->xl_rmid == RM_XLOG_ID && record->xl_info == XLOG_SWITCH)
	{
		/* Pretend it extends to end of segment */
		state->EndRecPtr += XLogSegSize - 1;
		state->EndRecPtr -= state->EndRecPtr % XLogSegSize;
	}
	
	/*If return NULL here ,it is reached the end of xlog, 
           otherwise(other return NULL this func) it is got a boundary record with discontinuous xlogfile*/


	rrctl.logprivate.serialwal = true;
	if(!record)
		rrctl.logprivate.endptr_reached = true;
	if (DecodeXLogRecord(state, record, errormsg))
		return record;
	else
	{
		return NULL;
	}

err:

	/*
	 * Invalidate the xlog page we've cached. We might read from a different
	 * source after failure.
	 */
	state->readSegNo = 0;
	state->readOff = 0;
	state->readLen = 0;

	if (state->errormsg_buf[0] != '\0')
		*errormsg = state->errormsg_buf;
	rrctl.logprivate.serialwal = true;
	rrctl.logprivate.endptr_reached = true;
	return NULL;
}

/*
 * Validate an XLOG record header.
 *
 * This is just a convenience subroutine to avoid duplicated code in
 * XLogReadRecord.  It's not intended for use from anywhere else.
 */
static bool
ValidXLogRecordHeader_logminer(XLogReaderState *state, XLogRecPtr RecPtr,
					  XLogRecPtr PrevRecPtr, XLogRecord *record,
					  bool randAccess)
{
	if (record->xl_tot_len < SizeOfXLogRecord)
	{
		return false;
	}
	if (record->xl_rmid > RM_MAX_ID)
	{
		ereport(ERROR,(errmsg("invalid resource manager ID %u at %X/%X",
							  record->xl_rmid, (uint32) (RecPtr >> 32),(uint32) RecPtr)));
	}
	if (randAccess)
	{
		/*
		 * We can't exactly verify the prev-link, but surely it should be less
		 * than the record's own address.
		 */
		if (!(record->xl_prev < RecPtr))
		{
			ereport(ERROR,(errmsg("record with incorrect prev-link %X/%X at %X/%X",
								  (uint32) (record->xl_prev >> 32),
								  (uint32) record->xl_prev,
								  (uint32) (RecPtr >> 32), (uint32) RecPtr)));
		}
	}
	else
	{
		/*
		 * Record's prev-link should exactly match our previous location. This
		 * check guards against torn WAL pages where a stale but valid-looking
		 * WAL record starts on a sector boundary.
		 */
		if (record->xl_prev != PrevRecPtr)
		{
			ereport(ERROR,(errmsg("record with incorrect prev-link %X/%X at %X/%X",
								  (uint32) (record->xl_prev >> 32),
								  (uint32) record->xl_prev,
								  (uint32) (RecPtr >> 32), (uint32) RecPtr)));
		}
	}

	return true;
}



/*
 * CRC-check an XLOG record.  We do not believe the contents of an XLOG
 * record (other than to the minimal extent of computing the amount of
 * data to read in) until we've checked the CRCs.
 *
 * We assume all of the record (that is, xl_tot_len bytes) has been read
 * into memory at *record.  Also, ValidXLogRecordHeader_logminer() has accepted the
 * record's header, which means in particular that xl_tot_len is at least
 * SizeOfXlogRecord, so it is safe to fetch xl_len.
 */
static bool
ValidXLogRecord_logminer(XLogReaderState *state, XLogRecord *record, XLogRecPtr recptr)
{
	pg_crc32c	crc;

	/* Calculate the CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
	/* include the record header last */
	COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(record->xl_crc, crc))
	{
		ereport(ERROR,(errmsg("incorrect resource manager data checksum in record at %X/%X",
							  (uint32) (recptr >> 32), (uint32) recptr)));
	}

	return true;
}


/*
 * Validate a page header
 */
static bool
ValidXLogPageHeader_logminer(XLogReaderState *state, XLogRecPtr recptr,
						XLogPageHeader hdr)
{
	XLogRecPtr	recaddr;
	XLogSegNo	segno;
	int32		offset;

	Assert((recptr % XLOG_BLCKSZ) == 0);

	XLByteToSeg(recptr, segno);
	offset = recptr % XLogSegSize;

	XLogSegNoOffsetToRecPtr(segno, offset, recaddr);

	if (hdr->xlp_magic != XLOG_PAGE_MAGIC)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);
		return false;
	}

	if ((hdr->xlp_info & ~XLP_ALL_FLAGS) != 0)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);
		ereport(ERROR,(errmsg("invalid info bits %04X in log segment %s, offset %u",
							  hdr->xlp_info,
							  fname,
							  offset)));
		return false;
	}

	if (hdr->xlp_info & XLP_LONG_HEADER)
	{
		XLogLongPageHeader longhdr = (XLogLongPageHeader) hdr;

		if (state->system_identifier &&
			longhdr->xlp_sysid != state->system_identifier)
		{
			char		fhdrident_str[32];
			char		sysident_str[32];

			/*
			 * Format sysids separately to keep platform-dependent format code
			 * out of the translatable message string.
			 */
			snprintf(fhdrident_str, sizeof(fhdrident_str), UINT64_FORMAT,
					 longhdr->xlp_sysid);
			snprintf(sysident_str, sizeof(sysident_str), UINT64_FORMAT,
					 state->system_identifier);
			ereport(ERROR,(errmsg("WAL file is from different database system: WAL file database system identifier is %s, pg_control database system identifier is %s",
								  fhdrident_str, sysident_str)));
			return false;
		}
		else if (longhdr->xlp_seg_size != XLogSegSize)
		{
			ereport(ERROR,(errmsg("WAL file is from different database system: incorrect XLOG_SEG_SIZE in page header")));
			return false;
		}
		else if (longhdr->xlp_xlog_blcksz != XLOG_BLCKSZ)
		{
			ereport(ERROR,(errmsg("WAL file is from different database system: incorrect XLOG_BLCKSZ in page header")));
			return false;
		}
	}
	else if (offset == 0)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);

		/* hmm, first page of file doesn't have a long header? */
		ereport(ERROR,(errmsg( "invalid info bits %04X in log segment %s, offset %u",
							  hdr->xlp_info,
							  fname,
							  offset)));
		return false;
	}

	if (hdr->xlp_pageaddr != recaddr)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);
		rrctl.logprivate.endptr_reached = true;
		/*
		ereport(NOTICE,(errmsg("unexpected pageaddr %X/%X in log segment %s, offset %u",
							  (uint32) (hdr->xlp_pageaddr >> 32), (uint32) hdr->xlp_pageaddr,
							  fname,
							  offset)));
		*/
		return false;
	}

	/*
	 * Since child timelines are always assigned a TLI greater than their
	 * immediate parent's TLI, we should never see TLI go backwards across
	 * successive pages of a consistent WAL sequence.
	 *
	 * Sometimes we re-read a segment that's already been (partially) read. So
	 * we only verify TLIs for pages that are later than the last remembered
	 * LSN.
	 */
	if (recptr > state->latestPagePtr)
	{
		if (hdr->xlp_tli < state->latestPageTLI)
		{
			char		fname[MAXFNAMELEN];

			XLogFileName(fname, state->readPageTLI, segno);

			ereport(ERROR,(errmsg("out-of-sequence timeline ID %u (after %u) in log segment %s, offset %u",
								  hdr->xlp_tli,
								  state->latestPageTLI,
								  fname,
								  offset)));
			return false;
		}
	}
	state->latestPagePtr = recptr;
	state->latestPageTLI = hdr->xlp_tli;

	return true;
}


 /*
 * Functions that are currently not needed in the backend, but are better
 * implemented inside xlogreader.c because of the internal facilities available
 * here.
 */

/*
 * Find the first record with an lsn >= RecPtr.
 *
 * Useful for checking whether RecPtr is a valid xlog address for reading, and
 * to find the first valid address after some address when dumping records for
 * debugging purposes.
 */
XLogRecPtr
XLogFindFirstRecord(XLogReaderState *state, XLogRecPtr RecPtr)
 {
	 XLogReaderState saved_state = *state;
	 XLogRecPtr  targetPagePtr;
	 XLogRecPtr  tmpRecPtr;
	 int		 targetRecOff;
	 XLogRecPtr  found = InvalidXLogRecPtr;
	 uint32 	 pageHeaderSize;
	 XLogPageHeader header;
	 int		 readLen;
	 char		*errormsg;
 
	 Assert(!XLogRecPtrIsInvalid(RecPtr));
 
	 targetRecOff = RecPtr % XLOG_BLCKSZ;
 
	 /* scroll back to page boundary */
	 targetPagePtr = RecPtr - targetRecOff;
 
	 /* Read the page containing the record */
	 readLen = ReadPageInternal_logminer(state, targetPagePtr, targetRecOff);
	 if (readLen < 0)
		 goto err;
 
	 header = (XLogPageHeader) state->readBuf;
 
	 pageHeaderSize = XLogPageHeaderSize(header);
 
	 /* make sure we have enough data for the page header */
	 readLen = ReadPageInternal_logminer(state, targetPagePtr, pageHeaderSize);
	 if (readLen < 0)
		 goto err;
 
	 /* skip over potential continuation data */
	 if (header->xlp_info & XLP_FIRST_IS_CONTRECORD)
	 {
		 /* record headers are MAXALIGN'ed */
		 tmpRecPtr = targetPagePtr + pageHeaderSize
			 + MAXALIGN(header->xlp_rem_len);
	 }
	 else
	 {
		 tmpRecPtr = targetPagePtr + pageHeaderSize;
	 }
 
	 /*
	  * we know now that tmpRecPtr is an address pointing to a valid XLogRecord
	  * because either we're at the first record after the beginning of a page
	  * or we just jumped over the remaining data of a continuation.
	  */
	 rrctl.logprivate.changewal = false;

	 while (XLogReadRecord_logminer(state, tmpRecPtr, &errormsg) != NULL)
	 {
		 /* continue after the record */
		 tmpRecPtr = InvalidXLogRecPtr;
 
		 /* past the record we've found, break out */
		 if (RecPtr <= state->ReadRecPtr)
		 {
			 found = state->ReadRecPtr;
			 if (found == InvalidXLogRecPtr)
					ereport(ERROR,(errmsg("could not find a valid record after %X/%X",
						(uint32) (RecPtr >> 32),
						(uint32) RecPtr)));
		
			if (found != RecPtr && (RecPtr % XLogSegSize) != 0)
				printf("first record is after %X/%X, at %X/%X, skipping over %u bytes\n",
				   (uint32) (RecPtr >> 32), (uint32) RecPtr,
				   (uint32) (found >> 32), (uint32) found,
				   (uint32) (found - RecPtr));
			 goto out;
		 }
	 }
 
 err:
 out:
	 /* Reset state to what we had before finding the record */
	 state->readSegNo = 0;
	 state->readOff = 0;
	 state->readLen = 0;
	 state->ReadRecPtr = saved_state.ReadRecPtr;
	 state->EndRecPtr = saved_state.EndRecPtr;
 
	 return found;
 }

 
static bool
ifSerialWalfile(XLogSegNo xlnPre,XLogSegNo xlnCur)
{
	XLogSegNo	segIdPre;
	XLogSegNo	xlogIdPre;
	XLogSegNo	segIdCur;
	XLogSegNo	xlogIdCur;
	XLogSegNo	temp = 1;

	segIdPre = xlnPre % (temp << 32);
	xlogIdPre = xlnPre / (temp << 32);
	segIdCur = xlnCur % (temp << 32);
	xlogIdCur = xlnCur / (temp << 32);

	if(xlogIdPre == xlogIdCur && segIdPre + 1 == segIdCur)
		return true;
	if(xlogIdPre + 1 == xlogIdCur && 0xff == segIdPre && 0x01 == segIdCur)
		return true;
	return false;
}


int
XLogMinerXLogRead(const char *directory, TimeLineID *timeline_id,
				 XLogRecPtr startptr, char *buf, Size count)
{
	char	   *p;
	XLogRecPtr	recptr;
	Size		nbytes;
	XLogSegNo sendSegNo_temp = 0;

	int	*sendFile;
	XLogSegNo *sendSegNo;
	uint32 *sendOff;

	sendFile = &rrctl.lfctx.sendFile;
	sendSegNo = &rrctl.lfctx.sendSegNo;
	sendOff = &rrctl.lfctx.sendOff;

	p = buf;
	recptr = startptr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;
		char		*xlogfilename = NULL;
		char		*temp_path = NULL;
		char		*temp_fname = NULL;
		TimeLineID	tid;
		

		startoff = recptr % XLogSegSize;

		if (*sendFile < 0 || !XLByteInSeg(recptr, *sendSegNo))
		{

			/* Switch to another logfile segment */
			if (*sendFile >= 0)
			{
				close(*sendFile);
				*sendFile = -1;
			}
			xlogfilename = getNextXlogFile((char*)(&rrctl.lfctx),false);
			if(xlogfilename)
			{
				split_path_fname(xlogfilename, &temp_path, &temp_fname);
				*sendFile = xlog_file_open(temp_path, temp_fname);
			}
			else
				return PG_LOGMINER_WALFILE_ENDALL;
			
			if (*sendFile < 0)
			{
				ereport(NOTICE,(errmsg("could not find file \"%s\": %s",temp_fname, strerror(errno))));
				*sendFile = -1;
				*sendSegNo = 0;
				*sendOff = 0;
				return PG_LOGMINER_WALFILE_ERROR_NOFIND;
			}

			XLogFromFileName(temp_fname, &tid, &sendSegNo_temp);
			*timeline_id = tid;
			XLogSegNoOffsetToRecPtr(sendSegNo_temp, 0, rrctl.logprivate.startptr);
			if(!ifSerialWalfile(*sendSegNo,sendSegNo_temp) && 0 != *sendSegNo)
			{
				rrctl.logprivate.serialwal = false;
				rrctl.logprivate.changewal= true;
			}
			else
				rrctl.logprivate.serialwal = true;
			*sendSegNo = sendSegNo_temp;

			recptr = rrctl.logprivate.startptr;
			rrctl.logprivate.changewal= true;
			if(temp_path)
				pfree(temp_path);
			if(temp_fname)
				pfree(temp_fname);
			*sendOff = 0;
		}

		/* Need to seek in the file? */
		if (*sendOff != startoff)
		{
			if (lseek(*sendFile, (off_t) startoff, SEEK_SET) < 0)
			{
				int			err = errno;
				char		fname[MAXPGPATH];

				XLogFileName(fname, tid, *sendSegNo);
				ereport(ERROR,(errmsg("could not seek in log segment %s to offset %u: %s",
							fname, startoff, strerror(err))));
			}
			*sendOff = startoff;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (XLogSegSize - startoff))
			segbytes = XLogSegSize - startoff;
		else
			segbytes = nbytes;

		readbytes = read(*sendFile, p, segbytes);
		if (readbytes <= 0)
		{
			int			err = errno;
			char		fname[MAXPGPATH];

			XLogFileName(fname, tid, *sendSegNo);
			ereport(ERROR,(errmsg("could not read from log segment %s, offset %d, length %d: %s"),
						fname, *sendOff, segbytes, strerror(err)));
		}

		/* Update state for read */
		recptr += readbytes;

		*sendOff += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}
	return 0;
}

/*
strategy that get data from xlogfile list
*/
int
XLogMinerReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				 XLogRecPtr targetPtr, char *readBuff, TimeLineID *curFileTLI)
{
	XLogMinerPrivate *private = state->private_data;
	int			count = XLOG_BLCKSZ;
	int			statu = 0;
	
	statu = XLogMinerXLogRead(NULL, &private->timeline, targetPagePtr,
					 readBuff, count);
	*curFileTLI = private->timeline;
	if(PG_LOGMINER_WALFILE_ERROR_NOFIND == statu || PG_LOGMINER_WALFILE_ENDALL == statu)
		count = PG_LOGMINER_WALFILE_ERROR_COUNT;
	return count;
}


