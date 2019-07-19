/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package fsblkstorage

import (
	"os"

	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/pkg/errors"
)

type archiveMgr struct {
	ledgerID       string
	ledgerDir      string
	indexDir       string
	dbProvider     *leveldbhelper.Provider
	indexStore     *blockIndex
	targetBlockNum uint64
}

// Archive reverts changes made to the block store beyond a given block number.
func Archive(blockStorageDir, ledgerID string, targetBlockNum uint64, indexConfig *blkstorage.IndexConfig) error {
	r, err := newArchiveMgr(blockStorageDir, ledgerID, indexConfig, targetBlockNum)
	if err != nil {
		return err
	}
	defer r.dbProvider.Close()

	logger.Infof("Rolling back block index to block number [%d]", targetBlockNum)
	if err := r.archiveBlockIndex(); err != nil {
		return err
	}

	logger.Infof("Rolling back block files to block number [%d]", targetBlockNum)
	if err := r.archiveBlockFiles(); err != nil {
		return err
	}

	return nil
}

func newArchiveMgr(blockStorageDir, ledgerID string, indexConfig *blkstorage.IndexConfig, targetBlockNum uint64) (*archiveMgr, error) {
	r := &archiveMgr{}

	r.ledgerID = ledgerID
	conf := &Conf{blockStorageDir: blockStorageDir}
	r.ledgerDir = conf.getLedgerBlockDir(ledgerID)
	r.targetBlockNum = targetBlockNum

	r.indexDir = conf.getIndexDir()
	r.dbProvider = leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: r.indexDir})
	var err error
	indexDB := r.dbProvider.GetDBHandle(ledgerID)
	r.indexStore, err = newBlockIndex(indexConfig, indexDB)
	return r, err
}

func (r *archiveMgr) archiveBlockIndex() error {
	lastBlockNumber, err := r.indexStore.getLastBlockIndexed()
	if err == errIndexEmpty {
		return nil
	}
	if err != nil {
		return err
	}

	// we remove index associated with only 10 blocks at a time
	// to avoid overuse of memory occupied by the leveldb batch.
	// If we assume a block size of 2000 transactions and 4 indices
	// per transaction and 2 index per block, the total number of
	// index keys to be added in the batch would be 80020. Even if a
	// key takes 100 bytes (overestimation), the memory utilization
	// of a batch of 10 blocks would be 7 MB only.
	batchLimit := uint64(10)

	// start each iteration of the loop with full range for deletion
	// and shrink the range to batchLimit if the range is greater than batchLimit
	start, end := r.targetBlockNum+1, lastBlockNumber
	for end >= start {
		if end-start >= batchLimit {
			start = end - batchLimit + 1 // 1 is added as range is inclusive
		}
		logger.Infof("Deleting index associated with block number [%d] to [%d]", start, end)
		if err := r.deleteIndexEntriesRange(start, end); err != nil {
			return err
		}
		start, end = r.targetBlockNum+1, start-1
	}
	return nil
}

func (r *archiveMgr) deleteIndexEntriesRange(startBlkNum, endBlkNum uint64) error {
	batch := leveldbhelper.NewUpdateBatch()
	lp, err := r.indexStore.getBlockLocByBlockNum(startBlkNum)
	if err != nil {
		return err
	}
	stream, err := newBlockStream(r.ledgerDir, lp.fileSuffixNum, int64(lp.offset), -1)
	defer stream.close()

	numberOfBlocksToRetrieve := endBlkNum - startBlkNum + 1
	for numberOfBlocksToRetrieve > 0 {
		blockBytes, placementInfo, err := stream.nextBlockBytesAndPlacementInfo()
		if err != nil {
			return err
		}

		blockInfo, err := extractSerializedBlockInfo(blockBytes)
		if err != nil {
			return err
		}

		err = populateBlockInfoWithDuplicateTxids(blockInfo, placementInfo, r.indexStore)
		if err != nil {
			return err
		}
		addIndexEntriesToBeDeleted(batch, blockInfo, r.indexStore)
		numberOfBlocksToRetrieve--
	}

	batch.Put(indexCheckpointKey, encodeBlockNum(startBlkNum-1))
	return r.indexStore.db.WriteBatch(batch, true)
}

func (r *archiveMgr) archiveBlockFiles() error {
	logger.Infof("Deleting checkpointInfo")
	if err := r.indexStore.db.Delete(blkMgrInfoKey, true); err != nil {
		return err
	}
	// must not use index for block location search since the index can be behind the target block
	targetFileNum, err := binarySearchFileNumForBlock(r.ledgerDir, r.targetBlockNum)
	if err != nil {
		return err
	}
	lastFileNum, err := retrieveLastFileSuffix(r.ledgerDir)
	if err != nil {
		return err
	}

	logger.Infof("Removing all block files with suffixNum in the range [%d] to [%d]",
		targetFileNum+1, lastFileNum)

	for n := lastFileNum; n >= targetFileNum+1; n-- {
		filepath := deriveBlockfilePath(r.ledgerDir, n)
		if err := os.Remove(filepath); err != nil {
			return errors.Wrapf(err, "error removing the block file [%s]", filepath)
		}
	}

	logger.Infof("Truncating block file [%d] to the end boundary of block number [%d]", targetFileNum, r.targetBlockNum)
	endOffset, err := calculateEndOffSet(r.ledgerDir, targetFileNum, r.targetBlockNum)
	if err != nil {
		return err
	}

	filePath := deriveBlockfilePath(r.ledgerDir, targetFileNum)
	if err := os.Truncate(filePath, endOffset); err != nil {
		return errors.Wrapf(err, "error trucating the block file [%s]", filePath)
	}

	return nil
}
