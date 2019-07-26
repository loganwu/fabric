/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package node

import (
	"github.com/hyperledger/fabric/core/ledger/kvledger"
	"github.com/hyperledger/fabric/peer/common"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	blknumb   uint64
	channelId string
)

func archiveCmd() *cobra.Command {
	nodeArchiveCmd.ResetFlags()
	flags := nodeArchiveCmd.Flags()
	flags.StringVarP(&channelId, "channelID", "c", common.UndefinedParamValue, "Channel to archive.")
	flags.Uint64VarP(&blknumb, "blocknumber", "b", 0, "Block number to which peer will arhive to.")

	return nodeArchiveCmd
}

var nodeArchiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "archive at a block number.",
	Long:  `Archive peer to a specified block number. Before executing this command, the peer must be stopped `,
	RunE: func(cmd *cobra.Command, args []string) error {
		if channelId == common.UndefinedParamValue {
			return errors.New("Must supply channel ID")
		}
		if blknumb <= 1 {
			return errors.New("archive block number must be bigger than 1")
		}
		return kvledger.ArchiveKVLedger(channelId, blknumb)
	},
}
