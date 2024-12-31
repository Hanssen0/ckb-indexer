import {
  autoRun,
  formatSortableInt,
  headerToRepoBlock,
  parseSortableInt,
  withTransaction,
} from "@app/commons";
import { Block } from "@app/schemas";
import { ccc } from "@ckb-ccc/core";
import { Injectable, Logger } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import {
  And,
  EntityManager,
  LessThan,
  LessThanOrEqual,
  MoreThan,
} from "typeorm";
import { SyncStatusRepo, UdtBalanceRepo, UdtInfoRepo } from "./repos";
import { BlockRepo } from "./repos/block.repo";
import { UdtParserBuilder } from "./udtParser";

const SYNC_KEY = "SYNCED";
const PENDING_KEY = "PENDING";

type GeneratedBlock = {
  height: ccc.Num;
  block?: ccc.ClientBlock;
  txs: {
    tx: ccc.Transaction;
    prefetchedInputs: Promise<ccc.CellInput>[];
  }[];
};
// get block in range (start, end]
async function* getBlocks(
  client: ccc.Client,
  startLike: ccc.NumLike,
  endLike: ccc.NumLike,
): AsyncGenerator<GeneratedBlock> {
  const start = ccc.numFrom(startLike);
  const end = ccc.numFrom(endLike);

  const blocksLength = Number(end - start);
  // Prefetch for performance
  const blocks = [];
  for (let i = 0; i < blocksLength; i += 1) {
    const height = ccc.numFrom(i + 1) + start;
    blocks.push(
      (async (): Promise<GeneratedBlock> => {
        const block = await client.getBlockByNumber(height);

        if (i) {
          await blocks[i - 1];
        }

        const txs = [];
        for (const tx of block?.transactions ?? []) {
          txs.push({
            tx,
            prefetchedInputs: tx.inputs.map(async (input) => {
              await input.completeExtraInfos(client);
              return input;
            }),
          });
        }

        return { height, block, txs };
      })(),
    );
  }

  for (const block of blocks) {
    yield await block;
  }
}

@Injectable()
export class SyncService {
  private readonly logger = new Logger(SyncService.name);
  private readonly client: ccc.Client;
  private readonly blockLimitPerInterval: number | undefined;
  private readonly blockSyncStart: number | undefined;
  private readonly confirmations: number | undefined;

  private startTip?: ccc.Num;
  private startTipTime?: number;
  private syncedBlocks: number = 0;
  private syncedBlockTime: number = 0;

  constructor(
    configService: ConfigService,
    private readonly udtParserBuilder: UdtParserBuilder,
    private readonly entityManager: EntityManager,
    private readonly syncStatusRepo: SyncStatusRepo,
    private readonly udtInfoRepo: UdtInfoRepo,
    private readonly udtBalanceRepo: UdtBalanceRepo,
    private readonly blockRepo: BlockRepo,
  ) {
    const isMainnet = configService.get<boolean>("sync.isMainnet");
    const ckbRpcUri = configService.get<string>("sync.ckbRpcUri");
    const maxConcurrent = configService.get<number>("sync.maxConcurrent");
    this.client = isMainnet
      ? new ccc.ClientPublicMainnet({ url: ckbRpcUri, maxConcurrent })
      : new ccc.ClientPublicTestnet({ url: ckbRpcUri, maxConcurrent });

    this.blockLimitPerInterval = configService.get<number>(
      "sync.blockLimitPerInterval",
    );
    this.blockSyncStart = configService.get<number>("sync.blockSyncStart");
    this.confirmations = configService.get<number>("sync.confirmations");

    const syncInterval = configService.get<number>("sync.interval");
    if (syncInterval !== undefined) {
      autoRun(this.logger, syncInterval, () => this.sync());
    }

    const clearInterval = configService.get<number>("sync.clearInterval");
    if (this.confirmations !== undefined && clearInterval !== undefined) {
      autoRun(this.logger, clearInterval, () => this.clear());
    }
  }

  async sync() {
    // Will break when endBlock === tip
    while (true) {
      const pendingStatus = await this.syncStatusRepo.syncHeight(
        PENDING_KEY,
        this.blockSyncStart,
      );
      const pendingHeight = parseSortableInt(pendingStatus.value);

      const tipTime = Date.now();
      const tip = await this.client.getTip();
      const tipCost =
        this.startTip !== undefined && this.startTipTime !== undefined
          ? (tipTime - this.startTipTime) / Number(tip - this.startTip)
          : 9999999999;
      if (this.startTip === undefined || this.startTipTime === undefined) {
        this.startTip = tip;
        this.startTipTime = tipTime;
      }

      const endBlock =
        this.blockLimitPerInterval === undefined
          ? tip
          : ccc.numMin(
              pendingHeight + ccc.numFrom(this.blockLimitPerInterval),
              tip,
            );

      let txsCount = 0;
      for await (const { height, block, txs } of getBlocks(
        this.client,
        pendingHeight,
        endBlock,
      )) {
        txsCount += txs.length;
        if (!block) {
          this.logger.error(`Failed to get block ${height}`);
          break;
        }

        const udtParser = this.udtParserBuilder.build(height);

        await withTransaction(
          this.entityManager,
          undefined,
          async (entityManager) => {
            const blockRepo = new BlockRepo(entityManager);
            const syncStatusRepo = new SyncStatusRepo(entityManager);
            await blockRepo.insert({
              hash: block.header.hash,
              parentHash: block.header.parentHash,
              height: formatSortableInt(block.header.number),
              timestamp: Number(block.header.timestamp / 1000n),
            });

            for (const { tx, prefetchedInputs } of txs) {
              await udtParser.udtInfoHandleTx(
                entityManager,
                tx,
                prefetchedInputs,
              );
            }

            await syncStatusRepo.updateSyncHeight(pendingStatus, height);
          },
        );

        this.syncedBlocks += 1;
      }

      this.syncedBlockTime += Date.now() - tipTime;

      const blocksDiff = Number(tip - endBlock);
      const syncCost = this.syncedBlockTime / this.syncedBlocks;
      const estimatedTime =
        (blocksDiff * syncCost * tipCost) / (tipCost - syncCost);
      this.logger.log(
        `Tip ${tip}, synced block ${endBlock}, ${blocksDiff} blocks / ~${estimatedTime !== undefined ? (estimatedTime / 1000 / 60).toFixed(1) : "-"} mins left. ${txsCount} transactions processed`,
      );

      if (endBlock === tip) {
        break;
      }
    }
  }

  async clear() {
    if (this.confirmations === undefined) {
      return;
    }
    if (!(await this.syncStatusRepo.initialized())) {
      return;
    }

    const pendingHeight = parseSortableInt(
      (await this.syncStatusRepo.assertSyncHeight(PENDING_KEY)).value,
    );
    const confirmedHeight = pendingHeight - ccc.numFrom(this.confirmations);

    const syncedStatus = await this.syncStatusRepo.assertSyncHeight(SYNC_KEY);
    if (parseSortableInt(syncedStatus.value) >= confirmedHeight) {
      return;
    }
    await this.syncStatusRepo.updateSyncHeight(syncedStatus, confirmedHeight);
    this.logger.log(`Clearing up to height ${confirmedHeight}`);

    let deleteUdtInfoCount = 0;
    while (true) {
      const udtInfo = await this.udtInfoRepo.findOne({
        where: {
          updatedAtHeight: And(
            LessThanOrEqual(formatSortableInt(confirmedHeight)),
            MoreThan(formatSortableInt("-1")),
          ),
        },
        order: {
          updatedAtHeight: "DESC",
        },
      });
      if (!udtInfo) {
        // No more confirmed data
        break;
      }

      await withTransaction(
        this.entityManager,
        undefined,
        async (entityManager) => {
          const udtInfoRepo = new UdtInfoRepo(entityManager);

          // Delete all history data, and set the latest confirmed data as permanent data
          const deleted = await udtInfoRepo.delete({
            hash: udtInfo.hash,
            updatedAtHeight: LessThan(udtInfo.updatedAtHeight),
          });
          deleteUdtInfoCount += deleted.affected ?? 0;

          await udtInfoRepo.update(
            { id: udtInfo.id },
            { updatedAtHeight: formatSortableInt("-1") },
          );
        },
      );
    }

    let deleteUdtBalanceCount = 0;
    while (true) {
      const udtBalance = await this.udtBalanceRepo.findOne({
        where: {
          updatedAtHeight: And(
            LessThanOrEqual(formatSortableInt(confirmedHeight)),
            MoreThan(formatSortableInt("-1")),
          ),
        },
        order: {
          updatedAtHeight: "DESC",
        },
      });
      if (!udtBalance) {
        // No more confirmed data
        break;
      }

      await withTransaction(
        this.entityManager,
        undefined,
        async (entityManager) => {
          const udtBalanceRepo = new UdtBalanceRepo(entityManager);

          // Delete all history data, and set the latest confirmed data as permanent data
          const deleted = await udtBalanceRepo.delete({
            addressHash: udtBalance.addressHash,
            tokenHash: udtBalance.tokenHash,
            updatedAtHeight: LessThan(udtBalance.updatedAtHeight),
          });
          deleteUdtBalanceCount += deleted.affected ?? 0;

          await udtBalanceRepo.update(
            { id: udtBalance.id },
            { updatedAtHeight: formatSortableInt("-1") },
          );
        },
      );
    }

    this.logger.log(
      `Cleared ${deleteUdtInfoCount} confirmed UDT info, ${deleteUdtBalanceCount} confirmed UDT balance`,
    );
  }

  async getBlockHeader(params: {
    blockNumber?: number;
    fromDb: boolean;
  }): Promise<Block | undefined> {
    const { blockNumber, fromDb } = params;
    if (blockNumber) {
      if (fromDb) {
        return await this.blockRepo.getBlockByNumber(ccc.numFrom(blockNumber));
      } else {
        const header = await this.client.getHeaderByNumber(blockNumber);
        return headerToRepoBlock(header);
      }
    } else {
      if (fromDb) {
        return await this.blockRepo.getTipBlock();
      } else {
        const header = await this.client.getTipHeader();
        return headerToRepoBlock(header);
      }
    }
  }
}
