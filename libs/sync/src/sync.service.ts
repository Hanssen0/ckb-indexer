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
import { Worker } from "worker_threads";
import { SyncStatusRepo, UdtBalanceRepo, UdtInfoRepo } from "./repos";
import { BlockRepo } from "./repos/block.repo";
import { UdtParser } from "./udtParser";

const SYNC_KEY = "SYNCED";
const PENDING_KEY = "PENDING";

function getBlocksOnWorker(
  worker: Worker,
  start: ccc.NumLike,
  end: ccc.NumLike,
): Promise<{ height: ccc.Num; block: ccc.ClientBlock }[]> {
  return new Promise((resolve, reject) => {
    worker.removeAllListeners("message");
    worker.removeAllListeners("error");

    worker.postMessage({
      start,
      end,
    });

    worker.addListener("message", resolve);
    worker.addListener("error", reject);
  });
}

async function* getBlocks(props: {
  start: ccc.NumLike;
  end: ccc.NumLike;
  workers?: number;
  chunkSize?: number;
  isMainnet?: boolean;
  rpcUri?: string;
  rpcTimeout?: number;
  maxConcurrent?: number;
}) {
  const start = ccc.numFrom(props.start);
  const end = ccc.numFrom(props.end);
  const workers = props.workers ?? 8;
  const chunkSize = ccc.numFrom(props.chunkSize ?? 5);

  const queries: ReturnType<typeof getBlocksOnWorker>[] = [];
  const freeWorkers = Array.from(
    new Array(workers),
    () =>
      new Worker("./dist/workers/getBlock.js", {
        workerData: {
          isMainnet: props.isMainnet,
          rpcUri: props.rpcUri,
          rpcTimeout: props.rpcTimeout,
          maxConcurrent: props.maxConcurrent,
        },
      }),
  );

  let offset = start;
  while (true) {
    const workerEnd = ccc.numMin(offset + chunkSize, end);
    if (freeWorkers.length === 0 || offset === workerEnd) {
      const query = queries.shift();
      if (!query) {
        break;
      }
      for (const block of await query) {
        yield block;
      }
      continue;
    }

    const worker = freeWorkers.shift()!;
    queries.push(
      getBlocksOnWorker(worker, offset, workerEnd).then((res) => {
        freeWorkers.push(worker);
        return res;
      }),
    );
    offset = workerEnd;
  }

  freeWorkers.forEach((worker) => worker.terminate());
}

@Injectable()
export class SyncService {
  private readonly logger = new Logger(SyncService.name);

  private readonly isMainnet: boolean | undefined;
  private readonly ckbRpcUri: string | undefined;
  private readonly ckbRpcTimeout: number | undefined;
  private readonly maxConcurrent: number | undefined;
  private readonly client: ccc.Client;

  private readonly threads: number | undefined;
  private readonly blockChunk: number | undefined;
  private readonly blockLimitPerInterval: number | undefined;
  private readonly blockSyncStart: number | undefined;
  private readonly confirmations: number | undefined;

  private startTip?: ccc.Num;
  private startTipTime?: number;
  private syncedBlocks: number = 0;
  private syncedBlockTime: number = 0;

  constructor(
    configService: ConfigService,
    private readonly udtParser: UdtParser,
    private readonly entityManager: EntityManager,
    private readonly syncStatusRepo: SyncStatusRepo,
    private readonly udtInfoRepo: UdtInfoRepo,
    private readonly udtBalanceRepo: UdtBalanceRepo,
    private readonly blockRepo: BlockRepo,
  ) {
    this.isMainnet = configService.get<boolean>("sync.isMainnet");
    this.ckbRpcUri = configService.get<string>("sync.ckbRpcUri");
    this.ckbRpcTimeout = configService.get<number>("sync.ckbRpcTimeout");
    this.maxConcurrent = configService.get<number>("sync.maxConcurrent");
    this.client = this.isMainnet
      ? new ccc.ClientPublicMainnet({
          url: this.ckbRpcUri,
          maxConcurrent: this.maxConcurrent,
        })
      : new ccc.ClientPublicTestnet({
          url: this.ckbRpcUri,
          maxConcurrent: this.maxConcurrent,
        });
    this.threads = configService.get<number>("sync.threads");
    this.blockChunk = configService.get<number>("sync.blockChunk");

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
          : undefined;
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
      for await (const { height, block } of getBlocks({
        start: pendingHeight,
        end: endBlock,
        workers: this.threads,
        chunkSize: this.blockChunk,
        rpcUri: this.ckbRpcUri,
        rpcTimeout: this.ckbRpcTimeout,
        isMainnet: this.isMainnet,
        maxConcurrent: this.maxConcurrent,
      })) {
        if (!block) {
          this.logger.error(`Failed to get block ${height}`);
          break;
        }
        txsCount += block.transactions.length;

        const txDiffs = await Promise.all(
          block.transactions.map(async (txLike) => {
            const tx = ccc.Transaction.from(txLike);
            const diffs = await this.udtParser.udtInfoHandleTx(tx);
            return { tx, diffs };
          }),
        );

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

            for (const { tx, diffs } of txDiffs) {
              await this.udtParser.saveDiffs(entityManager, tx, height, diffs);
            }

            await syncStatusRepo.updateSyncHeight(pendingStatus, height);
          },
        );

        this.syncedBlocks += 1;

        if (this.syncedBlocks % 1000 === 0) {
          const syncedBlockTime = this.syncedBlockTime + Date.now() - tipTime;
          const blocksDiff = Number(tip - endBlock);
          const syncCost = syncedBlockTime / this.syncedBlocks;
          const estimatedTime = tipCost
            ? (blocksDiff * syncCost * tipCost) / (tipCost - syncCost)
            : blocksDiff * syncCost;
          this.logger.log(
            `Tip ${tip} ${tipCost ? (tipCost / 1000).toFixed(1) : "-"} s/block, synced block ${height}, ${(
              (this.syncedBlocks * 1000) /
              syncedBlockTime
            ).toFixed(1)} blocks/s (~${
              estimatedTime !== undefined
                ? (estimatedTime / 1000 / 60).toFixed(1)
                : "-"
            } mins left). ${txsCount} transactions processed`,
          );
          txsCount = 0;
        }
      }
      this.syncedBlockTime += Date.now() - tipTime;

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
