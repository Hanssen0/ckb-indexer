import { Module } from "@nestjs/common";
import { SyncStatusRepo, UdtBalanceRepo, UdtInfoRepo } from "./repos";
import { BlockRepo } from "./repos/block.repo";
import { SyncController } from "./sync.controller";
import { SyncService } from "./sync.service";
import { UdtParser } from "./udtParser";

@Module({
  providers: [
    SyncService,
    UdtParser,
    BlockRepo,
    SyncStatusRepo,
    UdtBalanceRepo,
    UdtInfoRepo,
  ],
  exports: [SyncService, UdtParser],
  controllers: [SyncController],
})
export class SyncModule {}
