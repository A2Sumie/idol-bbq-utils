-- AlterTable
-- Normalized OneBot message ids per outbound send (SnowLuma `get_msg` /
-- `delete_msg` receipt + recall support). Nullable JSON: old builds ignore the
-- column, so rolling back only requires deploying the previous image.
ALTER TABLE "outbound_messages" ADD COLUMN "message_ids" JSONB;
