export const hosted = import.meta.env.VITE_PUBLISHED_DEMO === true;
export const snapshotUrl = import.meta.env.VITE_SNAPSHOT_URL || "/api/snapshot";
export const channelLabels = {
  shopify: "Shopify",
  amazon: "Amazon",
  pos: "In-store POS",
};
