import { useEffect, useState } from "react";
import { validate } from "../lib/model.js";
import { snapshotUrl } from "../lib/config.js";

export function useSnapshot() {
  const [data, setData] = useState(null),
    [error, setError] = useState(""),
    [loading, setLoading] = useState(false);
  async function refresh() {
    setLoading(true);
    setError("");
    try {
      const response = await fetch(snapshotUrl, { cache: "no-store" });
      if (!response.ok)
        throw new Error(`Snapshot request failed (${response.status})`);
      setData(validate(await response.json()));
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }
  useEffect(() => {
    refresh();
  }, []);
  return { data, error, loading, refresh };
}
